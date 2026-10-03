import { Logger, type OnModuleInit } from '@nestjs/common';
import {
  columns,
  ensureLockRows,
  keyColumn,
  lockKeys,
  mysqlErrorCode,
  quoteTable,
  retryOnDeadlock,
  SqlParams,
  toBool,
  toInt,
  toJson,
  toText,
  type MigrationSqlOptions,
  type MigrationStatementsOptions,
  type SqlExecutor,
  type SqlTransaction,
  type StoreReadiness,
} from '@nestjs/store-kit/mysql';
import type { WorkflowInstance, WorkflowJournalEntry, WorkflowStatus, WorkflowWait } from '../interfaces/workflow-instance.interface.js';
import type {
  NewWorkflowInstance,
  NewWorkflowSignal,
  WorkflowCancelRequest,
  WorkflowClaim,
  WorkflowClaimRequest,
  WorkflowInstanceDetails,
  WorkflowListQuery,
  WorkflowPurgeQuery,
  WorkflowPurgeResult,
  WorkflowReopen,
  WorkflowScheduleClaimRequest,
  WorkflowScheduleQuery,
  WorkflowScheduleRecord,
  WorkflowScheduleSave,
  WorkflowScheduleWrite,
  WorkflowSignalQuery,
  WorkflowSignalRecord,
  WorkflowSignalResult,
  WorkflowStore,
  WorkflowWrite,
} from '../interfaces/workflow-store.interface.js';
import { RateWindowClaim, type PickedInstance } from '../sql/rate-windows.js';
import { CANCELLABLE_STATUSES, INSTANCE_COLUMNS, RUNNABLE_STATUSES, SCHEDULE_COLUMNS, SIGNAL_COLUMNS, toInstance, toSchedule, type Row } from '../sql/workflow-rows.js';
import type { WorkflowStorage } from '../storage/workflow.storage.js';
import type { MySqlWorkflowStoreOptions } from './interfaces/mysql-workflow-store-options.interface.js';
import { checkKey, MYSQL_KEY_LIMITS as L } from './key-limits.js';
import { mysqlWorkflowStoreSchema } from './migrations/index.js';

/** ER_DUP_ENTRY: MySQL rolled the statement back (not the transaction): the row exists. */
const DUPLICATE_KEY = 1062;

/**
 * The lock (a row of the kit's `<schema>_locks`) that serializes signals (exclusive) with the writes that replace an
 * instance's waits (shared), until the transaction ends: signal ids become visible in id order, and a signal can't
 * slip between a suspension's check for missed signals and its commit.
 */
const SIGNALS_LOCK = 'signals';

/** The lock a claim of `workflow` takes while it counts the slots of its concurrency limit. */
const concurrencyLock = (workflow: string) => `concurrency:${workflow}`;

const RUNNABLE = `(${RUNNABLE_STATUSES.map((status) => `'${status}'`).join(', ')})`;
const CANCELLABLE = `(${CANCELLABLE_STATUSES.map((status) => `'${status}'`).join(', ')})`;

/** How many rounds of candidates a claim tries at most, passing over those other claims hold (see `lockDue()`). */
const CLAIM_ROUNDS = 10;

/** The rows of one multi-row insert (Prisma prepares statements on the server: 65,535 placeholders at most). */
const ROWS_PER_INSERT = 1_000;

/**
 * The first-party `WorkflowStore` on MySQL (8.4 LTS and 9.x), through the client the application already has
 * (`fromMysql2()`, `fromSequelize()`, `fromDrizzle()`, `fromTypeOrm()`, `fromPrisma()`,
 * `fromKysely()`). It keeps its tables in the connection's database, named `<schema>_<table>`
 * (`nest_workflows_instances`...), which its migrations create and bring up to date, and it joins the application's
 * transactions for `start()` and `signal()` with `{ transaction }`.
 *
 * ```ts
 * @Module({
 *   imports: [WorkflowsModule.forRoot()],
 *   providers: [
 *     {
 *       provide: MySqlWorkflowStore,
 *       inject: [getDrizzleToken(), WorkflowStorage],
 *       useFactory: (db: Database, storage: WorkflowStorage) => new MySqlWorkflowStore({ executor: fromDrizzle(db) }, storage),
 *     },
 *   ],
 * })
 * export class AppModule {}
 * ```
 *
 * Given `storage`, it registers itself (`storage.registerSource(this)`), as every store does. It checks the server and
 * its schema, or migrates it (`migrate`), in `onModuleInit` (so before the worker starts), or at its first call
 * outside Nest.
 *
 * On MySQL:
 * - Ids, names and keys are case- and accent-sensitive, and bounded: an instance id, a workflow's or a signal's name,
 *   a signal's key or id, a concurrency or rate-limit key hold at most 255 characters, a schedule's id 230, a
 *   journal entry's name 512. A longer one fails with a `RangeError` that names it, before any SQL.
 * - The application's transactions may be REPEATABLE READ, MySQL's default. `signal(..., { transaction })` finds the
 *   instances to wake with a locking read of their waits, which sees the latest committed ones whatever the level; the
 *   signal lock is held until the application's transaction ends, as on PostgreSQL. `start(..., { transaction })`
 *   reads the last signal id from the transaction's snapshot: under REPEATABLE READ, signals committed after the
 *   transaction's first read can match the new instance's waits.
 * - The store's own transactions run READ COMMITTED and are run again when MySQL breaks a deadlock (1213). A deadlock in
 *   the application's transaction rolled the application's own writes back too: the store lets it through, for the
 *   application to retry its transaction.
 */
export class MySqlWorkflowStore implements WorkflowStore, OnModuleInit {
  /**
   * The SQL of the store's migrations, for teams that apply migrations with their own tool (drizzle-kit, TypeORM,
   * Prisma Migrate, Flyway...) and run the store with `migrate: false`: from version `from` (default `0`, a new
   * database) to `to` (default: the version this version of the package needs), with the bookkeeping that tells the
   * store which versions a schema has (`<schema>_migrations`). MySQL commits each DDL statement on its own, so the
   * statements don't run in one transaction: apply them in order, each once. Downgrades aren't supported. With
   * `statementBreakpoints`, drizzle-kit's `--> statement-breakpoint` separates the statements, which its MySQL
   * migrator needs (it runs one statement per call).
   *
   * ```ts
   * // drizzle/0003_workflows.sql, created empty by `npx drizzle-kit generate --custom --name=workflows`
   * writeFileSync('drizzle/0003_workflows.sql', MySqlWorkflowStore.migrationSql({ statementBreakpoints: true }));
   * ```
   */
  static migrationSql(options: MigrationSqlOptions = {}): string {
    return mysqlWorkflowStoreSchema.sql(options);
  }

  /**
   * `migrationSql()`'s statements, one per string, for a migration tool that runs one statement per call (TypeORM's
   * `queryRunner.query()`, mysql2): run them in order, each once.
   *
   * ```ts
   * export class Workflows1790000000000 implements MigrationInterface {
   *   async up(queryRunner: QueryRunner): Promise<void> {
   *     for (const statement of MySqlWorkflowStore.migrationStatements()) {
   *       await queryRunner.query(statement);
   *     }
   *   }
   * }
   * ```
   */
  static migrationStatements(options: MigrationStatementsOptions = {}): string[] {
    return mysqlWorkflowStoreSchema.statements(options);
  }

  /** The schema version this version of the package needs: its last migration. */
  static readonly schemaVersion = mysqlWorkflowStoreSchema.latest;

  private readonly logger = new Logger('WorkflowsModule');
  private readonly executor: SqlExecutor;
  private readonly schema: string;
  private readonly t: Record<'instances' | 'journal' | 'waits' | 'signals' | 'schedules' | 'rateLimits', string>;
  /** The server and the schema checked, and the schema migrated (`migrate`), before the first statement. */
  private readonly readiness: StoreReadiness;
  /** The lock keys whose rows this store created, or found (see `createLockRows()`). */
  private readonly lockRows = new Set<string>();

  constructor(options: MySqlWorkflowStoreOptions, storage?: WorkflowStorage) {
    const resolved = mysqlWorkflowStoreSchema.resolveOptions(options);
    this.executor = resolved.executor;
    this.schema = resolved.schema;
    const t = (table: string) => quoteTable(this.schema, table, 'MySqlWorkflowStore');
    this.t = {
      instances: t('instances'),
      journal: t('journal'),
      waits: t('waits'),
      signals: t('signals'),
      schedules: t('schedules'),
      rateLimits: t('rate_limits'),
    };
    this.readiness = mysqlWorkflowStoreSchema.readiness({ ...resolved, logger: this.logger });
    storage?.registerSource(this);
  }

  /**
   * Checks the server and migrates the schema (`migrate`) or checks it, before the worker starts: startup fails if it
   * can't serve. Then creates the signal lock's row (see `createLockRows()`).
   */
  async onModuleInit(): Promise<void> {
    await this.ready();
  }

  /**
   * Applies the migrations the schema hasn't had yet, whatever `migrate` says, one statement at a time under a lock
   * (`GET_LOCK()`): of processes that migrate together, one applies them, and a run that stopped halfway resumes where
   * it stopped. Resolves to the versions it applied (`[]`: none were pending).
   */
  migrate(): Promise<number[]> {
    return this.readiness.migrate();
  }

  // ---------------------------------------------------------------- instances

  async create(instance: NewWorkflowInstance): Promise<{ instance: WorkflowInstance; created: boolean }> {
    await this.ready();
    checkInstance(instance);
    return retryOnDeadlock(this.executor, (tx) => this.insertInstance(tx, instance));
  }

  /** `start(..., { transaction })`: the instance commits or rolls back with the application's rows. */
  async createInTransaction(transaction: unknown, instance: NewWorkflowInstance): Promise<{ instance: WorkflowInstance; created: boolean }> {
    const tx = this.executor.wrapTransaction(transaction);
    await this.readiness.readyIn(tx);
    checkInstance(instance);
    return this.insertInstance(tx, instance);
  }

  async get(id: string, options: { journal?: boolean } = {}): Promise<WorkflowInstanceDetails | null> {
    await this.ready();
    // One statement, one snapshot. JSON_ARRAYAGG() takes no ORDER BY: the positions and sequence numbers come along.
    const journal = options.journal
      ? `,
  (SELECT CAST(JSON_ARRAYAGG(JSON_OBJECT('seq', j.seq, 'entry', j.entry)) AS CHAR) FROM ${this.t.journal} j WHERE j.instance_id = i.id) AS journal`
      : '';
    const p = new SqlParams();
    const [row] = await this.executor.query<Row>(
      `SELECT ${columns(INSTANCE_COLUMNS, 'i')},
  (SELECT CAST(JSON_ARRAYAGG(JSON_OBJECT('position', w.position, 'signal', w.\`signal\`, 'key', w.\`key\`)) AS CHAR) FROM ${this.t.waits} w WHERE w.instance_id = i.id) AS waits${journal}
FROM ${this.t.instances} i
WHERE i.id = ${p.text(id)}`,
      p.values,
    );
    if (!row) {
      return null;
    }

    const waits = ((toJson(row.waits) ?? []) as Array<WorkflowWait & { position: number }>)
      .sort((a, b) => a.position - b.position)
      .map(({ signal, key }) => ({ signal, key }));
    const details: WorkflowInstanceDetails = { ...toInstance(row), waits };
    if (!options.journal) {
      return details;
    }

    const entries = ((toJson(row.journal) ?? []) as Array<{ seq: number; entry: WorkflowJournalEntry }>).sort((a, b) => a.seq - b.seq);
    return { ...details, journal: entries.map(({ entry }) => entry) };
  }

  async list(query: WorkflowListQuery): Promise<WorkflowInstance[]> {
    await this.ready();
    const p = new SqlParams();
    const where = [
      ...(query.status ? [p.in('i.status', query.status)] : []),
      ...(query.workflow !== undefined ? [`i.workflow = ${p.text(query.workflow)}`] : []),
      ...(query.version !== undefined ? [`i.version = ${p.int(query.version)}`] : []),
      ...(query.parentId !== undefined ? [`i.parent_id = ${p.text(query.parentId)}`] : []),
      ...(query.scheduleId !== undefined ? [`i.schedule_id = ${p.text(query.scheduleId)}`] : []),
    ];
    const rows = await this.executor.query<Row>(
      `SELECT ${columns(INSTANCE_COLUMNS, 'i')} FROM ${this.t.instances} i${where.length > 0 ? ` WHERE ${where.join(' AND ')}` : ''}
ORDER BY i.created_at, i.id LIMIT ${p.limit(query.limit)} OFFSET ${p.limit(query.offset)}`,
      p.values,
    );
    return rows.map(toInstance);
  }

  async requestCancel(id: string, { reason, now, terminate }: WorkflowCancelRequest): Promise<boolean> {
    await this.ready();
    // A terminate also stops a compensating instance, and follows a cancel. One conditional update by primary key: of
    // two concurrent requests, the second waits for the first's row and finds it changed.
    const applies = terminate ? `status IN ${RUNNABLE} AND NOT terminate_requested` : `status IN ${CANCELLABLE} AND NOT cancel_requested`;
    const p = new SqlParams();
    const { affectedRows } = await this.executor.execute(
      `UPDATE ${this.t.instances}
SET cancel_requested = TRUE,${terminate ? ' terminate_requested = TRUE,' : ''} cancel_reason = ${p.text(reason)}, updated_at = ${p.bigint(now)},
  wake_at = LEAST(COALESCE(wake_at, ${p.bigint(now)}), ${p.bigint(now)})
WHERE id = ${p.text(id)} AND ${applies}`,
      p.values,
    );
    return affectedRows === 1;
  }

  /** `WorkflowClient.retry()`: one conditional update, then the journal, in one transaction. */
  async reopen(id: string, reopen: WorkflowReopen): Promise<boolean> {
    await this.ready();
    checkEntries(reopen.entries);
    return retryOnDeadlock(this.executor, async (tx) => {
      // The update locks the row; of two concurrent retries, the second waits for it and finds it changed.
      const p = new SqlParams();
      const { affectedRows } = await tx.execute(
        `UPDATE ${this.t.instances}
SET status = ${p.text(reopen.status)}, error = ${p.json(reopen.error)}${reopen.deadline === undefined ? '' : `, deadline = ${p.bigint(reopen.deadline)}`}, wake_at = ${p.bigint(reopen.now)}, updated_at = ${p.bigint(reopen.now)}
WHERE id = ${p.text(id)} AND lease_token IS NULL AND status = ${p.text(reopen.expect.status)} AND runs = ${p.int(reopen.expect.runs)}`,
        p.values,
      );
      if (affectedRows === 0) {
        return false;
      }

      if (reopen.entries.length > 0) {
        await this.upsertEntries(tx, id, reopen.entries);
      }
      return true;
    });
  }

  /** `WorkflowClient.delete()`: its journal and waits go with it, in the same transaction. */
  async delete(id: string, statuses: WorkflowStatus[]): Promise<boolean> {
    await this.ready();
    return retryOnDeadlock(this.executor, async (tx) => {
      // Shared, before the row: a signal in the application's REPEATABLE READ transaction locks the waits it reads, then
      // the instances it wakes, so this delete, which locks the instance, then its waits, waits for it first.
      await lockKeys(tx, this.schema, SIGNALS_LOCK, { shared: true });
      const p = new SqlParams();
      const { affectedRows } = await tx.execute(`DELETE FROM ${this.t.instances} WHERE id = ${p.text(id)} AND ${p.in('status', statuses)}`, p.values);
      if (affectedRows === 0) {
        return false;
      }

      await this.deleteChildren(tx, [id]);
      return true;
    });
  }

  // ---------------------------------------------------------------- signals

  async signal(signal: NewWorkflowSignal): Promise<WorkflowSignalResult> {
    await this.ready();
    checkSignal(signal);
    return retryOnDeadlock(this.executor, (tx) => this.insertSignal(tx, signal));
  }

  /**
   * `signal(..., { transaction })`: the signal and its wake-ups commit with the application's rows. Any isolation
   * level: the wake-up reads the latest committed waits with a locking read, even under REPEATABLE READ.
   */
  async signalInTransaction(transaction: unknown, signal: NewWorkflowSignal): Promise<WorkflowSignalResult> {
    const tx = this.executor.wrapTransaction(transaction);
    await this.readiness.readyIn(tx);
    checkSignal(signal);
    return this.insertSignal(tx, signal);
  }

  async signals(query: WorkflowSignalQuery): Promise<WorkflowSignalRecord[]> {
    await this.ready();
    const p = new SqlParams();
    const rows = await this.executor.query<Row>(
      `SELECT ${columns(SIGNAL_COLUMNS, 's')} FROM ${this.t.signals} s
WHERE s.name = ${p.text(query.name)} AND ${p.equals('s.`key`', query.key)} AND s.id > ${p.bigint(query.afterId)} AND s.id <= ${p.bigint(query.upToId)}
ORDER BY s.id`,
      p.values,
    );
    return rows.map((row) => ({ id: toInt(row.id)!, name: row.name!, key: row.key, payload: toJson(row.payload), createdAt: toInt(row.created_at)! }));
  }

  // ---------------------------------------------------------------- retention

  async purge(query: WorkflowPurgeQuery): Promise<WorkflowPurgeResult> {
    await this.ready();
    const finished = (p: SqlParams) => `${p.in('i.status', query.statuses)} AND i.updated_at < ${p.bigint(query.before)}`;

    // Instances, oldest first: candidates by a consistent read, then those still finished locked (the condition again:
    // an instance reopened since stays), skipping any another transaction holds. Their journals and waits go with them.
    const instances = await retryOnDeadlock(this.executor, async (tx) => {
      // As delete(): a signal in the application's transaction locks waits before instances.
      await lockKeys(tx, this.schema, SIGNALS_LOCK, { shared: true });
      const c = new SqlParams();
      const candidates = await tx.query<Row>(
        `SELECT CAST(i.id AS CHAR) AS id FROM ${this.t.instances} i WHERE ${finished(c)} ORDER BY i.updated_at, i.id LIMIT ${c.limit(query.limit)}`,
        c.values,
      );
      if (candidates.length === 0) {
        return 0;
      }

      const l = new SqlParams();
      const locked = await tx.query<Row>(
        `SELECT CAST(i.id AS CHAR) AS id FROM ${this.t.instances} i FORCE INDEX (PRIMARY)
WHERE ${l.in('i.id', candidates.map((row) => row.id!))} AND ${finished(l)} FOR UPDATE SKIP LOCKED`,
        l.values,
      );
      const ids = locked.map((row) => row.id!);
      if (ids.length === 0) {
        return 0;
      }

      const d = new SqlParams();
      await tx.execute(`DELETE FROM ${this.t.instances} WHERE ${d.in('id', ids)}`, d.values);
      await this.deleteChildren(tx, ids);
      return ids.length;
    });

    // Signals no instance can take: at or below every unfinished instance's cursor (new ones start at the newest
    // signal), old enough, and never the newest, so the last signal id never goes back.
    const signals = await retryOnDeadlock(this.executor, async (tx) => {
      const [bounds] = await tx.query<Row>(
        `SELECT CAST(MAX(s.id) AS CHAR) AS newest,
  CAST((SELECT MIN(i.signal_cursor) FROM ${this.t.instances} i WHERE i.status IN ${RUNNABLE}) AS CHAR) AS lowest
FROM ${this.t.signals} s`,
      );
      const newest = toInt(bounds?.newest);
      if (newest === null) {
        return 0;
      }

      const c = new SqlParams();
      const candidates = await tx.query<Row>(
        `SELECT CAST(s.id AS CHAR) AS id FROM ${this.t.signals} s
WHERE s.created_at < ${c.bigint(query.before)} AND s.id <= ${c.bigint(Math.min(newest - 1, toInt(bounds!.lowest) ?? newest))}
ORDER BY s.id LIMIT ${c.limit(query.limit)}`,
        c.values,
      );
      if (candidates.length === 0) {
        return 0;
      }

      const d = new SqlParams();
      return (await tx.execute(`DELETE FROM ${this.t.signals} WHERE ${d.in('id', candidates.map((row) => row.id!))}`, d.values)).affectedRows;
    });

    // Rate-limit windows that ended, oldest first: candidates by a consistent read, then those still ended locked by key
    // (a claim may have opened a new window in one meanwhile), skipping those a claim is locking: waiting for them, in
    // another order than the claim's, could deadlock.
    const rateLimits = await retryOnDeadlock(this.executor, async (tx) => {
      const c = new SqlParams();
      const candidates = await tx.query<Row>(
        `SELECT ${columns(['workflow', 'key'], 'rl')} FROM ${this.t.rateLimits} rl WHERE rl.window_end < ${c.bigint(query.before)}
ORDER BY rl.window_end, rl.workflow, rl.\`key\` LIMIT ${c.limit(query.limit)}`,
        c.values,
      );
      if (candidates.length === 0) {
        return 0;
      }

      const l = new SqlParams();
      const locked = await tx.query<Row>(
        `SELECT ${columns(['workflow', 'key'], 'rl')} FROM ${this.t.rateLimits} rl FORCE INDEX (PRIMARY)
WHERE (rl.workflow, rl.\`key\`) IN (${candidates.map((row) => `(${l.text(row.workflow)}, ${l.text(row.key)})`).join(', ')}) AND rl.window_end < ${l.bigint(query.before)}
FOR UPDATE SKIP LOCKED`,
        l.values,
      );
      if (locked.length === 0) {
        return 0;
      }

      const d = new SqlParams();
      return (await tx.execute(`DELETE FROM ${this.t.rateLimits} WHERE (workflow, \`key\`) IN (${locked.map((row) => `(${d.text(row.workflow)}, ${d.text(row.key)})`).join(', ')})`, d.values))
        .affectedRows;
    });

    return { instances, signals, rateLimits };
  }

  // ---------------------------------------------------------------- schedules

  async saveSchedule(save: WorkflowScheduleSave): Promise<WorkflowScheduleRecord | null> {
    await this.ready();
    checkKey('a schedule id', save.id, L.scheduleId);
    checkKey("a workflow's name", save.workflow, L.workflow);
    return retryOnDeadlock(this.executor, async (tx) => {
      const p = new SqlParams();
      if (save.expectRevision === null) {
        try {
          await tx.execute(
            `INSERT INTO ${this.t.schedules} (id, workflow, declared, spec, input, paused, wake_at, state, revision, created_at, updated_at)
VALUES (${p.text(save.id)}, ${p.text(save.workflow)}, ${p.bool(save.declared)}, ${p.json(save.spec)}, ${p.json(save.input)}, ${p.bool(save.paused)},
  ${p.bigint(save.wakeAt)}, ${p.json(save.state)}, 1, ${p.bigint(save.now)}, ${p.bigint(save.now)})`,
            p.values,
          );
        } catch (error) {
          if (mysqlErrorCode(error) === DUPLICATE_KEY) {
            return null;
          }
          throw error;
        }
      } else {
        // One conditional update: of two saves that read the same revision, the second waits for the row and finds it
        // changed.
        const { affectedRows } = await tx.execute(
          `UPDATE ${this.t.schedules}
SET workflow = ${p.text(save.workflow)}, declared = ${p.bool(save.declared)}, spec = ${p.json(save.spec)}, input = ${p.json(save.input)}, paused = ${p.bool(save.paused)},
  wake_at = ${p.bigint(save.wakeAt)}, state = ${p.json(save.state)}, updated_at = ${p.bigint(save.now)}, revision = revision + 1${save.releaseLease ? ', lease_token = NULL, lease_until = NULL' : ''}
WHERE id = ${p.text(save.id)} AND revision = ${p.int(save.expectRevision)}`,
          p.values,
        );
        if (affectedRows === 0) {
          return null;
        }
      }
      // As stored: the row this transaction wrote, and still locks.
      return this.readSchedule(tx, save.id);
    });
  }

  async getSchedule(id: string): Promise<WorkflowScheduleRecord | null> {
    await this.ready();
    return this.readSchedule(this.executor, id);
  }

  async listSchedules(query: WorkflowScheduleQuery): Promise<WorkflowScheduleRecord[]> {
    await this.ready();
    const p = new SqlParams();
    const where = [
      ...(query.workflow !== undefined ? [`s.workflow = ${p.text(query.workflow)}`] : []),
      ...(query.declared !== undefined ? [`s.declared = ${p.bool(query.declared)}`] : []),
    ];
    const rows = await this.executor.query<Row>(
      `SELECT ${columns(SCHEDULE_COLUMNS, 's')} FROM ${this.t.schedules} s${where.length > 0 ? ` WHERE ${where.join(' AND ')}` : ''}
ORDER BY s.id LIMIT ${p.limit(query.limit)} OFFSET ${p.limit(query.offset)}`,
      p.values,
    );
    return rows.map(toSchedule);
  }

  async deleteSchedule(id: string, revision?: number): Promise<boolean> {
    await this.ready();
    const p = new SqlParams();
    const { affectedRows } = await this.executor.execute(
      `DELETE FROM ${this.t.schedules} WHERE id = ${p.text(id)}${revision !== undefined ? ` AND revision = ${p.int(revision)}` : ''}`,
      p.values,
    );
    return affectedRows === 1;
  }

  async claimSchedules(request: WorkflowScheduleClaimRequest): Promise<WorkflowScheduleRecord[]> {
    await this.ready();
    checkKey('a lease token', request.token, L.leaseToken);
    return retryOnDeadlock(this.executor, async (tx) => {
      const due = (p: SqlParams) =>
        `NOT s.paused AND s.wake_at IS NOT NULL AND s.wake_at <= ${p.bigint(request.now)} AND (s.lease_until IS NULL OR s.lease_until < ${p.bigint(request.now)}) AND ${p.in('s.workflow', request.workflows)}`;
      const ids = await this.lockDue(tx, this.t.schedules, 's', due, 's.wake_at, s.id', request.limit);
      if (ids.length === 0) {
        return [];
      }

      const p = new SqlParams();
      await tx.execute(
        `UPDATE ${this.t.schedules} FORCE INDEX (PRIMARY) SET lease_token = ${p.text(request.token)}, lease_owner = ${p.text(request.owner)}, lease_until = ${p.bigint(request.leaseUntil)}
WHERE ${p.in('id', ids)}`,
        p.values,
      );
      const r = new SqlParams();
      const rows = await tx.query<Row>(`SELECT ${columns(SCHEDULE_COLUMNS, 's')} FROM ${this.t.schedules} s WHERE ${r.in('s.id', ids)} ORDER BY s.wake_at, s.id`, r.values);
      return rows.map(toSchedule);
    });
  }

  async writeSchedule(id: string, token: string, write: WorkflowScheduleWrite): Promise<boolean> {
    await this.ready();
    const p = new SqlParams();
    const { affectedRows } = await this.executor.execute(
      `UPDATE ${this.t.schedules}
SET state = ${p.json(write.state)}, wake_at = ${p.bigint(write.wakeAt)}, revision = revision + 1, updated_at = ${p.bigint(write.now)}${write.release ? ', lease_token = NULL, lease_until = NULL' : ''}
WHERE id = ${p.text(id)} AND lease_token = ${p.text(token)}`,
      p.values,
    );
    return affectedRows === 1;
  }

  // ---------------------------------------------------------------- the worker

  async claim(request: WorkflowClaimRequest): Promise<WorkflowClaim> {
    await this.ready();
    checkKey('a lease token', request.token, L.leaseToken);
    await this.createLockRows((request.limits ?? []).map((limit) => concurrencyLock(limit.workflow)));
    return retryOnDeadlock(this.executor, async (tx) => {
      if (request.limits?.length || request.rateLimits?.length) {
        return this.claimWithin(tx, request);
      }

      const ids = await this.lockDue(tx, this.t.instances, 'i', (p) => this.isDue(p, request, 'i'), 'i.priority, i.wake_at, i.created_at, i.id', request.limit);
      // The last signal id, read after the claim: the executions' signal cursor.
      return { instances: ids.length > 0 ? await this.lease(tx, request, ids) : [], lastSignalId: await this.lastSignalId(tx) };
    });
  }

  /**
   * A claim under concurrency or rate limits. Claims of a workflow with a concurrency limit take its lock first (the
   * kit's `lockKeys()`, in one order), so they count the slots live leases hold and lease the instances that fit one
   * after the other: two never both take the last slot. Rate-limit windows are counted again under their rows' locks,
   * once the instances are picked (see `takeRoom()`).
   */
  private async claimWithin(tx: SqlTransaction, request: WorkflowClaimRequest): Promise<WorkflowClaim> {
    const limits = request.limits ?? [];
    const rates = request.rateLimits ?? [];
    await lockKeys(
      tx,
      this.schema,
      limits.map((limit) => concurrencyLock(limit.workflow)),
    );

    // Candidates with no room at all are passed over; then each concurrency key's first, as many as its free slots; of
    // those, each rate key's, as many as its window has room for; of those, each workflow's, as many as both its free
    // slots and its window allow. A full key is passed over, not waited behind. The windows read here are a snapshot,
    // counted again under their locks in takeRoom().
    const limitRows = limits.map((l) => ({ workflow: l.workflow, total: l.limit, per_key: l.perKey }));
    const rateRows = rates.map((r) => ({ workflow: r.workflow, total: r.limit?.max ?? null, per_key: r.perKey?.max ?? null }));
    const rules = (json: string) =>
      `JSON_TABLE(${json}, '$[*]' COLUMNS (workflow ${keyColumn(L.workflow)} PATH '$.workflow', total int PATH '$.total', per_key int PATH '$.per_key'))`;
    const p = new SqlParams();
    const rows = await tx.query<Row>(
      `WITH limits AS (
  SELECT * FROM ${rules(p.json(limitRows))} AS l
),
rates AS (
  SELECT * FROM ${rules(p.json(rateRows))} AS r
),
held AS (
  SELECT i.workflow AS workflow, i.concurrency_key AS ckey, COUNT(*) AS n
  FROM ${this.t.instances} i
  WHERE i.lease_until >= ${p.bigint(request.now)} AND i.workflow IN (SELECT workflow FROM limits)
  GROUP BY i.workflow, i.concurrency_key
),
used AS (
  SELECT rl.workflow AS workflow, rl.\`key\` AS rkey, rl.\`count\` AS n
  FROM ${this.t.rateLimits} rl
  WHERE rl.window_end > ${p.bigint(request.now)} AND rl.workflow IN (SELECT workflow FROM rates)
),
due AS (
  SELECT i.id AS id, i.workflow AS workflow, i.concurrency_key AS ckey, i.rate_limit_key AS rkey, i.priority AS priority,
    i.wake_at AS wake_at, i.created_at AS created_at,
    ROW_NUMBER() OVER (PARTITION BY i.workflow, i.concurrency_key ORDER BY i.priority, i.wake_at, i.created_at, i.id) AS key_rank
  FROM ${this.t.instances} i
  LEFT JOIN limits l ON l.workflow = i.workflow
  LEFT JOIN rates r ON r.workflow = i.workflow
  WHERE ${this.isDue(p, request, 'i')}
    AND (l.total IS NULL OR COALESCE((SELECT SUM(h.n) FROM held h WHERE h.workflow = l.workflow), 0) < l.total)
    AND (l.per_key IS NULL OR i.concurrency_key IS NULL
      OR COALESCE((SELECT h.n FROM held h WHERE h.workflow = l.workflow AND h.ckey = i.concurrency_key), 0) < l.per_key)
    AND (r.total IS NULL OR COALESCE((SELECT u.n FROM used u WHERE u.workflow = r.workflow AND u.rkey = ''), 0) < r.total)
    AND (r.per_key IS NULL OR i.rate_limit_key IS NULL
      OR COALESCE((SELECT u.n FROM used u WHERE u.workflow = r.workflow AND u.rkey = i.rate_limit_key), 0) < r.per_key)
),
fits_key AS (
  SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.workflow, d.rkey ORDER BY d.priority, d.wake_at, d.created_at, d.id) AS rate_key_rank
  FROM due d
  LEFT JOIN limits l ON l.workflow = d.workflow
  WHERE l.per_key IS NULL OR d.ckey IS NULL
    OR d.key_rank <= l.per_key - COALESCE((SELECT h.n FROM held h WHERE h.workflow = d.workflow AND h.ckey = d.ckey), 0)
),
fits_rate_key AS (
  SELECT f.*, ROW_NUMBER() OVER (PARTITION BY f.workflow ORDER BY f.priority, f.wake_at, f.created_at, f.id) AS workflow_rank
  FROM fits_key f
  LEFT JOIN rates r ON r.workflow = f.workflow
  WHERE r.per_key IS NULL OR f.rkey IS NULL
    OR f.rate_key_rank <= r.per_key - COALESCE((SELECT u.n FROM used u WHERE u.workflow = f.workflow AND u.rkey = f.rkey), 0)
)
SELECT CAST(f.id AS CHAR) AS id FROM fits_rate_key f
LEFT JOIN limits l ON l.workflow = f.workflow
LEFT JOIN rates r ON r.workflow = f.workflow
WHERE (l.total IS NULL OR f.workflow_rank <= l.total - COALESCE((SELECT SUM(h.n) FROM held h WHERE h.workflow = f.workflow), 0))
  AND (r.total IS NULL OR f.workflow_rank <= r.total - COALESCE((SELECT u.n FROM used u WHERE u.workflow = f.workflow AND u.rkey = ''), 0))
ORDER BY f.priority, f.wake_at, f.created_at, f.id
LIMIT ${p.limit(request.limit)}`,
      p.values,
    );
    if (rows.length === 0) {
      return { instances: [], lastSignalId: await this.lastSignalId(tx) };
    }

    const lock = new SqlParams();
    const picked = await tx.query<Row>(
      `SELECT ${columns(['id', 'workflow', 'rate_limit_key'], 'i')} FROM ${this.t.instances} i FORCE INDEX (PRIMARY)
WHERE ${lock.in('i.id', rows.map((row) => row.id!))} AND ${this.isDue(lock, request, 'i')}
ORDER BY i.priority, i.wake_at, i.created_at, i.id
FOR UPDATE SKIP LOCKED`,
      lock.values,
    );
    const granted = await this.takeRoom(
      tx,
      request,
      picked.map((row) => ({ id: row.id!, workflow: row.workflow!, rateLimitKey: row.rate_limit_key })),
    );
    return { instances: granted.length > 0 ? await this.lease(tx, request, granted) : [], lastSignalId: await this.lastSignalId(tx) };
  }

  /**
   * Of `picked` (in claim order), the instances their rate-limit windows have room for, recorded in the windows. Each
   * window's row is inserted where missing and locked, in one order (`RateWindowClaim.windows`): the count read under
   * the lock is exact (a concurrent claim of the same window waits for this one), and a purge can't delete the row in
   * between.
   */
  private async takeRoom(tx: SqlTransaction, request: WorkflowClaimRequest, picked: PickedInstance[]): Promise<string[]> {
    const claim = new RateWindowClaim(request, picked);
    if (claim.windows.length === 0) {
      return picked.map((instance) => instance.id);
    }

    // An existing row is locked exclusively by the update that leaves it as it is. (Its count of affected rows says
    // nothing: an insert and an unchanged duplicate count alike.)
    const p = new SqlParams();
    await tx.execute(
      `INSERT INTO ${this.t.rateLimits} (workflow, \`key\`, window_end, \`count\`)
VALUES ${claim.windows.map(({ workflow, key }) => `(${p.text(workflow)}, ${p.text(key)}, 0, 0)`).join(', ')}
ON DUPLICATE KEY UPDATE \`count\` = \`count\``,
      p.values,
    );
    // The rows this transaction just locked: a plain read sees their latest values (nobody else can change them now).
    const r = new SqlParams();
    const locked = await tx.query<Row>(
      `SELECT ${columns(['workflow', 'key', 'window_end', 'count'], 'rl')} FROM ${this.t.rateLimits} rl
WHERE (rl.workflow, rl.\`key\`) IN (${claim.windows.map(({ workflow, key }) => `(${r.text(workflow)}, ${r.text(key)})`).join(', ')})`,
      r.values,
    );
    const { granted, changed } = claim.grant(locked.map((row) => ({ workflow: row.workflow!, key: row.key!, windowEnd: toInt(row.window_end)!, count: toInt(row.count)! })));

    if (changed.length > 0) {
      const u = new SqlParams();
      const values = changed.map((w) => ({ workflow: w.workflow, key: w.key, window_end: w.windowEnd, count: w.count }));
      await tx.execute(
        `UPDATE ${this.t.rateLimits} rl
JOIN JSON_TABLE(${u.json(values)}, '$[*]' COLUMNS (
  workflow ${keyColumn(L.workflow)} PATH '$.workflow',
  \`key\` ${keyColumn(L.rateLimitKey)} PATH '$.key',
  window_end bigint PATH '$.window_end',
  \`count\` int PATH '$.count'
)) AS v ON rl.workflow = v.workflow AND rl.\`key\` = v.\`key\`
SET rl.window_end = v.window_end, rl.\`count\` = v.\`count\``,
        u.values,
      );
    }
    return granted;
  }

  /**
   * Locks up to `limit` rows of `table` (as `alias`) that `due` selects, in `order`, passing over the rows other
   * transactions hold, as PostgreSQL's `FOR UPDATE SKIP LOCKED` under `ORDER BY ... LIMIT` does. InnoDB's own locks
   * every row its sort reads, not only those it returns (a claim of 7 among 60 due instances held all 60, and a
   * concurrent claim found none), so each round:
   *
   * 1. a consistent read finds the next candidates in `order` (it locks nothing, whatever the plan): as many as the rows
   *    still wanted, and as many more as other transactions held in the rounds before, so a round reaches past the rows
   *    the claims beside this one are taking;
   * 2. a locking read takes those still due that no one holds (`SKIP LOCKED`), by primary key and in its order, up to the
   *    rows wanted: InnoDB stops there, and locks no candidate it doesn't return. (Among the candidates of a round, the
   *    ones taken are the first free by id: under contention, a claim may take a less overdue one than another claim.)
   *
   * Resolves to the ids locked.
   */
  private async lockDue(tx: SqlTransaction, table: string, alias: string, due: (p: SqlParams) => string, order: string, limit: number): Promise<string[]> {
    const locked: string[] = [];
    const tried: string[] = [];
    let held = 0;
    for (let round = 0; round < CLAIM_ROUNDS && locked.length < limit; round++) {
      const wanted = limit - locked.length;
      const batch = wanted + held;
      const p = new SqlParams();
      const candidates = await tx.query<Row>(
        `SELECT CAST(${alias}.id AS CHAR) AS id FROM ${table} ${alias}
WHERE ${due(p)}${tried.length > 0 ? ` AND NOT (${p.in(`${alias}.id`, tried)})` : ''}
ORDER BY ${order} LIMIT ${p.limit(batch)}`,
        p.values,
      );
      if (candidates.length === 0) {
        break;
      }

      const ids = candidates.map((row) => row.id!);
      tried.push(...ids);
      const q = new SqlParams();
      const rows = await tx.query<Row>(
        `SELECT CAST(${alias}.id AS CHAR) AS id FROM ${table} ${alias} FORCE INDEX (PRIMARY)
WHERE ${q.in(`${alias}.id`, ids)} AND ${due(q)} ORDER BY ${alias}.id LIMIT ${q.limit(wanted)} FOR UPDATE SKIP LOCKED`,
        q.values,
      );
      locked.push(...rows.map((row) => row.id!));
      held += candidates.length - rows.length;
      if (candidates.length < batch) {
        break;
      }
    }
    return locked;
  }

  /** Leases the instances `ids` (locked by this transaction), and reads them back in claim order. */
  private async lease(tx: SqlTransaction, request: WorkflowClaimRequest, ids: string[]): Promise<WorkflowInstance[]> {
    const p = new SqlParams();
    await tx.execute(
      `UPDATE ${this.t.instances} FORCE INDEX (PRIMARY)
SET lease_token = ${p.text(request.token)}, lease_owner = ${p.text(request.owner)}, lease_until = ${p.bigint(request.leaseUntil)},
  runs = runs + 1, updated_at = ${p.bigint(request.now)}, status = CASE WHEN status = 'compensating' THEN status ELSE 'running' END
WHERE ${p.in('id', ids)}`,
      p.values,
    );
    const r = new SqlParams();
    const rows = await tx.query<Row>(
      `SELECT ${columns(INSTANCE_COLUMNS, 'i')} FROM ${this.t.instances} i WHERE ${r.in('i.id', ids)} ORDER BY i.priority, i.wake_at, i.created_at, i.id`,
      r.values,
    );
    return rows.map(toInstance);
  }

  async renew(id: string, token: string, leaseUntil: number): Promise<{ cancelRequested: boolean; terminateRequested: boolean } | null> {
    await this.ready();
    // The fence: the rows the update matched (it matches the lease holder's row even when it writes the value the row
    // has, a manual clock's extension). The flags, read right after, are the instance's latest.
    const p = new SqlParams();
    const { affectedRows } = await this.executor.execute(
      `UPDATE ${this.t.instances} SET lease_until = ${p.bigint(leaseUntil)} WHERE id = ${p.text(id)} AND lease_token = ${p.text(token)}`,
      p.values,
    );
    if (affectedRows === 0) {
      return null;
    }

    const r = new SqlParams();
    const [row] = await this.executor.query<Row>(`SELECT ${columns(['cancel_requested', 'terminate_requested'])} FROM ${this.t.instances} WHERE id = ${r.text(id)}`, r.values);
    return row ? { cancelRequested: toBool(row.cancel_requested), terminateRequested: toBool(row.terminate_requested) } : null;
  }

  async write(id: string, token: string, write: WorkflowWrite): Promise<boolean> {
    await this.ready();
    checkEntries(write.entries);
    if (write.signal) {
      checkSignal(write.signal);
    }
    for (const wait of write.release?.waits ?? []) {
      checkKey("a wait's signal name", wait.signal, L.signal);
      checkKey("a wait's signal key", wait.key, L.signalKey);
    }

    return retryOnDeadlock(this.executor, async (tx) => {
      const release = write.release;
      if (write.signal) {
        await lockKeys(tx, this.schema, SIGNALS_LOCK);
      } else if (release) {
        // Shared, before the row: a suspension's check for missed signals never interleaves with a signal. And any
        // release replaces the instance's waits, which a signal in the application's REPEATABLE READ transaction locks
        // before the instances it wakes: waiting for it here, this write never takes the two in the other order.
        await lockKeys(tx, this.schema, SIGNALS_LOCK, { shared: true });
      }

      // The fence: only the lease holder writes, and the row stays locked until commit.
      const fence = new SqlParams();
      const [fenced] = await tx.query<Row>(
        `SELECT ${columns(['cancel_requested'])} FROM ${this.t.instances} WHERE id = ${fence.text(id)} AND lease_token = ${fence.text(token)} FOR UPDATE`,
        fence.values,
      );
      if (!fenced) {
        return false;
      }

      if (write.entries.length > 0) {
        await this.upsertEntries(tx, id, write.entries);
      }
      if (write.signal) {
        await this.insertSignal(tx, write.signal);
      }

      // Fields the write leaves out (undefined) keep their value.
      let wakeAt: number | null | undefined;
      if (release) {
        const missed = await this.replaceWaits(tx, id, release);
        wakeAt = missed || (toBool(fenced.cancel_requested) && write.status === 'suspended') ? write.now : release.wakeAt;
      }
      const p = new SqlParams();
      const set = [
        ...(write.status !== undefined ? [`status = ${p.text(write.status)}`] : []),
        ...(write.output !== undefined ? [`output = ${p.json(write.output)}`] : []),
        ...(write.error !== undefined ? [`error = ${p.json(write.error)}`] : []),
        ...(write.customStatus !== undefined ? [`custom_status = ${p.json(write.customStatus)}`] : []),
        ...(release ? ['lease_token = NULL', 'lease_until = NULL', `wake_at = ${p.bigint(wakeAt ?? null)}`] : []),
      ];
      if (set.length > 0) {
        await tx.execute(`UPDATE ${this.t.instances} SET ${set.join(', ')}, updated_at = ${p.bigint(write.now)} WHERE id = ${p.text(id)}`, p.values);
      }
      return true;
    });
  }

  // ---------------------------------------------------------------- internals

  /** The store's readiness (the server and the schema), then the signal lock's row: before its own first statement. */
  private async ready(): Promise<void> {
    await this.readiness.ready();
    await this.createLockRows([SIGNALS_LOCK]);
  }

  /**
   * Creates the rows of lock keys (the kit's `<schema>_locks`, one per key, never deleted) ahead of time, with the kit's
   * `ensureLockRows()`: in transactions of their own, which commit before any transaction takes those locks. When the
   * transaction that created a lock's row rolls back, MySQL makes the transactions waiting for that row deadlock
   * (1213): so a lock's first use, which may be in the application's transaction (`signal(..., { transaction })`) or in
   * one that fails, never creates its row. The rows there already are only noted, by a read that takes no lock: a
   * process's startup never waits behind the transactions holding them (a signal in the application's transaction).
   * The store's lock keys are few: `signals`, and a concurrency lock per workflow with a limit. (A store first called
   * inside the application's transaction, before `onModuleInit()` or any call outside one, creates the signal lock's row
   * there.)
   */
  private async createLockRows(keys: readonly string[]): Promise<void> {
    const missing = keys.filter((key) => !this.lockRows.has(key));
    if (missing.length === 0) {
      return;
    }

    await ensureLockRows(this.executor, this.schema, missing);
    for (const key of missing) {
      this.lockRows.add(key);
    }
  }

  private async insertInstance(tx: SqlTransaction, i: NewWorkflowInstance): Promise<{ instance: WorkflowInstance; created: boolean }> {
    // An instance deleted between the insert that met it and the read of it is created again.
    for (let attempt = 1; ; attempt++) {
      // The last signal id, read on its own: as an INSERT's subquery, REPEATABLE READ would lock the signals' last row
      // and the gap after it until the transaction ends, and hold every signal up behind the application's transaction.
      const cursor = await this.lastSignalId(tx);
      const p = new SqlParams();
      let created = true;
      try {
        await tx.execute(
          `INSERT INTO ${this.t.instances} (id, workflow, version, parent_id, parent_close, concurrency_key, rate_limit_key, priority, schedule_id,
  scheduled_at, status, input, deadline, wake_at, signal_cursor, created_at, updated_at)
VALUES (${p.text(i.id)}, ${p.text(i.workflow)}, ${p.int(i.version)}, ${p.text(i.parentId ?? null)}, ${p.text(i.parentClose ?? null)},
  ${p.text(i.concurrencyKey ?? null)}, ${p.text(i.rateLimitKey ?? null)}, ${p.int(i.priority ?? 0)}, ${p.text(i.scheduleId ?? null)},
  ${p.bigint(i.scheduledAt ?? null)}, 'pending', ${p.json(i.input)}, ${p.bigint(i.deadline)}, ${p.bigint(i.now)},
  ${p.bigint(cursor)}, ${p.bigint(i.now)}, ${p.bigint(i.now)})`,
          p.values,
        );
      } catch (error) {
        // MySQL rolled the statement back, not the transaction: the application's goes on.
        if (mysqlErrorCode(error) !== DUPLICATE_KEY) {
          throw error;
        }
        created = false;
      }

      // After a duplicate, a locking read: a plain one would read a REPEATABLE READ transaction's snapshot, which may
      // predate the row. (A row this transaction wrote is its own either way.)
      const read = new SqlParams();
      const [row] = await tx.query<Row>(
        `SELECT ${columns(INSTANCE_COLUMNS, 'i')} FROM ${this.t.instances} i WHERE i.id = ${read.text(i.id)}${created ? '' : ' FOR SHARE'}`,
        read.values,
      );
      if (row) {
        return { instance: toInstance(row), created };
      }
      if (attempt === 3) {
        throw new Error(`MySqlWorkflowStore: instance "${i.id}" was deleted each time its creation met it.`);
      }
    }
  }

  private async insertSignal(tx: SqlTransaction, s: NewWorkflowSignal): Promise<WorkflowSignalResult> {
    await lockKeys(tx, this.schema, SIGNALS_LOCK);

    // A name and dedupe id stored before make this a no-op: the unique key decides. MySQL rolls back only the statement
    // that met it, so this works in the application's transaction too; the earlier signal is read with a locking read,
    // which sees it whatever the transaction's snapshot.
    const p = new SqlParams();
    try {
      await tx.execute(
        `INSERT INTO ${this.t.signals} (name, \`key\`, dedupe_id, payload, created_at)
VALUES (${p.text(s.name)}, ${p.text(s.key)}, ${p.text(s.dedupeId)}, ${p.json(s.payload)}, ${p.bigint(s.now)})`,
        p.values,
      );
    } catch (error) {
      if (mysqlErrorCode(error) !== DUPLICATE_KEY) {
        throw error;
      }

      const read = new SqlParams();
      const [earlier] = await tx.query<Row>(
        `SELECT ${columns(['id', 'key'], 's')} FROM ${this.t.signals} s WHERE s.name = ${read.text(s.name)} AND s.dedupe_id = ${read.text(s.dedupeId)} FOR SHARE`,
        read.values,
      );
      return { id: toInt(earlier!.id)!, woken: 0, created: false, key: toText(earlier!.key) };
    }

    // The id the insert took: LAST_INSERT_ID() is the connection's, and this transaction's statements share one.
    const [inserted] = await tx.query<Row>('SELECT CAST(LAST_INSERT_ID() AS CHAR) AS id');

    // Every suspended instance waiting for it, not already due. The waits are read with a locking read, which sees the
    // latest committed ones whatever the transaction's level (under the signal lock, no suspension can commit others
    // meanwhile); the instances are then updated by primary key, so a REPEATABLE READ transaction locks no more rows
    // than those, whatever the plan. The rows the update matched are the instances woken.
    const w = new SqlParams();
    const waiting = await tx.query<Row>(
      `SELECT CAST(wt.instance_id AS CHAR) AS id FROM ${this.t.waits} wt FORCE INDEX (waits_signal)
WHERE wt.\`signal\` = ${w.text(s.name)} AND ${w.equals('wt.`key`', s.key)} FOR SHARE`,
      w.values,
    );
    const ids = [...new Set(waiting.map((row) => row.id!))];
    let woken = 0;
    for (let start = 0; start < ids.length; start += ROWS_PER_INSERT) {
      const u = new SqlParams();
      const { affectedRows } = await tx.execute(
        `UPDATE ${this.t.instances} FORCE INDEX (PRIMARY) SET wake_at = ${u.bigint(s.now)}, updated_at = ${u.bigint(s.now)}
WHERE ${u.in('id', ids.slice(start, start + ROWS_PER_INSERT))} AND status = 'suspended' AND (wake_at IS NULL OR wake_at > ${u.bigint(s.now)})`,
        u.values,
      );
      woken += affectedRows;
    }
    return { id: toInt(inserted!.id)!, woken, created: true, key: s.key };
  }

  /**
   * Replaces the instance's waits, and says whether a signal committed after the execution read its cursor matches
   * one of the new ones: then the instance stays due instead of losing it. (Run under the signal lock, shared: every
   * signal before it has committed, and none after it can commit before this transaction.)
   */
  private async replaceWaits(tx: SqlTransaction, id: string, release: NonNullable<WorkflowWrite['release']>): Promise<boolean> {
    const d = new SqlParams();
    await tx.execute(`DELETE FROM ${this.t.waits} WHERE instance_id = ${d.text(id)}`, d.values);
    if (release.waits.length === 0) {
      return false;
    }

    for (let start = 0; start < release.waits.length; start += ROWS_PER_INSERT) {
      const p = new SqlParams();
      const rows = release.waits
        .slice(start, start + ROWS_PER_INSERT)
        .map((wait, offset) => `(${p.text(id)}, ${p.int(start + offset)}, ${p.text(wait.signal)}, ${p.text(wait.key)})`);
      await tx.execute(`INSERT INTO ${this.t.waits} (instance_id, position, \`signal\`, \`key\`) VALUES ${rows.join(', ')}`, p.values);
    }

    const m = new SqlParams();
    const [hit] = await tx.query<Row>(
      `SELECT CAST(s.id AS CHAR) AS id FROM ${this.t.signals} s
WHERE s.id > ${m.bigint(release.signalCursor)} AND (${release.waits.map((wait) => `(s.name = ${m.text(wait.signal)} AND ${m.equals('s.`key`', wait.key)})`).join(' OR ')})
LIMIT 1`,
      m.values,
    );
    return hit !== undefined;
  }

  /** Journal entries by name: a new name goes last (`seq`), a known one is replaced in place. */
  private async upsertEntries(tx: SqlTransaction, id: string, entries: WorkflowJournalEntry[]): Promise<void> {
    for (let start = 0; start < entries.length; start += ROWS_PER_INSERT) {
      const p = new SqlParams();
      const rows = entries.slice(start, start + ROWS_PER_INSERT).map((entry) => `(${p.text(id)}, ${p.text(entry.name)}, ${p.json(entry)})`);
      // The row alias names the values of the row being inserted (VALUES() is deprecated). The count isn't read.
      await tx.execute(`INSERT INTO ${this.t.journal} (instance_id, name, entry) VALUES ${rows.join(', ')} AS v ON DUPLICATE KEY UPDATE entry = v.entry`, p.values);
    }
  }

  /** Deletes the journals and waits of instances deleted in this transaction. */
  private async deleteChildren(tx: SqlTransaction, ids: string[]): Promise<void> {
    for (const table of [this.t.journal, this.t.waits]) {
      const p = new SqlParams();
      await tx.execute(`DELETE FROM ${table} WHERE ${p.in('instance_id', ids)}`, p.values);
    }
  }

  private async readSchedule(db: SqlTransaction, id: string): Promise<WorkflowScheduleRecord | null> {
    const p = new SqlParams();
    const [row] = await db.query<Row>(`SELECT ${columns(SCHEDULE_COLUMNS, 's')} FROM ${this.t.schedules} s WHERE s.id = ${p.text(id)}`, p.values);
    return row ? toSchedule(row) : null;
  }

  /** Due, unleased instances of the versions the worker runs, as `alias` in a statement of `p`. */
  private isDue(p: SqlParams, request: WorkflowClaimRequest, alias: string): string {
    return `${alias}.wake_at IS NOT NULL AND ${alias}.wake_at <= ${p.bigint(request.now)} AND ${alias}.status IN ${RUNNABLE}
  AND (${alias}.lease_until IS NULL OR ${alias}.lease_until < ${p.bigint(request.now)})
  AND (${alias}.workflow, ${alias}.version) IN (${request.workflows.map((w) => `(${p.text(w.name)}, ${p.int(w.version)})`).join(', ')})`;
  }

  private async lastSignalId(db: SqlTransaction): Promise<number> {
    const [row] = await db.query<Row>(`SELECT CAST(COALESCE(MAX(s.id), 0) AS CHAR) AS id FROM ${this.t.signals} s`);
    return toInt(row?.id) ?? 0;
  }
}

/** The keys a new instance brings, checked against their columns' lengths. */
function checkInstance(i: NewWorkflowInstance): void {
  checkKey('an instance id', i.id, L.instanceId);
  checkKey("a workflow's name", i.workflow, L.workflow);
  checkKey("an instance's parent id", i.parentId, L.instanceId);
  checkKey('a concurrency key', i.concurrencyKey, L.concurrencyKey);
  checkKey('a rate-limit key', i.rateLimitKey, L.rateLimitKey);
  checkKey('a schedule id', i.scheduleId, L.scheduleId);
}

function checkSignal(s: NewWorkflowSignal): void {
  checkKey("a signal's name", s.name, L.signal);
  checkKey("a signal's key", s.key, L.signalKey);
  checkKey("a signal's id", s.dedupeId, L.dedupeId);
}

function checkEntries(entries: readonly WorkflowJournalEntry[]): void {
  for (const entry of entries) {
    checkKey("a journal entry's name (a step's, a wait's, or the engine's, such as $child: and a child's id)", entry.name, L.journalName);
  }
}
