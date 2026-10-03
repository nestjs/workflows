import { Logger, type OnModuleInit } from '@nestjs/common';
import {
  advisoryLock,
  assertReadCommittedTransaction,
  columns,
  quoteSchema,
  SqlParams,
  toBool,
  toInt,
  toJson,
  toText,
  type MigrationSqlOptions,
  type SqlExecutor,
  type SqlTransaction,
  type SqlTransactionOptions,
  type StoreReadiness,
} from '@nestjs/store-kit/postgres';
import type { WorkflowInstance, WorkflowJournalEntry, WorkflowStatus } from '../interfaces/workflow-instance.interface.js';
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
import { CANCELLABLE_STATUSES, INSTANCE_COLUMNS, RUNNABLE_STATUSES, SCHEDULE_COLUMNS, toInstance, toSchedule, type Row } from '../sql/workflow-rows.js';
import type { WorkflowStorage } from '../storage/workflow.storage.js';
import type { PostgresWorkflowStoreOptions } from './interfaces/postgres-workflow-store-options.interface.js';
import { workflowStoreSchema } from './migrations/index.js';

/** A statement that waited for a lock sees what the lock's holder committed. */
const READ_COMMITTED: SqlTransactionOptions = { isolationLevel: 'read committed' };

const RUNNABLE = `(${RUNNABLE_STATUSES.map((status) => `'${status}'`).join(', ')})`;
const CANCELLABLE = `(${CANCELLABLE_STATUSES.map((status) => `'${status}'`).join(', ')})`;

/**
 * The first-party `WorkflowStore` on PostgreSQL, through the client the application already has (`fromPg()`,
 * `fromSequelize()`, `fromDrizzle()`, `fromTypeOrm()`, `fromPrisma()`, `fromKysely()`). It keeps its tables in a
 * schema of its own (`nest_workflows` by default), which its migrations create and bring up to date, and it joins
 * the application's transactions for `start()` and `signal()` with `{ transaction }`.
 *
 * ```ts
 * @Module({
 *   imports: [WorkflowsModule.forRoot()],
 *   providers: [
 *     {
 *       provide: PostgresWorkflowStore,
 *       inject: [getDrizzleToken(), WorkflowStorage],
 *       useFactory: (db: Database, storage: WorkflowStorage) => new PostgresWorkflowStore({ executor: fromDrizzle(db) }, storage),
 *     },
 *   ],
 * })
 * export class AppModule {}
 * ```
 *
 * Given `storage`, it registers itself (`storage.registerSource(this)`), as every store does. It checks its schema,
 * or migrates it (`migrate`), in `onModuleInit` (so before the worker starts), or at its first call outside Nest.
 */
export class PostgresWorkflowStore implements WorkflowStore, OnModuleInit {
  /**
   * The SQL of the store's migrations, for teams that apply migrations with their own tool (drizzle-kit, TypeORM,
   * Prisma Migrate, Flyway...) and run the store with `migrate: false`: from version `from` (default `0`, a new
   * database) to `to` (default: the version this version of the package needs), with the bookkeeping that tells the
   * store which versions a schema has. A schema's version is `SELECT max(version) FROM <schema>.migrations`.
   * Downgrades aren't supported. From version 0 it starts with `CREATE SCHEMA IF NOT EXISTS`, which needs the CREATE
   * privilege on the database even when the schema exists: drop that statement if someone created the schema for you.
   * With `statementBreakpoints`, drizzle-kit's `--> statement-breakpoint` separates the statements, for a custom
   * drizzle-kit migration: its migrator then runs them one at a time, which PGlite needs.
   *
   * ```ts
   * // drizzle/0003_workflows.sql, created empty by `npx drizzle-kit generate --custom --name=workflows`
   * writeFileSync('drizzle/0003_workflows.sql', PostgresWorkflowStore.migrationSql({ statementBreakpoints: true }));
   * ```
   */
  static migrationSql(options: MigrationSqlOptions = {}): string {
    return workflowStoreSchema.sql(options);
  }

  /** The schema version this version of the package needs: its last migration. */
  static readonly schemaVersion = workflowStoreSchema.latest;

  private readonly logger = new Logger('WorkflowsModule');
  private readonly executor: SqlExecutor;
  private readonly schema: string;
  private readonly t: Record<'instances' | 'journal' | 'waits' | 'signals' | 'schedules' | 'rateLimits', string>;
  /**
   * Serializes signals with each other (exclusive) and with suspensions that register waits (shared), until the
   * transaction ends: signal ids become visible in id order, and a signal can't slip between a suspension's check for
   * missed signals and its commit.
   */
  private readonly signalLock: string;
  /** The schema migrated (`migrate`) or checked before the first statement: see `onModuleInit()`. */
  private readonly readiness: StoreReadiness;

  constructor(options: PostgresWorkflowStoreOptions, storage?: WorkflowStorage) {
    const resolved = workflowStoreSchema.resolveOptions(options);
    this.executor = resolved.executor;
    this.schema = resolved.schema;
    const s = quoteSchema(this.schema, 'PostgresWorkflowStore');
    this.t = {
      instances: `${s}.instances`,
      journal: `${s}.journal`,
      waits: `${s}.waits`,
      signals: `${s}.signals`,
      schedules: `${s}.schedules`,
      rateLimits: `${s}.rate_limits`,
    };
    this.signalLock = `@nestjs/workflows:${this.schema}:signals`;
    this.readiness = workflowStoreSchema.readiness({ ...resolved, logger: this.logger });
    storage?.registerSource(this);
  }

  /** Migrates the schema (`migrate`) or checks it, before the worker starts: startup fails if it can't serve. */
  async onModuleInit(): Promise<void> {
    await this.readiness.ready();
  }

  /**
   * Applies the migrations the schema hasn't had yet, whatever `migrate` says, in one transaction under an advisory
   * lock: of processes that migrate together, one applies them. Resolves to the versions it applied (`[]`: none were
   * pending).
   */
  migrate(): Promise<number[]> {
    return this.readiness.migrate();
  }

  // ---------------------------------------------------------------- instances

  async create(instance: NewWorkflowInstance): Promise<{ instance: WorkflowInstance; created: boolean }> {
    await this.readiness.ready();
    return this.insertInstance(this.executor, instance);
  }

  /** `start(..., { transaction })`: the instance commits or rolls back with the application's rows. */
  async createInTransaction(transaction: unknown, instance: NewWorkflowInstance): Promise<{ instance: WorkflowInstance; created: boolean }> {
    const tx = this.executor.wrapTransaction(transaction);
    await this.readiness.readyIn(tx);
    return this.insertInstance(tx, instance);
  }

  async get(id: string, options: { journal?: boolean } = {}): Promise<WorkflowInstanceDetails | null> {
    await this.readiness.ready();
    const p = new SqlParams();
    const journal = options.journal
      ? `, (SELECT coalesce(jsonb_agg(j.entry ORDER BY j.seq), '[]')::text FROM ${this.t.journal} j WHERE j.instance_id = i.id) AS journal`
      : '';
    const [row] = await this.executor.query<Row>(
      `SELECT ${columns(INSTANCE_COLUMNS, 'i')},
  (SELECT coalesce(json_agg(json_build_object('signal', w.signal, 'key', w.key) ORDER BY w.position), '[]')::text FROM ${this.t.waits} w WHERE w.instance_id = i.id) AS waits${journal}
FROM ${this.t.instances} i
WHERE i.id = ${p.text(id)}`,
      p.values,
    );
    if (!row) {
      return null;
    }

    const details: WorkflowInstanceDetails = { ...toInstance(row), waits: toJson(row.waits) as WorkflowInstanceDetails['waits'] };
    return options.journal ? { ...details, journal: toJson(row.journal) as WorkflowJournalEntry[] } : details;
  }

  async list(query: WorkflowListQuery): Promise<WorkflowInstance[]> {
    await this.readiness.ready();
    const p = new SqlParams();
    const where = [
      ...(query.status ? [p.in('status', query.status)] : []),
      ...(query.workflow !== undefined ? [`workflow = ${p.text(query.workflow)}`] : []),
      ...(query.version !== undefined ? [`version = ${p.int(query.version)}`] : []),
      ...(query.parentId !== undefined ? [`parent_id = ${p.text(query.parentId)}`] : []),
      ...(query.scheduleId !== undefined ? [`schedule_id = ${p.text(query.scheduleId)}`] : []),
    ];
    const rows = await this.executor.query<Row>(
      `SELECT ${columns(INSTANCE_COLUMNS)} FROM ${this.t.instances} i${where.length > 0 ? ` WHERE ${where.join(' AND ')}` : ''}
ORDER BY i.created_at, i.id LIMIT ${p.int(query.limit)} OFFSET ${p.int(query.offset)}`,
      p.values,
    );
    return rows.map(toInstance);
  }

  async requestCancel(id: string, { reason, now, terminate }: WorkflowCancelRequest): Promise<boolean> {
    await this.readiness.ready();
    // A terminate also stops a compensating instance, and follows a cancel.
    const applies = terminate ? `status IN ${RUNNABLE} AND NOT terminate_requested` : `status IN ${CANCELLABLE} AND NOT cancel_requested`;
    const p = new SqlParams();
    const at = p.bigint(now);
    const accepted = await this.executor.query<Row>(
      `UPDATE ${this.t.instances}
SET cancel_requested = true,${terminate ? ' terminate_requested = true,' : ''} cancel_reason = ${p.text(reason)}, updated_at = ${at}, wake_at = least(coalesce(wake_at, ${at}), ${at})
WHERE id = ${p.text(id)} AND ${applies}
RETURNING id`,
      p.values,
    );
    return accepted.length === 1;
  }

  /** `WorkflowClient.retry()`: one conditional update, then the journal, in one transaction. */
  async reopen(id: string, reopen: WorkflowReopen): Promise<boolean> {
    await this.readiness.ready();
    return this.executor.transaction(async (tx) => {
      // The update locks the row; of two concurrent retries, the second finds it changed.
      const p = new SqlParams();
      const now = p.bigint(reopen.now);
      const deadline = reopen.deadline === undefined ? '' : `, deadline = ${p.bigint(reopen.deadline)}`;
      const reopened = await tx.query<Row>(
        `UPDATE ${this.t.instances}
SET status = ${p.text(reopen.status)}, error = ${p.json(reopen.error)}${deadline}, wake_at = ${now}, updated_at = ${now}
WHERE id = ${p.text(id)} AND lease_token IS NULL AND status = ${p.text(reopen.expect.status)} AND runs = ${p.int(reopen.expect.runs)}
RETURNING id`,
        p.values,
      );
      if (reopened.length === 0) {
        return false;
      }

      if (reopen.entries.length > 0) {
        await this.upsertEntries(tx, id, reopen.entries);
      }
      return true;
    }, READ_COMMITTED);
  }

  /** `WorkflowClient.delete()`: its journal and waits go with it (ON DELETE CASCADE). */
  async delete(id: string, statuses: WorkflowStatus[]): Promise<boolean> {
    await this.readiness.ready();
    const p = new SqlParams();
    const deleted = await this.executor.query<Row>(`DELETE FROM ${this.t.instances} WHERE id = ${p.text(id)} AND ${p.in('status', statuses)} RETURNING id`, p.values);
    return deleted.length === 1;
  }

  // ---------------------------------------------------------------- signals

  async signal(signal: NewWorkflowSignal): Promise<WorkflowSignalResult> {
    await this.readiness.ready();
    return this.executor.transaction((tx) => this.insertSignal(tx, signal), READ_COMMITTED);
  }

  /** `signal(..., { transaction })`: the signal and its wake-ups commit with the application's rows. */
  async signalInTransaction(transaction: unknown, signal: NewWorkflowSignal): Promise<WorkflowSignalResult> {
    const tx = this.executor.wrapTransaction(transaction);
    await this.readiness.readyIn(tx);
    await assertReadCommittedTransaction(tx, {
      operation: 'signal() with { transaction }',
      reason: 'its wake-ups would miss the waits committed after its snapshot',
    });
    return this.insertSignal(tx, signal);
  }

  async signals(query: WorkflowSignalQuery): Promise<WorkflowSignalRecord[]> {
    await this.readiness.ready();
    const p = new SqlParams();
    const rows = await this.executor.query<Row>(
      `SELECT id::text AS id, name, key, payload::text AS payload, created_at::text AS created_at FROM ${this.t.signals} s
WHERE name = ${p.text(query.name)} AND ${p.equals('key', query.key)} AND id > ${p.bigint(query.afterId)} AND id <= ${p.bigint(query.upToId)}
ORDER BY s.id`,
      p.values,
    );
    return rows.map((row) => ({ id: toInt(row.id)!, name: row.name!, key: row.key, payload: toJson(row.payload), createdAt: toInt(row.created_at)! }));
  }

  // ---------------------------------------------------------------- retention

  async purge(query: WorkflowPurgeQuery): Promise<WorkflowPurgeResult> {
    await this.readiness.ready();
    const count = async (statement: string, p: SqlParams) => toInt((await this.executor.query<Row>(statement, p.values))[0]?.n) ?? 0;

    // The condition again on the deleted rows: an instance reopened since the subquery read it stays. Its journal and
    // waits go with it (ON DELETE CASCADE).
    const p1 = new SqlParams();
    const finished = `${p1.in('status', query.statuses)} AND updated_at < ${p1.bigint(query.before)}`;
    const instances = await count(
      `WITH purged AS (
  DELETE FROM ${this.t.instances}
  WHERE id IN (SELECT id FROM ${this.t.instances} WHERE ${finished} ORDER BY updated_at, id LIMIT ${p1.int(query.limit)}) AND ${finished}
  RETURNING 1
)
SELECT count(*)::text AS n FROM purged`,
      p1,
    );

    // Signals no instance can take: at or below every unfinished instance's cursor (new ones start at the newest
    // signal), old enough, and never the newest, so the last signal id never goes back.
    const p2 = new SqlParams();
    const newest = `(SELECT max(id) FROM ${this.t.signals})`;
    const signals = await count(
      `WITH pruned AS (
  DELETE FROM ${this.t.signals}
  WHERE id IN (
    SELECT id FROM ${this.t.signals}
    WHERE created_at < ${p2.bigint(query.before)} AND id < ${newest}
      AND id <= coalesce((SELECT min(signal_cursor) FROM ${this.t.instances} WHERE status IN ${RUNNABLE}), ${newest})
    ORDER BY id LIMIT ${p2.int(query.limit)}
  )
  RETURNING 1
)
SELECT count(*)::text AS n FROM pruned`,
      p2,
    );

    // Rate-limit windows that ended: again on the deleted rows, as a claim may have opened a new one meanwhile. Rows a
    // claim is locking are skipped: waiting for them, in another order than the claim's, could deadlock.
    const p3 = new SqlParams();
    const ended = `window_end < ${p3.bigint(query.before)}`;
    const rateLimits = await count(
      `WITH ended AS (
  DELETE FROM ${this.t.rateLimits}
  WHERE (workflow, key) IN (
    SELECT workflow, key FROM ${this.t.rateLimits} WHERE ${ended} ORDER BY window_end, workflow, key LIMIT ${p3.int(query.limit)} FOR UPDATE SKIP LOCKED
  ) AND ${ended}
  RETURNING 1
)
SELECT count(*)::text AS n FROM ended`,
      p3,
    );

    return { instances, signals, rateLimits };
  }

  // ---------------------------------------------------------------- schedules

  async saveSchedule(save: WorkflowScheduleSave): Promise<WorkflowScheduleRecord | null> {
    await this.readiness.ready();
    const p = new SqlParams();
    const now = p.bigint(save.now);
    const fields = {
      workflow: p.text(save.workflow),
      declared: p.bool(save.declared),
      spec: p.json(save.spec),
      input: p.json(save.input),
      paused: p.bool(save.paused),
      wake_at: p.bigint(save.wakeAt),
      state: p.json(save.state),
    };

    if (save.expectRevision === null) {
      const [created] = await this.executor.query<Row>(
        `INSERT INTO ${this.t.schedules} (id, ${Object.keys(fields).join(', ')}, revision, created_at, updated_at)
VALUES (${p.text(save.id)}, ${Object.values(fields).join(', ')}, 1, ${now}, ${now})
ON CONFLICT (id) DO NOTHING
RETURNING ${columns(SCHEDULE_COLUMNS)}`,
        p.values,
      );
      return created ? toSchedule(created) : null;
    }

    // One conditional update: of two saves that read the same revision, the second finds it changed.
    const set = Object.entries(fields).map(([column, value]) => `${column} = ${value}`);
    const [saved] = await this.executor.query<Row>(
      `UPDATE ${this.t.schedules}
SET ${set.join(', ')}, updated_at = ${now}, revision = revision + 1${save.releaseLease ? ', lease_token = NULL, lease_until = NULL' : ''}
WHERE id = ${p.text(save.id)} AND revision = ${p.int(save.expectRevision)}
RETURNING ${columns(SCHEDULE_COLUMNS)}`,
      p.values,
    );
    return saved ? toSchedule(saved) : null;
  }

  async getSchedule(id: string): Promise<WorkflowScheduleRecord | null> {
    await this.readiness.ready();
    const p = new SqlParams();
    const [row] = await this.executor.query<Row>(`SELECT ${columns(SCHEDULE_COLUMNS)} FROM ${this.t.schedules} WHERE id = ${p.text(id)}`, p.values);
    return row ? toSchedule(row) : null;
  }

  async listSchedules(query: WorkflowScheduleQuery): Promise<WorkflowScheduleRecord[]> {
    await this.readiness.ready();
    const p = new SqlParams();
    const where = [
      ...(query.workflow !== undefined ? [`workflow = ${p.text(query.workflow)}`] : []),
      ...(query.declared !== undefined ? [`declared = ${p.bool(query.declared)}`] : []),
    ];
    const rows = await this.executor.query<Row>(
      `SELECT ${columns(SCHEDULE_COLUMNS)} FROM ${this.t.schedules} s${where.length > 0 ? ` WHERE ${where.join(' AND ')}` : ''}
ORDER BY s.id LIMIT ${p.int(query.limit)} OFFSET ${p.int(query.offset)}`,
      p.values,
    );
    return rows.map(toSchedule);
  }

  async deleteSchedule(id: string, revision?: number): Promise<boolean> {
    await this.readiness.ready();
    const p = new SqlParams();
    const deleted = await this.executor.query<Row>(
      `DELETE FROM ${this.t.schedules} WHERE id = ${p.text(id)}${revision !== undefined ? ` AND revision = ${p.int(revision)}` : ''} RETURNING id`,
      p.values,
    );
    return deleted.length === 1;
  }

  async claimSchedules(request: WorkflowScheduleClaimRequest): Promise<WorkflowScheduleRecord[]> {
    await this.readiness.ready();
    // Locked; schedules another claim is locking right now are skipped instead of waited for.
    const p = new SqlParams();
    const now = p.bigint(request.now);
    const rows = await this.executor.query<Row>(
      `WITH claimed AS (
  UPDATE ${this.t.schedules}
  SET lease_token = ${p.text(request.token)}, lease_owner = ${p.text(request.owner)}, lease_until = ${p.bigint(request.leaseUntil)}
  WHERE id IN (
    SELECT id FROM ${this.t.schedules}
    WHERE NOT paused AND wake_at IS NOT NULL AND wake_at <= ${now} AND (lease_until IS NULL OR lease_until < ${now}) AND ${p.in('workflow', request.workflows)}
    ORDER BY wake_at, id LIMIT ${p.int(request.limit)}
    FOR UPDATE SKIP LOCKED
  )
  RETURNING *
)
SELECT ${columns(SCHEDULE_COLUMNS)} FROM claimed ORDER BY claimed.wake_at, claimed.id`,
      p.values,
    );
    return rows.map(toSchedule);
  }

  async writeSchedule(id: string, token: string, write: WorkflowScheduleWrite): Promise<boolean> {
    await this.readiness.ready();
    const p = new SqlParams();
    const written = await this.executor.query<Row>(
      `UPDATE ${this.t.schedules}
SET state = ${p.json(write.state)}, wake_at = ${p.bigint(write.wakeAt)}, revision = revision + 1, updated_at = ${p.bigint(write.now)}${write.release ? ', lease_token = NULL, lease_until = NULL' : ''}
WHERE id = ${p.text(id)} AND lease_token = ${p.text(token)}
RETURNING id`,
      p.values,
    );
    return written.length === 1;
  }

  // ---------------------------------------------------------------- the worker

  async claim(request: WorkflowClaimRequest): Promise<WorkflowClaim> {
    await this.readiness.ready();
    if (request.limits?.length || request.rateLimits?.length) {
      return this.executor.transaction((tx) => this.claimWithin(tx, request), READ_COMMITTED);
    }

    // Locked; rows another claim is locking right now are skipped instead of waited for.
    const p = new SqlParams();
    const due = `SELECT i.id FROM ${this.t.instances} i WHERE ${this.isDue(p, request, 'i')}
ORDER BY i.priority, i.wake_at, i.created_at, i.id LIMIT ${p.int(request.limit)}
FOR UPDATE SKIP LOCKED`;
    return { instances: await this.lease(this.executor, request, due, p), lastSignalId: await this.lastSignalId(this.executor) };
  }

  /**
   * A claim under concurrency or rate limits. Claims of a workflow with a concurrency limit take its lock first (in
   * name order, so two claims never wait for each other's), so they count the slots live leases hold and lease the
   * instances that fit one after the other: two never both take the last slot. Rate-limit windows are counted again
   * under their rows' locks, once the instances are picked (see `takeRoom()`).
   */
  private async claimWithin(tx: SqlTransaction, request: WorkflowClaimRequest): Promise<WorkflowClaim> {
    const limits = request.limits ?? [];
    const rates = request.rateLimits ?? [];
    await advisoryLock(tx, limits.map((limit) => `@nestjs/workflows:${this.schema}:concurrency:${limit.workflow}`));

    // Candidates with no room at all are passed over; then each concurrency key's first, as many as its free slots; of
    // those, each rate key's, as many as its window has room for; of those, each workflow's, as many as both its free
    // slots and its window allow. A full key is passed over, not waited behind. The windows read here are a snapshot,
    // re-counted under their locks in takeRoom().
    const p = new SqlParams();
    const now = p.bigint(request.now);
    const limitRows = limits.map((l) => ({ workflow: l.workflow, total: l.limit, per_key: l.perKey }));
    const rateRows = rates.map((r) => ({ workflow: r.workflow, total: r.limit?.max ?? null, per_key: r.perKey?.max ?? null }));
    const rows = await tx.query<Row>(
      `WITH limits AS (
  SELECT * FROM jsonb_to_recordset(${p.json(limitRows)}) AS l(workflow text, total int, per_key int)
),
rates AS (
  SELECT * FROM jsonb_to_recordset(${p.json(rateRows)}) AS r(workflow text, total int, per_key int)
),
held AS (
  SELECT i.workflow AS workflow, i.concurrency_key AS key, count(*)::int AS n
  FROM ${this.t.instances} i
  WHERE i.lease_until >= ${now} AND i.workflow IN (SELECT workflow FROM limits)
  GROUP BY 1, 2
),
used AS (
  SELECT rl.workflow AS workflow, rl.key AS key, rl.count AS n
  FROM ${this.t.rateLimits} rl
  WHERE rl.window_end > ${now} AND rl.workflow IN (SELECT workflow FROM rates)
),
due AS (
  SELECT i.id AS id, i.workflow AS workflow, i.concurrency_key AS key, i.rate_limit_key AS rate_key, i.priority AS priority,
    i.wake_at AS wake_at, i.created_at AS created_at,
    row_number() OVER (PARTITION BY i.workflow, i.concurrency_key ORDER BY i.priority, i.wake_at, i.created_at, i.id) AS key_rank
  FROM ${this.t.instances} i
  LEFT JOIN limits l ON l.workflow = i.workflow
  LEFT JOIN rates r ON r.workflow = i.workflow
  WHERE ${this.isDue(p, request, 'i')}
    AND (l.total IS NULL OR coalesce((SELECT sum(n) FROM held h WHERE h.workflow = l.workflow), 0) < l.total)
    AND (l.per_key IS NULL OR i.concurrency_key IS NULL
      OR coalesce((SELECT n FROM held h WHERE h.workflow = l.workflow AND h.key = i.concurrency_key), 0) < l.per_key)
    AND (r.total IS NULL OR coalesce((SELECT n FROM used u WHERE u.workflow = r.workflow AND u.key = ''), 0) < r.total)
    AND (r.per_key IS NULL OR i.rate_limit_key IS NULL
      OR coalesce((SELECT n FROM used u WHERE u.workflow = r.workflow AND u.key = i.rate_limit_key), 0) < r.per_key)
),
fits_key AS (
  SELECT d.*, row_number() OVER (PARTITION BY d.workflow, d.rate_key ORDER BY d.priority, d.wake_at, d.created_at, d.id) AS rate_key_rank
  FROM due d
  LEFT JOIN limits l ON l.workflow = d.workflow
  WHERE l.per_key IS NULL OR d.key IS NULL
    OR d.key_rank <= l.per_key - coalesce((SELECT n FROM held h WHERE h.workflow = d.workflow AND h.key = d.key), 0)
),
fits_rate_key AS (
  SELECT f.*, row_number() OVER (PARTITION BY f.workflow ORDER BY f.priority, f.wake_at, f.created_at, f.id) AS workflow_rank
  FROM fits_key f
  LEFT JOIN rates r ON r.workflow = f.workflow
  WHERE r.per_key IS NULL OR f.rate_key IS NULL
    OR f.rate_key_rank <= r.per_key - coalesce((SELECT n FROM used u WHERE u.workflow = f.workflow AND u.key = f.rate_key), 0)
)
SELECT f.id FROM fits_rate_key f
LEFT JOIN limits l ON l.workflow = f.workflow
LEFT JOIN rates r ON r.workflow = f.workflow
WHERE (l.total IS NULL OR f.workflow_rank <= l.total - coalesce((SELECT sum(n)::int FROM held h WHERE h.workflow = f.workflow), 0))
  AND (r.total IS NULL OR f.workflow_rank <= r.total - coalesce((SELECT n FROM used u WHERE u.workflow = f.workflow AND u.key = ''), 0))
ORDER BY f.priority, f.wake_at, f.created_at, f.id
LIMIT ${p.int(request.limit)}`,
      p.values,
    );
    if (rows.length === 0) {
      return { instances: [], lastSignalId: await this.lastSignalId(tx) };
    }

    const lock = new SqlParams();
    const picked = await tx.query<Row>(
      `SELECT i.id, i.workflow, i.rate_limit_key FROM ${this.t.instances} i
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
    if (granted.length === 0) {
      return { instances: [], lastSignalId: await this.lastSignalId(tx) };
    }

    const p2 = new SqlParams();
    const instances = await this.lease(tx, request, `SELECT id FROM ${this.t.instances} WHERE ${p2.in('id', granted)}`, p2);
    return { instances, lastSignalId: await this.lastSignalId(tx) };
  }

  /**
   * Of `picked` (in claim order), the instances their rate-limit windows have room for, recorded in the windows. Each
   * window's row is inserted or locked first, in a fixed order: the count read under the lock is exact (a concurrent
   * claim of the same window waits for this one), and a purge can't delete the row in between.
   */
  private async takeRoom(tx: SqlTransaction, request: WorkflowClaimRequest, picked: PickedInstance[]): Promise<string[]> {
    const claim = new RateWindowClaim(request, picked);
    if (claim.windows.length === 0) {
      return picked.map((instance) => instance.id);
    }

    const p = new SqlParams();
    const locked = await tx.query<Row>(
      `INSERT INTO ${this.t.rateLimits} AS rl (workflow, key, window_end, count)
VALUES ${claim.windows.map(({ workflow, key }) => `(${p.text(workflow)}, ${p.text(key)}, 0, 0)`).join(', ')}
ON CONFLICT (workflow, key) DO UPDATE SET count = rl.count
RETURNING rl.workflow, rl.key, rl.window_end::text AS window_end, rl.count::text AS count`,
      p.values,
    );
    const { granted, changed } = claim.grant(locked.map((row) => ({ workflow: row.workflow!, key: row.key!, windowEnd: toInt(row.window_end)!, count: toInt(row.count)! })));

    if (changed.length > 0) {
      const u = new SqlParams();
      const values = changed.map((w) => ({ workflow: w.workflow, key: w.key, window_end: w.windowEnd, count: w.count }));
      await tx.query(
        `UPDATE ${this.t.rateLimits} rl SET window_end = v.window_end, count = v.count
FROM jsonb_to_recordset(${u.json(values)}) AS v(workflow text, key text, window_end bigint, count int)
WHERE rl.workflow = v.workflow AND rl.key = v.key`,
        u.values,
      );
    }
    return granted;
  }

  /** Leases the instances `due` (a subquery of `p`'s statement) selects and locks. */
  private async lease(db: SqlTransaction, request: WorkflowClaimRequest, due: string, p: SqlParams): Promise<WorkflowInstance[]> {
    const rows = await db.query<Row>(
      `WITH claimed AS (
  UPDATE ${this.t.instances}
  SET lease_token = ${p.text(request.token)}, lease_owner = ${p.text(request.owner)}, lease_until = ${p.bigint(request.leaseUntil)},
    runs = runs + 1, updated_at = ${p.bigint(request.now)}, status = CASE WHEN status = 'compensating' THEN status ELSE 'running' END
  WHERE id IN (${due})
  RETURNING *
)
SELECT ${columns(INSTANCE_COLUMNS)} FROM claimed ORDER BY claimed.priority, claimed.wake_at, claimed.created_at, claimed.id`,
      p.values,
    );
    return rows.map(toInstance);
  }

  async renew(id: string, token: string, leaseUntil: number): Promise<{ cancelRequested: boolean; terminateRequested: boolean } | null> {
    await this.readiness.ready();
    const p = new SqlParams();
    const [row] = await this.executor.query<Row>(
      `UPDATE ${this.t.instances} SET lease_until = ${p.bigint(leaseUntil)} WHERE id = ${p.text(id)} AND lease_token = ${p.text(token)}
RETURNING cancel_requested::text AS cancel_requested, terminate_requested::text AS terminate_requested`,
      p.values,
    );
    return row ? { cancelRequested: toBool(row.cancel_requested), terminateRequested: toBool(row.terminate_requested) } : null;
  }

  async write(id: string, token: string, write: WorkflowWrite): Promise<boolean> {
    await this.readiness.ready();
    return this.executor.transaction(async (tx) => {
      const release = write.release;
      if (write.signal) {
        await advisoryLock(tx, this.signalLock);
      } else if (release && release.waits.length > 0) {
        await advisoryLock(tx, this.signalLock, { shared: true });
      }

      // The fence: only the lease holder writes, and the row stays locked until commit.
      const fence = new SqlParams();
      const [fenced] = await tx.query<Row>(
        `SELECT cancel_requested::text AS cancel_requested FROM ${this.t.instances} WHERE id = ${fence.text(id)} AND lease_token = ${fence.text(token)} FOR UPDATE`,
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
      const p = new SqlParams();
      const set = [
        ...(write.status !== undefined ? [`status = ${p.text(write.status)}`] : []),
        ...(write.output !== undefined ? [`output = ${p.json(write.output)}`] : []),
        ...(write.error !== undefined ? [`error = ${p.json(write.error)}`] : []),
        ...(write.customStatus !== undefined ? [`custom_status = ${p.json(write.customStatus)}`] : []),
      ];
      if (release) {
        const missed = await this.replaceWaits(tx, id, release);
        const wakeNow = missed || (toBool(fenced.cancel_requested) && write.status === 'suspended');
        set.push('lease_token = NULL', 'lease_until = NULL', `wake_at = ${p.bigint(wakeNow ? write.now : release.wakeAt)}`);
      }
      if (set.length > 0) {
        await tx.query(`UPDATE ${this.t.instances} SET ${set.join(', ')}, updated_at = ${p.bigint(write.now)} WHERE id = ${p.text(id)}`, p.values);
      }
      return true;
    }, READ_COMMITTED);
  }

  // ---------------------------------------------------------------- internals

  private async insertInstance(db: SqlTransaction, i: NewWorkflowInstance): Promise<{ instance: WorkflowInstance; created: boolean }> {
    // An instance deleted between the insert that met it and the read of it is created again.
    for (let attempt = 1; ; attempt++) {
      const p = new SqlParams();
      const now = p.bigint(i.now);
      const [created] = await db.query<Row>(
        `INSERT INTO ${this.t.instances} (id, workflow, version, parent_id, parent_close, concurrency_key, rate_limit_key, priority, schedule_id,
  scheduled_at, status, input, deadline, wake_at, signal_cursor, created_at, updated_at)
VALUES (${p.text(i.id)}, ${p.text(i.workflow)}, ${p.int(i.version)}, ${p.text(i.parentId ?? null)}, ${p.text(i.parentClose ?? null)},
  ${p.text(i.concurrencyKey ?? null)}, ${p.text(i.rateLimitKey ?? null)}, ${p.int(i.priority ?? 0)}, ${p.text(i.scheduleId ?? null)},
  ${p.bigint(i.scheduledAt ?? null)}, 'pending', ${p.json(i.input)}, ${p.bigint(i.deadline)}, ${now},
  (SELECT coalesce(max(id), 0) FROM ${this.t.signals}), ${now}, ${now})
ON CONFLICT (id) DO NOTHING
RETURNING ${columns(INSTANCE_COLUMNS)}`,
        p.values,
      );
      if (created) {
        return { instance: toInstance(created), created: true };
      }

      const read = new SqlParams();
      const [existing] = await db.query<Row>(`SELECT ${columns(INSTANCE_COLUMNS)} FROM ${this.t.instances} WHERE id = ${read.text(i.id)}`, read.values);
      if (existing) {
        return { instance: toInstance(existing), created: false };
      }
      if (attempt === 3) {
        throw new Error(`PostgresWorkflowStore: instance "${i.id}" was deleted each time its creation met it.`);
      }
    }
  }

  private async insertSignal(tx: SqlTransaction, s: NewWorkflowSignal): Promise<WorkflowSignalResult> {
    await advisoryLock(tx, this.signalLock);

    // A name and dedupe id stored before make this a no-op: the unique constraint decides, with nothing to catch,
    // because an error would abort the application's transaction.
    const p = new SqlParams();
    const [signal] = await tx.query<Row>(
      `INSERT INTO ${this.t.signals} (name, key, dedupe_id, payload, created_at)
VALUES (${p.text(s.name)}, ${p.text(s.key)}, ${p.text(s.dedupeId)}, ${p.json(s.payload)}, ${p.bigint(s.now)})
ON CONFLICT (name, dedupe_id) DO NOTHING
RETURNING id::text AS id`,
      p.values,
    );
    if (!signal) {
      const read = new SqlParams();
      const [earlier] = await tx.query<Row>(
        `SELECT id::text AS id, key FROM ${this.t.signals} WHERE name = ${read.text(s.name)} AND dedupe_id = ${read.text(s.dedupeId)}`,
        read.values,
      );
      return { id: toInt(earlier!.id)!, woken: 0, created: false, key: toText(earlier!.key) };
    }

    const w = new SqlParams();
    const now = w.bigint(s.now);
    const [woken] = await tx.query<Row>(
      `WITH woken AS (
  UPDATE ${this.t.instances} SET wake_at = ${now}, updated_at = ${now}
  WHERE status = 'suspended' AND (wake_at IS NULL OR wake_at > ${now})
    AND id IN (SELECT instance_id FROM ${this.t.waits} WHERE signal = ${w.text(s.name)} AND ${w.equals('key', s.key)})
  RETURNING 1
)
SELECT count(*)::text AS n FROM woken`,
      w.values,
    );
    return { id: toInt(signal.id)!, woken: toInt(woken?.n) ?? 0, created: true, key: s.key };
  }

  /**
   * Replaces the instance's waits, and says whether a signal committed after the execution read its cursor matches
   * one of the new ones: then the instance stays due instead of losing it.
   */
  private async replaceWaits(tx: SqlTransaction, id: string, release: NonNullable<WorkflowWrite['release']>): Promise<boolean> {
    const d = new SqlParams();
    await tx.query(`DELETE FROM ${this.t.waits} WHERE instance_id = ${d.text(id)}`, d.values);
    if (release.waits.length === 0) {
      return false;
    }

    const p = new SqlParams();
    const instance = p.text(id);
    await tx.query(
      `INSERT INTO ${this.t.waits} (instance_id, position, signal, key)
VALUES ${release.waits.map((wait, position) => `(${instance}, ${position}, ${p.text(wait.signal)}, ${p.text(wait.key)})`).join(', ')}`,
      p.values,
    );

    const m = new SqlParams();
    const matches = release.waits.map((wait) => `(name = ${m.text(wait.signal)} AND ${m.equals('key', wait.key)})`);
    const [hit] = await tx.query<Row>(
      `SELECT id::text AS id FROM ${this.t.signals} WHERE id > ${m.bigint(release.signalCursor)} AND (${matches.join(' OR ')}) LIMIT 1`,
      m.values,
    );
    return hit !== undefined;
  }

  /** Journal entries by name: a new name goes last (`seq`), a known one is replaced in place. */
  private async upsertEntries(tx: SqlTransaction, id: string, entries: WorkflowJournalEntry[]): Promise<void> {
    const p = new SqlParams();
    const instance = p.text(id);
    await tx.query(
      `INSERT INTO ${this.t.journal} (instance_id, name, entry)
VALUES ${entries.map((entry) => `(${instance}, ${p.text(entry.name)}, ${p.json(entry)})`).join(', ')}
ON CONFLICT (instance_id, name) DO UPDATE SET entry = excluded.entry`,
      p.values,
    );
  }

  /** Due, unleased instances of the versions the worker runs, as `alias` in a statement of `p`. */
  private isDue(p: SqlParams, request: WorkflowClaimRequest, alias: string): string {
    const now = p.bigint(request.now);
    const versions = request.workflows.map((w) => `(${p.text(w.name)}, ${p.int(w.version)})`).join(', ');
    return `${alias}.wake_at IS NOT NULL AND ${alias}.wake_at <= ${now} AND ${alias}.status IN ${RUNNABLE}
  AND (${alias}.lease_until IS NULL OR ${alias}.lease_until < ${now}) AND (${alias}.workflow, ${alias}.version) IN (VALUES ${versions})`;
  }

  private async lastSignalId(db: SqlTransaction): Promise<number> {
    const [row] = await db.query<Row>(`SELECT coalesce(max(id), 0)::text AS id FROM ${this.t.signals}`);
    return toInt(row?.id) ?? 0;
  }
}

