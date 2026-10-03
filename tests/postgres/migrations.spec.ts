/**
 * PostgresWorkflowStore's migrations, on the kit's StoreSchema (@nestjs/store-kit's own suite covers its machinery with
 * a schema of its own): a new database, a rerun, processes migrating at once, `migrationSql()` against what `migrate()`
 * applies and under which lock, the fixture's indexes, a colliding table, a schema behind the code (and ahead of it),
 * the production default, the default isolation, the options, and a first call inside the application's transaction.
 */
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { drizzle } from 'drizzle-orm/pglite';
import { migrate as drizzleMigrate } from 'drizzle-orm/pglite/migrator';
import { PGlite } from '@electric-sql/pglite';
import pg from 'pg';
import type { SqlExecutor as AnySqlExecutor } from '@nestjs/store-kit';
import { fromDrizzle, fromPg, PostgresWorkflowStore, WorkflowSchemaError, type SqlExecutor, type SqlTransaction } from '../../lib/postgres/index.js';
import { workflowStoreSchema } from '../../lib/postgres/migrations/index.js';
import { endPool } from '../support/postgres.js';
import { testDatabase } from './support.js';

const { database, reason } = await testDatabase('pgstore_migrations');
const pools: pg.Pool[] = [];

/** In a describe of tests that run on PostgreSQL: skips them, with the reason, where there's none. */
const onPostgres = () =>
  beforeEach((context) => {
    if (reason) {
      context.skip(reason);
    }
  });

// endPool, not pool.end(): end() resolves once each client has been asked to go, not once its socket has. The
// database is dropped WITH (FORCE) right after this hook, which terminates whatever backend is still attached, and
// a client ending at that moment gets the FATAL with nothing listening for it — an unhandled error, and a red run.
afterAll(async () => {
  await Promise.all(pools.map((pool) => endPool(pool)));
});

/** A pool of its own, as each process has. */
const pool = () => {
  const opened = new pg.Pool({ connectionString: database!.url, max: 2 });
  pools.push(opened);
  return opened;
};

const store = (schema: string, options: { migrate?: boolean; executor?: SqlExecutor<'postgres'> } = {}) =>
  new PostgresWorkflowStore({ executor: options.executor ?? fromPg(pool()), schema, migrate: options.migrate });

/** An executor that records every statement it runs, and its parameters. */
function recording(executor: SqlExecutor<'postgres'>): { executor: SqlExecutor<'postgres'>; statements: Array<{ text: string; params?: readonly unknown[] }> } {
  const statements: Array<{ text: string; params?: readonly unknown[] }> = [];
  const record = (tx: SqlTransaction): SqlTransaction => ({
    query: (text, params) => {
      statements.push({ text, params });
      return tx.query(text, params);
    },
  });
  return {
    statements,
    executor: {
      dialect: executor.dialect,
      query: (text, params) => {
        statements.push({ text, params });
        return executor.query(text, params);
      },
      transaction: (work, options) => executor.transaction((tx) => work(record(tx)), options),
      wrapTransaction: (transaction) => record(executor.wrapTransaction(transaction)),
    },
  };
}

/** The statements that change the schema: not the lock, and not the reads of what it has. */
const changes = (statements: Array<{ text: string }>) => statements.map((statement) => statement.text).filter((text) => !text.startsWith('SELECT'));

const rows = async (sql: string, params: unknown[] = []) => (await database!.admin.query(sql, params)).rows;

/** Everything about a schema's tables that migrations define, with the schema's name taken out. */
async function catalog(schema: string) {
  const anonymize = (value: unknown) => JSON.parse(JSON.stringify(value).replaceAll(schema, '<schema>'));
  return anonymize({
    columns: await rows(
      `SELECT table_name, column_name, ordinal_position, data_type, is_nullable, column_default, is_identity, identity_generation
       FROM information_schema.columns WHERE table_schema = $1 ORDER BY table_name, ordinal_position`,
      [schema],
    ),
    constraints: await rows(
      `SELECT conrelid::regclass::text AS table, conname, pg_get_constraintdef(oid) AS definition
       FROM pg_constraint WHERE connamespace = $1::regnamespace ORDER BY 1, 2`,
      [schema],
    ),
    indexes: await rows('SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = $1 ORDER BY tablename, indexname', [schema]),
    versions: await rows(`SELECT version, name FROM "${schema}".migrations ORDER BY version`),
  });
}

const tables = async (schema: string) =>
  (await rows('SELECT table_name FROM information_schema.tables WHERE table_schema = $1 ORDER BY table_name', [schema])).map((row) => row.table_name);

describe('migrate()', () => {
  onPostgres();

  it('creates the schema, its tables and the version record on a new database, and applies nothing the second time', async () => {
    const first = store('m_fresh');
    expect(await first.migrate()).toEqual([1]);
    expect(await tables('m_fresh')).toEqual(['instances', 'journal', 'migrations', 'rate_limits', 'schedules', 'signals', 'waits']);
    expect(await rows('SELECT version, name FROM m_fresh.migrations')).toEqual([{ version: 1, name: 'initial' }]);
    expect(PostgresWorkflowStore.schemaVersion).toBe(1);

    expect(await first.migrate()).toEqual([]);
    expect(await store('m_fresh').migrate()).toEqual([]);
    expect(await rows('SELECT count(*)::int AS n FROM m_fresh.migrations')).toEqual([{ n: 1 }]);
    await expect(store('m_fresh', { migrate: false }).onModuleInit()).resolves.toBeUndefined();
  });

  it('applies the migrations once when processes start together, each on its own connections', async () => {
    const stores = Array.from({ length: 8 }, () => store('m_together'));
    const applied = await Promise.all(stores.map((s) => s.migrate()));
    expect(applied.filter((versions) => versions.length > 0)).toEqual([[1]]);

    const starting = Array.from({ length: 8 }, () => store('m_starting'));
    await Promise.all(starting.map((s) => s.onModuleInit()));
    expect(await rows('SELECT version FROM m_starting.migrations')).toEqual([{ version: 1 }]);
    expect(await starting[3]!.create({ id: 'i-1', workflow: 'w', version: 1, input: null, deadline: null, now: 1 })).toMatchObject({ created: true });
  });

  it('runs the statements migrationSql() prints under the lock it has always taken, and a database migrated with them is the same as one migrate() made', async () => {
    const recorder = recording(fromPg(pool()));
    await store('m_migrated', { executor: recorder.executor }).migrate();
    expect(recorder.statements[0]).toEqual({ text: 'SELECT pg_advisory_xact_lock(hashtext($1::text))::text AS locked', params: ['@nestjs/workflows:migrate:m_migrated'] });
    const script = PostgresWorkflowStore.migrationSql({ schema: 'm_migrated' });
    expect(script).toBe(
      `-- @nestjs/workflows: PostgresWorkflowStore's schema "m_migrated", from version 0 to 1.\n-- Run it in one transaction.\n\n` +
        `${changes(recorder.statements).map((statement) => `${statement};`).join('\n\n')}\n`,
    );
    expect(changes(recorder.statements)).toEqual(workflowStoreSchema.statements({ schema: 'm_migrated' }));

    // As a team applies it with its own tool: one script, in one transaction.
    const client = await pool().connect();
    try {
      await client.query(`BEGIN; ${PostgresWorkflowStore.migrationSql({ schema: 'm_script' })} COMMIT;`);
    } finally {
      client.release();
    }
    expect(await catalog('m_script')).toEqual(await catalog('m_migrated'));
    await expect(store('m_script', { migrate: false }).onModuleInit()).resolves.toBeUndefined();
  });

  it("has every index and unique constraint of the tutorial's hand-written store (tests/fixtures/drizzle)", async () => {
    await store('m_indexes').migrate();
    const fixture = readFileSync(new URL('../fixtures/drizzle/0001_workflows.sql', import.meta.url), 'utf8');
    const names = [...fixture.matchAll(/(?:CREATE INDEX|CONSTRAINT) "workflow_(\w+?)"/g)]
      .map((match) => match[1]!)
      .filter((name) => !name.endsWith('_fk'))
      .map((name) => name.replace(/_(workflow_key|instance_id_position)_pk$/, '_pkey'));
    const indexes = (await rows('SELECT indexname FROM pg_indexes WHERE schemaname = $1', ['m_indexes'])).map((row) => row.indexname);
    expect(names).toHaveLength(15);
    expect(indexes).toEqual(expect.arrayContaining(names));

    // A deleted instance takes its journal and waits with it.
    const foreignKeys = await rows("SELECT conrelid::regclass::text AS table, pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE connamespace = 'm_indexes'::regnamespace AND contype = 'f' ORDER BY 1");
    expect(foreignKeys).toEqual([
      { table: 'm_indexes.journal', definition: 'FOREIGN KEY (instance_id) REFERENCES m_indexes.instances(id) ON DELETE CASCADE' },
      { table: 'm_indexes.waits', definition: 'FOREIGN KEY (instance_id) REFERENCES m_indexes.instances(id) ON DELETE CASCADE' },
    ]);
  });

  it("fails on a schema that has other tables of the store's names, and creates nothing", async () => {
    await rows('CREATE SCHEMA m_taken');
    await rows('CREATE TABLE m_taken.instances (id serial PRIMARY KEY)');
    const error = await store('m_taken').migrate().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(WorkflowSchemaError);
    expect(error).toMatchObject({ schema: 'm_taken', version: 0, requiredVersion: 1, cause: { message: 'relation "instances" already exists' } });
    expect((error as Error).message).toBe(
      'PostgresWorkflowStore: migrating schema "m_taken" from version 0 to 1 failed, and nothing was applied: relation "instances" already exists',
    );
    expect(await tables('m_taken')).toEqual(['instances']);
  });
});

describe('a schema behind the code', () => {
  onPostgres();

  it('fails the startup (and every call) with a WorkflowSchemaError that says how to migrate, changing nothing', async () => {
    const behind = store('m_behind', { migrate: false });
    const error = await behind.onModuleInit().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(WorkflowSchemaError);
    expect(error).toMatchObject({ name: 'WorkflowSchemaError', schema: 'm_behind', version: 0, requiredVersion: 1 });
    expect((error as Error).message).toBe(
      'PostgresWorkflowStore: schema "m_behind" is at version 0, and this version of @nestjs/workflows needs version 1. Apply its migrations: ' +
        'set `migrate: true` to apply them at startup, run `npx nest-workflows migrate --url <database url> --schema m_behind`, ' +
        "or apply `PostgresWorkflowStore.migrationSql({ schema: 'm_behind', from: 0 })` with your migration tool.",
    );
    await expect(behind.get('any')).rejects.toThrow(WorkflowSchemaError);
    expect(await rows("SELECT nspname FROM pg_namespace WHERE nspname = 'm_behind'")).toEqual([]);

    // Checked again at the next call: migrated meanwhile, it serves.
    await store('m_behind').migrate();
    expect(await behind.get('any')).toBeNull();
  });

  it('serves a schema ahead of the code, which a newer version of the package migrated during a rolling deploy', async () => {
    await store('m_ahead').migrate();
    await rows("INSERT INTO m_ahead.migrations (version, name) VALUES (2, 'newer')");

    const older = store('m_ahead', { migrate: false });
    await expect(older.onModuleInit()).resolves.toBeUndefined();
    expect(await older.create({ id: 'i-1', workflow: 'w', version: 1, input: null, deadline: null, now: 1 })).toMatchObject({ created: true });
    expect(await store('m_ahead').migrate()).toEqual([]);
  });
});

describe('options', () => {
  onPostgres();

  it("don't migrate by default with NODE_ENV=production, and do otherwise", async () => {
    const previous = process.env.NODE_ENV;
    try {
      process.env.NODE_ENV = 'production';
      await expect(store('m_production').onModuleInit()).rejects.toThrow(WorkflowSchemaError);
      process.env.NODE_ENV = 'development';
      await expect(store('m_production').onModuleInit()).resolves.toBeUndefined();
      delete process.env.NODE_ENV;
      await expect(store('m_unset').onModuleInit()).resolves.toBeUndefined();
    } finally {
      process.env.NODE_ENV = previous;
    }
    expect(await tables('m_production')).toContain('instances');
  });

  it("refuse connections whose default isolation isn't READ COMMITTED, before migrating (its races would fail)", async () => {
    const serializable = new pg.Pool({ connectionString: database!.url, max: 1, options: '-c default_transaction_isolation=serializable' });
    pools.push(serializable);
    const store = new PostgresWorkflowStore({ executor: fromPg(serializable), schema: 'm_isolation' });
    await expect(store.onModuleInit()).rejects.toThrow(
      "PostgresWorkflowStore needs the database's default transaction isolation to be READ COMMITTED (PostgreSQL's default), not serializable: its statements race each other",
    );
    await expect(store.get('any')).rejects.toThrow('not serializable');
    expect(await tables('m_isolation')).toEqual([]);
  });

  it('take a schema name of letters, digits and underscores, quoted in every statement', async () => {
    for (const schema of ['bad-name', '1st', '', 'x'.repeat(64), 'a"b', 'a$1']) {
      expect(() => new PostgresWorkflowStore({ executor: fromPg(pool()), schema })).toThrow(TypeError);
      expect(() => PostgresWorkflowStore.migrationSql({ schema })).toThrow(`PostgresWorkflowStore: invalid schema ${JSON.stringify(schema)}.`);
    }
    expect(await store('Mixed_Case').migrate()).toEqual([1]);
    expect(await tables('Mixed_Case')).toContain('instances');
  });

  it('refuse an executor that is none or of another database, and a migrate that is no boolean', () => {
    expect(() => new PostgresWorkflowStore({ executor: {} as SqlExecutor<'postgres'> })).toThrow('PostgresWorkflowStore: `executor` must be a SqlExecutor');
    const executor = fromPg(pool());
    const mysql = { dialect: 'mysql', query: executor.query.bind(executor), transaction: executor.transaction.bind(executor), wrapTransaction: executor.wrapTransaction.bind(executor) };
    // A MySQL executor is a compile error first (the options take SqlExecutor<'postgres'>), then a TypeError.
    // @ts-expect-error
    expect(() => new PostgresWorkflowStore({ executor: mysql as AnySqlExecutor<'mysql'> })).toThrow(
      "PostgresWorkflowStore runs on PostgreSQL, and `executor` is a MySQL executor: import the executor from '@nestjs/workflows/postgres' (fromPg, fromDrizzle, fromTypeOrm, fromPrisma or fromKysely).",
    );
    expect(() => new PostgresWorkflowStore({ executor: fromPg(pool()), migrate: 'yes' as unknown as boolean })).toThrow(
      'PostgresWorkflowStore: `migrate` must be true or false, not "yes".',
    );
  });
});

describe('migrationSql()', () => {
  it('prints the default schema from a new database, a range of versions, and never a downgrade', () => {
    const script = PostgresWorkflowStore.migrationSql();
    expect(script).toMatch(/^-- @nestjs\/workflows: PostgresWorkflowStore's schema "nest_workflows", from version 0 to 1\.\n/);
    expect(script).toContain('CREATE SCHEMA IF NOT EXISTS "nest_workflows";');
    expect(script).toContain(`INSERT INTO "nest_workflows".migrations (version, name) VALUES (1, 'initial');`);
    expect(PostgresWorkflowStore.migrationSql({ from: 1 })).not.toContain('CREATE');

    expect(() => PostgresWorkflowStore.migrationSql({ from: 1, to: 0 })).toThrow(RangeError);
    expect(() => PostgresWorkflowStore.migrationSql({ from: 1, to: 0 })).toThrow("downgrades aren't supported");
    expect(() => PostgresWorkflowStore.migrationSql({ to: 2 })).toThrow('no migrations lead from version 0 to 2');
    expect(() => PostgresWorkflowStore.migrationSql({ from: -1 })).toThrow(RangeError);
  });
});

describe("drizzle-kit's statement breakpoints, on PGlite", () => {
  it("runs migrationSql({ statementBreakpoints: true }) through Drizzle's migrator, one statement at a time, and the store serves on it", async () => {
    const [migrated, byDrizzle] = [new PGlite(), new PGlite()];
    const folder = mkdtempSync(join(tmpdir(), 'wft-drizzle-'));
    mkdirSync(join(folder, 'meta'));
    writeFileSync(join(folder, '0000_workflows.sql'), PostgresWorkflowStore.migrationSql({ statementBreakpoints: true }));
    writeFileSync(
      join(folder, 'meta', '_journal.json'),
      JSON.stringify({ version: '7', dialect: 'postgresql', entries: [{ idx: 0, version: '7', when: 1790000000000, tag: '0000_workflows', breakpoints: true }] }),
    );
    try {
      await new PostgresWorkflowStore({ executor: fromDrizzle(drizzle(migrated)) }).migrate();
      const db = drizzle(byDrizzle);
      await drizzleMigrate(db, { migrationsFolder: folder });

      const indexes = (pglite: PGlite) =>
        pglite.query("SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = 'nest_workflows' ORDER BY tablename, indexname").then((result) => result.rows);
      expect(await indexes(byDrizzle)).toEqual(await indexes(migrated));
      const store = new PostgresWorkflowStore({ executor: fromDrizzle(db), migrate: false });
      await expect(store.onModuleInit()).resolves.toBeUndefined();
      expect(await store.create({ id: 'i-1', workflow: 'w', version: 1, input: null, deadline: null, now: 1 })).toMatchObject({ created: true });
    } finally {
      rmSync(folder, { recursive: true, force: true });
      await migrated.close();
      await byDrizzle.close();
    }
  });
});

describe('a first call inside the application transaction, on PGlite', () => {
  it("checks the schema through that transaction instead of waiting for it, and can't migrate in it", async () => {
    const pglite = new PGlite();
    const db = drizzle(pglite);
    try {
      const instance = { id: 'i-1', workflow: 'w', version: 1, input: null, deadline: null, now: 1 };
      const unmigrated = new PostgresWorkflowStore({ executor: fromDrizzle(db) });
      await expect(db.transaction((tx) => unmigrated.createInTransaction(tx, instance))).rejects.toThrow(
        "is at version 0, and this version of @nestjs/workflows needs version 1. The store applies its migrations when the application starts (onModuleInit), or at its first call outside a transaction: it can't apply them in yours.",
      );

      await new PostgresWorkflowStore({ executor: fromDrizzle(db) }).migrate();
      const fresh = new PostgresWorkflowStore({ executor: fromDrizzle(db) });
      await db.transaction((tx) => fresh.createInTransaction(tx, instance));
      expect(await fresh.get('i-1')).toMatchObject({ status: 'pending' });
    } finally {
      await pglite.close();
    }
  });
});
