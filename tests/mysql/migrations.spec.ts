/**
 * MySqlWorkflowStore's migrations, on the kit's MySQL StoreSchema (@nestjs/store-kit's own suite covers its machinery
 * with a schema of its own): a new database and one already up to date, a run that failed halfway resuming where it
 * stopped, processes starting together, `migrationSql()` and `migrationStatements()` against what `migrate()` applies
 * (statement by statement, and through drizzle-kit's MySQL migrator), a schema behind the code (and ahead of it), the
 * production default, the options, the server and connection checks, and a first call inside the application's
 * transaction.
 */
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { drizzle } from 'drizzle-orm/mysql2';
import { migrate as drizzleMigrate } from 'drizzle-orm/mysql2/migrator';
import mysql from 'mysql2/promise';
import pg from 'pg';
import { fromMysql2, MySqlWorkflowStore, WorkflowSchemaError, type SqlExecutor, type SqlTransaction } from '../../lib/mysql/index.js';
import { mysqlWorkflowStoreSchema } from '../../lib/mysql/migrations/index.js';
import { fromPg, PostgresWorkflowStore, WorkflowSchemaError as PostgresSchemaError } from '../../lib/postgres/index.js';
import { workflowStoreSchema } from '../../lib/postgres/migrations/index.js';
import { onMysql, recording, testDatabase } from './support.js';

const { database, reason } = await testDatabase('mysql_migrations');
const pools: mysql.Pool[] = [];

/** Ends the pools opened so far: each test's, after it (the server is shared, and each holds a connection). */
const endPools = () => Promise.all(pools.splice(0).map((pool) => pool.end()));
afterEach(endPools);

/** A pool of its own, as each process has: one connection, the server is shared. */
const pool = (options: { url?: string; session?: string } = {}) => {
  const opened = mysql.createPool({ uri: options.url ?? database!.url, connectionLimit: 1 });
  if (options.session) {
    opened.on('connection', (connection) => {
      connection.query(options.session!);
    });
  }
  pools.push(opened);
  return opened;
};

const store = (schema: string, options: { migrate?: boolean; executor?: SqlExecutor } = {}) =>
  new MySqlWorkflowStore({ executor: options.executor ?? fromMysql2(pool()), schema, migrate: options.migrate });

const rows = async (sql: string, params: unknown[] = []) => (await database!.admin.query<mysql.RowDataPacket[]>(sql, params))[0] as Array<Record<string, any>>;

const instance = { id: 'i-1', workflow: 'w', version: 1, input: null, deadline: null, now: 1 };

/** The store's tables of `schema` (`<schema>_...`), the kit's two included. */
const tables = async (schema: string) =>
  (
    await rows('SELECT TABLE_NAME AS name FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND LEFT(TABLE_NAME, ?) = ? ORDER BY TABLE_NAME', [
      schema.length + 1,
      `${schema}_`,
    ])
  ).map((row) => row.name);

/** The DDL a run sent, in order: not the lock, the reads, nor the progress records. */
const ddl = (statements: Array<{ text: string }>) => statements.map((statement) => statement.text).filter((text) => /^(CREATE|ALTER|DROP)\b/.test(text));

/** Everything about a schema's tables that migrations define, with the schema's name taken out, read through `db`. */
async function catalog(db: SqlTransaction, schema: string) {
  const read = (sql: string) => db.query(sql, [schema.length + 1, `${schema}_`]);
  const anonymize = (value: unknown) => JSON.parse(JSON.stringify(value).replaceAll(`${schema}_`, '<schema>_'));
  return anonymize({
    columns: await read(
      `SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name, CAST(ORDINAL_POSITION AS CHAR) AS position, COLUMN_TYPE AS type, IS_NULLABLE AS nullable,
  COLUMN_DEFAULT AS column_default, COLLATION_NAME AS collation_name, COLUMN_KEY AS column_key, EXTRA AS extra
FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND LEFT(TABLE_NAME, ?) = ? ORDER BY TABLE_NAME, ORDINAL_POSITION`,
    ),
    indexes: await read(
      `SELECT TABLE_NAME AS table_name, INDEX_NAME AS index_name, CAST(SEQ_IN_INDEX AS CHAR) AS seq, COLUMN_NAME AS column_name, CAST(NON_UNIQUE AS CHAR) AS non_unique
FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND LEFT(TABLE_NAME, ?) = ? ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX`,
    ),
    versions: await db.query(`SELECT CAST(version AS CHAR) AS version, name FROM \`${schema}_migrations\` WHERE applied_at IS NOT NULL ORDER BY version`),
  });
}

const STORE_TABLES = ['instances', 'journal', 'locks', 'migrations', 'rate_limits', 'schedules', 'signals', 'waits'];

describe('migrate()', () => {
  onMysql(reason);

  it("creates the store's tables and the version record in the connection's database, and applies nothing the second time", async () => {
    const first = store('m_fresh');
    expect(await first.migrate()).toEqual([1]);
    expect(await tables('m_fresh')).toEqual(STORE_TABLES.map((table) => `m_fresh_${table}`));
    expect(await rows('SELECT version, name FROM m_fresh_migrations')).toEqual([{ version: 1, name: 'initial' }]);
    expect(MySqlWorkflowStore.schemaVersion).toBe(1);

    // Already up to date: nothing to apply, and a store with migrate: false serves.
    expect(await first.migrate()).toEqual([]);
    expect(await store('m_fresh').migrate()).toEqual([]);
    expect(await rows('SELECT CAST(COUNT(*) AS CHAR) AS n FROM m_fresh_migrations')).toEqual([{ n: '1' }]);
    const serving = store('m_fresh', { migrate: false });
    await expect(serving.onModuleInit()).resolves.toBeUndefined();
    expect(await serving.create(instance)).toMatchObject({ created: true, instance: { id: 'i-1', status: 'pending' } });
  });

  it('applies the migrations once when processes start together, each on its own connection', async () => {
    const stores = Array.from({ length: 5 }, () => store('m_together'));
    const applied = await Promise.all(stores.map((s) => s.migrate()));
    expect(applied.filter((versions) => versions.length > 0)).toEqual([[1]]);
    await endPools();

    const starting = Array.from({ length: 5 }, () => store('m_starting'));
    await Promise.all(starting.map((s) => s.onModuleInit()));
    expect(await rows('SELECT version, started, applied FROM m_starting_migrations')).toEqual([{ version: 1, started: 6, applied: 6 }]);
    expect(await starting[3]!.create(instance)).toMatchObject({ created: true });
  });

  it('resumes a run that failed halfway at the statement it stopped at, never applying one twice', async () => {
    await rows('CREATE TABLE m_resume_signals (id int NOT NULL PRIMARY KEY)');
    const error = await store('m_resume').migrate().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(WorkflowSchemaError);
    expect(error).toMatchObject({ schema: 'm_resume', version: 0, requiredVersion: 1, cause: { errno: 1050 } });
    expect((error as Error).message).toBe(
      'MySqlWorkflowStore: migrating schema "m_resume" from version 0 to 1 stopped at migration 1 (initial), statement 4 of 6: ' +
        "Table 'm_resume_signals' already exists. The statements before it are applied, and migrating again resumes at it.",
    );
    // MySQL commits each DDL statement on its own: the three before it stay, and the version isn't recorded.
    expect(await tables('m_resume')).toEqual(['m_resume_instances', 'm_resume_journal', 'm_resume_locks', 'm_resume_migrations', 'm_resume_signals', 'm_resume_waits']);
    await expect(store('m_resume', { migrate: false }).onModuleInit()).rejects.toThrow('is at version 0');

    await rows('DROP TABLE m_resume_signals');
    const recorder = recording(fromMysql2(pool()));
    expect(await store('m_resume', { executor: recorder.executor }).migrate()).toEqual([1]);
    // Only the statements from the one that failed: the kit's tables (IF NOT EXISTS), then signals, schedules, rate_limits.
    expect(ddl(recorder.statements).map((text) => /^CREATE TABLE (IF NOT EXISTS )?`(\w+)`/.exec(text)![2])).toEqual([
      'm_resume_migrations',
      'm_resume_locks',
      'm_resume_signals',
      'm_resume_schedules',
      'm_resume_rate_limits',
    ]);
    await store('m_whole').migrate();
    const executor = fromMysql2(pool());
    expect(await catalog(executor, 'm_resume')).toEqual(await catalog(executor, 'm_whole'));
    await expect(store('m_resume', { migrate: false }).onModuleInit()).resolves.toBeUndefined();
  });

  it('runs the DDL migrationStatements() lists, and a database migrated with them statement by statement is the same', async () => {
    const recorder = recording(fromMysql2(pool()));
    await store('m_migrated', { executor: recorder.executor }).migrate();
    const statements = MySqlWorkflowStore.migrationStatements({ schema: 'm_migrated' });
    expect(ddl(recorder.statements)).toEqual(statements.filter((statement) => !statement.startsWith('INSERT')));
    expect(statements.at(-1)).toBe("INSERT INTO `m_migrated_migrations` (version, name, applied_at) VALUES (1, 'initial', UNIX_TIMESTAMP() * 1000)");
    expect(statements).toEqual(mysqlWorkflowStoreSchema.statements({ schema: 'm_migrated' }));

    // As a team applies them with a tool that runs one statement per call (TypeORM's queryRunner.query(), mysql2).
    const connection = pool();
    for (const statement of MySqlWorkflowStore.migrationStatements({ schema: 'm_statements' })) {
      await connection.query(statement);
    }
    const executor = fromMysql2(pool());
    expect(await catalog(executor, 'm_statements')).toEqual(await catalog(executor, 'm_migrated'));
    const serving = store('m_statements', { migrate: false });
    await expect(serving.onModuleInit()).resolves.toBeUndefined();
    expect(await serving.create(instance)).toMatchObject({ created: true });
  });

  it("has every index and unique key of PostgresWorkflowStore's schema, under the same names", async () => {
    await store('m_indexes').migrate();
    const postgres = workflowStoreSchema.statements({ schema: 'x' }).join('\n');
    const names = [...postgres.matchAll(/CREATE INDEX (\w+)|CONSTRAINT (\w+) UNIQUE/g)].map((match) => match[1] ?? match[2]).sort();
    expect(names).toHaveLength(13);
    const indexes = await rows(
      "SELECT DISTINCT INDEX_NAME AS name FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND LEFT(TABLE_NAME, 10) = 'm_indexes_' AND INDEX_NAME <> 'PRIMARY' ORDER BY INDEX_NAME",
    );
    expect(indexes.map((row) => row.name)).toEqual(names);
  });

  it('keeps ids, names and keys in binary utf8mb4 columns, and no foreign keys', async () => {
    await store('m_types').migrate();
    const keys = await rows(
      "SELECT CONCAT(TABLE_NAME, '.', COLUMN_NAME) AS name, COLLATION_NAME AS collation_name FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND LEFT(TABLE_NAME, 8) = 'm_types_' AND DATA_TYPE = 'varchar' AND TABLE_NAME <> 'm_types_migrations' ORDER BY 1",
    );
    expect(keys.length).toBeGreaterThan(20);
    expect(keys.filter((key) => key.collation_name !== 'utf8mb4_0900_bin')).toEqual([]);
    expect(await rows("SELECT CONSTRAINT_NAME FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = DATABASE() AND LEFT(TABLE_NAME, 8) = 'm_types_'")).toEqual([]);
  });

  it('migrates where the server requires a primary key on every table (sql_require_primary_key, some managed MySQL)', async () => {
    const strict = store('m_primary', { executor: fromMysql2(pool({ session: 'SET SESSION sql_require_primary_key = ON' })) });
    expect(await strict.migrate()).toEqual([1]);
    expect(await strict.create(instance)).toMatchObject({ created: true });
  });
});

describe("drizzle-kit's statement breakpoints, through Drizzle's MySQL migrator", () => {
  onMysql(reason);

  it("runs migrationSql({ statementBreakpoints: true }) one statement at a time: the schema is migrate()'s, and serves", async () => {
    const folder = mkdtempSync(join(tmpdir(), 'wft-drizzle-mysql-'));
    mkdirSync(join(folder, 'meta'));
    writeFileSync(join(folder, '0000_workflows.sql'), MySqlWorkflowStore.migrationSql({ schema: 'm_by_drizzle', statementBreakpoints: true }));
    writeFileSync(
      join(folder, 'meta', '_journal.json'),
      JSON.stringify({ version: '5', dialect: 'mysql', entries: [{ idx: 0, version: '5', when: 1790000000000, tag: '0000_workflows', breakpoints: true }] }),
    );
    try {
      await store('m_by_kit').migrate();
      await drizzleMigrate(drizzle(pool()), { migrationsFolder: folder, migrationsTable: 'drizzle_journal_workflows' });
      const executor = fromMysql2(pool());
      expect(await catalog(executor, 'm_by_drizzle')).toEqual(await catalog(executor, 'm_by_kit'));
      const serving = store('m_by_drizzle', { migrate: false });
      await expect(serving.onModuleInit()).resolves.toBeUndefined();
      expect(await serving.create(instance)).toMatchObject({ created: true });
    } finally {
      rmSync(folder, { recursive: true, force: true });
    }
  });
});

describe('a schema behind the code', () => {
  onMysql(reason);

  it('fails the startup (and every call) with a WorkflowSchemaError that says how to migrate, creating nothing', async () => {
    const behind = store('m_behind', { migrate: false });
    const error = await behind.onModuleInit().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(WorkflowSchemaError);
    // The package's one error: /postgres and /mysql export the same class.
    expect(WorkflowSchemaError).toBe(PostgresSchemaError);
    expect(error).toMatchObject({ name: 'WorkflowSchemaError', schema: 'm_behind', version: 0, requiredVersion: 1 });
    expect((error as Error).message).toBe(
      'MySqlWorkflowStore: schema "m_behind" is at version 0, and this version of @nestjs/workflows needs version 1. Apply its migrations: ' +
        'set `migrate: true` to apply them at startup, run `npx @nestjs/workflows migrate --url <database url> --schema m_behind`, ' +
        "or apply `MySqlWorkflowStore.migrationSql({ schema: 'm_behind', from: 0 })` with your migration tool.",
    );
    await expect(behind.get('any')).rejects.toThrow(WorkflowSchemaError);
    expect(await tables('m_behind')).toEqual([]);

    // Checked again at the next call: migrated meanwhile, it serves.
    await store('m_behind').migrate();
    expect(await behind.get('any')).toBeNull();
  });

  it('serves a schema ahead of the code, which a newer version of the package migrated during a rolling deploy', async () => {
    await store('m_ahead').migrate();
    await rows("INSERT INTO m_ahead_migrations (version, name, applied_at) VALUES (2, 'newer', 1)");

    const older = store('m_ahead', { migrate: false });
    await expect(older.onModuleInit()).resolves.toBeUndefined();
    expect(await older.create(instance)).toMatchObject({ created: true });
    expect(await store('m_ahead').migrate()).toEqual([]);
  });
});

describe('options', () => {
  onMysql(reason);

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
    expect(await tables('m_production')).toContain('m_production_instances');
  });

  it("take a schema name of lowercase letters, digits and underscores, at most 40 characters: its tables' prefix", () => {
    for (const schema of ['Bad', 'bad-name', '1st', '', 'x'.repeat(41), 'a`b', 'a$1']) {
      expect(() => new MySqlWorkflowStore({ executor: fromMysql2(pool()), schema })).toThrow(TypeError);
      expect(() => MySqlWorkflowStore.migrationSql({ schema })).toThrow(`MySqlWorkflowStore: invalid schema ${JSON.stringify(schema)}.`);
    }
    expect(() => new MySqlWorkflowStore({ executor: fromMysql2(pool()), schema: 'x'.repeat(40) })).not.toThrow();
  });

  it("refuse an executor that is none or of PostgreSQL, naming the store's /mysql entry, and a migrate that is no boolean", () => {
    expect(() => new MySqlWorkflowStore({ executor: {} as SqlExecutor })).toThrow('MySqlWorkflowStore: `executor` must be a SqlExecutor');
    const postgres = fromPg(new pg.Pool({ connectionString: 'postgres://nobody@127.0.0.1:1/none' }));
    // A PostgreSQL executor is a compile error first (the options take SqlExecutor<'mysql'>), then a TypeError.
    // @ts-expect-error
    expect(() => new MySqlWorkflowStore({ executor: postgres })).toThrow(
      "MySqlWorkflowStore runs on MySQL, and `executor` is a PostgreSQL executor: import the executor from '@nestjs/workflows/mysql' (fromMysql2, fromDrizzle, fromTypeOrm, fromPrisma or fromKysely).",
    );
    // And the other way around.
    // @ts-expect-error
    expect(() => new PostgresWorkflowStore({ executor: fromMysql2(pool()) })).toThrow(
      "PostgresWorkflowStore runs on PostgreSQL, and `executor` is a MySQL executor: import the executor from '@nestjs/workflows/postgres' (fromPg, fromDrizzle, fromTypeOrm, fromPrisma or fromKysely).",
    );
    expect(() => new MySqlWorkflowStore({ executor: fromMysql2(pool()), migrate: 'yes' as unknown as boolean })).toThrow(
      'MySqlWorkflowStore: `migrate` must be true or false, not "yes".',
    );
  });
});

describe('the server and the connection', () => {
  onMysql(reason);

  it("refuse a lax sql_mode, which would cut a key too long for its column, before creating anything", async () => {
    const lax = store('m_lax', { executor: fromMysql2(pool({ session: "SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'" })) });
    await expect(lax.onModuleInit()).rejects.toThrow(
      'MySqlWorkflowStore needs a strict sql_mode (STRICT_TRANS_TABLES, MySQL\'s default), and this connection\'s is "NO_ENGINE_SUBSTITUTION"',
    );
    expect(await tables('m_lax')).toEqual([]);
  });

  it('refuse a connection without a database: the tables live in it', async () => {
    const url = new URL(database!.url);
    url.pathname = '/';
    const nowhere = store('m_nowhere', { executor: fromMysql2(pool({ url: url.toString() })) });
    await expect(nowhere.onModuleInit()).rejects.toThrow("MySqlWorkflowStore keeps its tables in the connection's database, and this connection has none");
  });

  it('refuse MariaDB by its version, before any SQL of the store', async () => {
    const real = fromMysql2(pool());
    const mariadb: SqlExecutor = {
      ...real,
      dialect: 'mysql',
      query: (text, params) => (text === 'SELECT VERSION() AS version' ? Promise.resolve([{ version: '11.4.2-MariaDB-ubu2404' }] as never) : real.query(text, params)),
      execute: real.execute.bind(real),
      transaction: real.transaction.bind(real),
      wrapTransaction: real.wrapTransaction.bind(real),
    };
    await expect(store('m_mariadb', { executor: mariadb }).onModuleInit()).rejects.toThrow(
      "MySqlWorkflowStore runs on MySQL, and this server is MariaDB (11.4.2-MariaDB-ubu2404): MariaDB isn't supported yet.",
    );
    expect(await tables('m_mariadb')).toEqual([]);
  });
});

describe('migrationSql()', () => {
  it('prints the default schema from a new database, a range of versions, and never a downgrade', () => {
    const script = MySqlWorkflowStore.migrationSql();
    expect(script).toMatch(/^-- @nestjs\/workflows: MySqlWorkflowStore's schema "nest_workflows" \(tables nest_workflows_\*\), from version 0 to 1\.\n/);
    expect(script).toContain("-- The statements don't run in one transaction: MySQL commits each DDL statement on its own. Apply them in order, each once.");
    expect(script).toContain('CREATE TABLE `nest_workflows_instances` (');
    expect(script).toContain("INSERT INTO `nest_workflows_migrations` (version, name, applied_at) VALUES (1, 'initial', UNIX_TIMESTAMP() * 1000);");
    expect(MySqlWorkflowStore.migrationSql({ from: 1 })).not.toContain('CREATE');
    expect(MySqlWorkflowStore.migrationSql({ statementBreakpoints: true })).toContain('\n--> statement-breakpoint\n');

    expect(() => MySqlWorkflowStore.migrationSql({ from: 1, to: 0 })).toThrow("downgrades aren't supported");
    expect(() => MySqlWorkflowStore.migrationSql({ to: 2 })).toThrow('no migrations lead from version 0 to 2');
    expect(() => MySqlWorkflowStore.migrationStatements({ from: -1 })).toThrow(RangeError);
  });
});

describe('a first call inside the application transaction, on a pool of one connection', () => {
  onMysql(reason);

  it("checks the schema through that transaction instead of waiting for the pool, and can't migrate in it", async () => {
    const one = pool();
    const unmigrated = new MySqlWorkflowStore({ executor: fromMysql2(one), schema: 'm_in_tx' });
    const inTransaction = async <T>(work: (connection: mysql.PoolConnection) => Promise<T>) => {
      const connection = await one.getConnection();
      try {
        await connection.beginTransaction();
        const result = await work(connection);
        await connection.commit();
        return result;
      } catch (error) {
        await connection.rollback();
        throw error;
      } finally {
        connection.release();
      }
    };
    await expect(inTransaction((connection) => unmigrated.createInTransaction(connection, instance))).rejects.toThrow(
      "is at version 0, and this version of @nestjs/workflows needs version 1. The store applies its migrations when the application starts (onModuleInit), or at its first call outside a transaction: it can't apply them in yours.",
    );

    await store('m_in_tx').migrate();
    const fresh = new MySqlWorkflowStore({ executor: fromMysql2(one), schema: 'm_in_tx' });
    await inTransaction((connection) => fresh.createInTransaction(connection, instance));
    expect(await fresh.get('i-1')).toMatchObject({ status: 'pending' });
  });
});
