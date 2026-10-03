import type { SqlExecutor } from '@nestjs/store-kit/mysql';

/** What `new MySqlWorkflowStore(options, storage)` takes. */
export interface MySqlWorkflowStoreOptions {
  /**
   * How the store reaches the database: `fromMysql2(pool)`, `fromSequelize(sequelize)`, `fromDrizzle(db)`,
   * `fromTypeOrm(dataSource)`, `fromPrisma(prisma)` or `fromKysely(db)` from `@nestjs/workflows/mysql`. The store keeps
   * its tables in the database the pool or ORM connects to; its own transactions run on it, and `start()` and
   * `signal()` with `{ transaction }` take that client's transaction object: Drizzle's `tx`, a TypeORM
   * `EntityManager`, a Prisma transaction client, a Kysely `Transaction`, a Sequelize `transaction`, a mysql2
   * connection after `beginTransaction()`. `fromSequelize()` needs mysql2's `FOUND_ROWS` client flag: Sequelize's
   * MySQL connection manager sets `flags: "-FOUND_ROWS"` unless the instance passes `dialectOptions: { flags: '' }`.
   */
  executor: SqlExecutor<'mysql'>;
  /**
   * The name the store's tables start with, in the connection's database: `<schema>_<table>` (`nest_workflows`:
   * `nest_workflows_instances`, `nest_workflows_signals`...). Keep it for the store alone. Lowercase letters, digits
   * and underscores, not starting with a digit, at most 40 characters. Default: `'nest_workflows'`.
   */
  schema?: string;
  /**
   * Apply the store's pending migrations at startup, one statement at a time under a lock (`GET_LOCK()`), so processes
   * that start together migrate once, and a run that stopped halfway resumes where it stopped. With `false`, startup
   * fails with a `WorkflowSchemaError` while the schema is behind this version of the package: apply them with
   * `npx nest-workflows migrate`, or with your own migration tool (`MySqlWorkflowStore.migrationSql()` or
   * `migrationStatements()`). Default: `true`, except when `NODE_ENV` is `production`.
   */
  migrate?: boolean;
}
