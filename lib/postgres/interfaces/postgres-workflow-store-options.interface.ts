import type { SqlExecutor } from '@nestjs/store-kit/postgres';

/** What `new PostgresWorkflowStore(options, storage)` takes. */
export interface PostgresWorkflowStoreOptions {
  /**
   * How the store reaches the database: `fromPg(pool)`, `fromSequelize(sequelize)`, `fromDrizzle(db)`,
   * `fromTypeOrm(dataSource)`, `fromPrisma(prisma)` or `fromKysely(db)`. The store's own transactions run on it, and
   * `start()` and `signal()` with `{ transaction }` take that client's transaction object: Drizzle's `tx`, a TypeORM
   * `EntityManager`, a Prisma transaction client, a Kysely `Transaction`, a Sequelize `transaction`, a node-postgres
   * client after `BEGIN`.
   */
  executor: SqlExecutor<'postgres'>;
  /**
   * The schema that holds the store's tables, created by its first migration: keep it for the store alone. Letters,
   * digits and underscores, not starting with a digit, at most 63 characters. Default: `'nest_workflows'`.
   */
  schema?: string;
  /**
   * Apply the store's pending migrations at startup, in one transaction under an advisory lock, so processes that
   * start together migrate once. With `false`, startup fails with a `WorkflowSchemaError` while the schema is behind
   * this version of the package: apply them with `npx nest-workflows migrate`, or with your own migration tool
   * (`PostgresWorkflowStore.migrationSql()`). Default: `true`, except when `NODE_ENV` is `production`.
   */
  migrate?: boolean;
}
