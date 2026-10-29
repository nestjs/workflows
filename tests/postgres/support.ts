/**
 * PostgresWorkflowStore's tests: the database clients an application may hand it (node-postgres, Drizzle on
 * node-postgres and on PGlite, TypeORM, Prisma, Kysely), each with the ORM's own way of running a transaction, and a
 * database per test file on PostgreSQL (`SQL_TEST_PG_URL`, else a throwaway cluster, else those tests are skipped
 * with the reason), through tests/support/postgres.ts, which names and sweeps them.
 */
import { PGlite } from '@electric-sql/pglite';
import { PrismaPg } from '@prisma/adapter-pg';
import { drizzle as drizzlePg } from 'drizzle-orm/node-postgres';
import { pgTable, text } from 'drizzle-orm/pg-core';
import { drizzle as drizzlePglite } from 'drizzle-orm/pglite';
import { Kysely, PostgresDialect } from 'kysely';
import pg from 'pg';
import { Column, DataSource, Entity, PrimaryColumn } from 'typeorm';
import { fromDrizzle, fromKysely, fromPg, fromPrisma, fromTypeOrm, PostgresWorkflowStore, type SqlExecutor } from '../../lib/postgres/index.js';
import { workflowStoreContract } from '../../lib/testing/index.js';
import { PrismaClient } from '../fixtures/prisma/generated/client.js';
import { endPool, startPostgres } from '../support/postgres.js';

/** The application's table, written in the same transactions as the workflows it starts and signals. */
export const ORDERS_DDL = 'CREATE TABLE IF NOT EXISTS orders (id text PRIMARY KEY, status text NOT NULL)';

export type Isolation = 'read committed' | 'repeatable read';

/** A database client as an application holds one. */
export interface Client {
  name: string;
  executor: SqlExecutor<'postgres'>;
  /** A transaction as the application runs one with this client: `work` gets the ORM's own transaction object. */
  transaction<T>(work: (tx: unknown) => Promise<T>, isolation?: Isolation): Promise<T>;
  /** What an application might pass by mistake instead of its transaction: the pool, the database, the client. */
  root: unknown;
  /** The application's write, through its transaction object. */
  insertOrder(tx: unknown, id: string): Promise<void>;
  close(): Promise<void>;
}

export interface ClientFactory {
  name: string;
  open(url: string): Promise<Client>;
}

const drizzleOrders = pgTable('orders', { id: text('id').primaryKey(), status: text('status').notNull() });

@Entity('orders')
class OrderEntity {
  @PrimaryColumn('text')
  id!: string;

  @Column('text')
  status!: string;
}

interface KyselyDatabase {
  orders: { id: string; status: string };
}

export const pgClient: ClientFactory = {
  name: 'fromPg (node-postgres Pool)',
  async open(url) {
    const pool = new pg.Pool({ connectionString: url, max: 10 });
    return {
      name: this.name,
      executor: fromPg(pool),
      root: pool,
      async transaction(work, isolation) {
        const client = await pool.connect();
        try {
          await client.query(isolation ? `BEGIN ISOLATION LEVEL ${isolation.toUpperCase()}` : 'BEGIN');
          const result = await work(client);
          await client.query('COMMIT');
          return result;
        } catch (error) {
          await client.query('ROLLBACK');
          throw error;
        } finally {
          client.release();
        }
      },
      insertOrder: async (tx, id) => {
        await (tx as pg.PoolClient).query("INSERT INTO orders (id, status) VALUES ($1, 'placed')", [id]);
      },
      close: () => endPool(pool),
    };
  },
};

export const drizzleClient: ClientFactory = {
  name: 'fromDrizzle (node-postgres)',
  async open(url) {
    const pool = new pg.Pool({ connectionString: url, max: 10 });
    const db = drizzlePg(pool);
    return {
      name: this.name,
      executor: fromDrizzle(db),
      root: db,
      transaction: (work, isolation) => db.transaction(work, isolation ? { isolationLevel: isolation } : undefined),
      insertOrder: async (tx, id) => {
        await (tx as typeof db).insert(drizzleOrders).values({ id, status: 'placed' });
      },
      close: () => endPool(pool),
    };
  },
};

export const typeOrmClient: ClientFactory = {
  name: 'fromTypeOrm',
  async open(url) {
    const dataSource = await new DataSource({ type: 'postgres', url, entities: [OrderEntity], poolSize: 10 }).initialize();
    return {
      name: this.name,
      executor: fromTypeOrm(dataSource),
      root: dataSource.manager,
      transaction: (work, isolation) => (isolation ? dataSource.transaction(isolation.toUpperCase() as 'REPEATABLE READ', work) : dataSource.transaction(work)),
      insertOrder: async (tx, id) => {
        await (tx as DataSource['manager']).insert(OrderEntity, { id, status: 'placed' });
      },
      close: () => dataSource.destroy(),
    };
  },
};

export const prismaClient: ClientFactory = {
  name: 'fromPrisma (@prisma/adapter-pg)',
  async open(url) {
    const prisma = new PrismaClient({ adapter: new PrismaPg({ connectionString: url, max: 10 }) });
    return {
      name: this.name,
      executor: fromPrisma(prisma),
      root: prisma,
      // The application's own limits. Prisma's default maxWait (2 s) is shorter than the queue for a connection gets
      // on a busy machine when 60 signals, which the store serializes, share a pool of 10 with 60 suspensions.
      transaction: (work, isolation) =>
        prisma.$transaction((tx) => work(tx), {
          maxWait: 10_000,
          timeout: 30_000,
          ...(isolation ? { isolationLevel: isolation === 'repeatable read' ? 'RepeatableRead' : 'ReadCommitted' } : {}),
        }),
      insertOrder: async (tx, id) => {
        await (tx as PrismaClient).order.create({ data: { id, status: 'placed' } });
      },
      close: () => prisma.$disconnect(),
    };
  },
};

export const kyselyClient: ClientFactory = {
  name: 'fromKysely',
  async open(url) {
    const pool = new pg.Pool({ connectionString: url, max: 10 });
    const db = new Kysely<KyselyDatabase>({ dialect: new PostgresDialect({ pool }) });
    return {
      name: this.name,
      executor: fromKysely(db),
      root: db,
      transaction: (work, isolation) => (isolation ? db.transaction().setIsolationLevel(isolation).execute(work) : db.transaction().execute(work)),
      insertOrder: async (tx, id) => {
        await (tx as typeof db).insertInto('orders').values({ id, status: 'placed' }).execute();
      },
      // db.destroy() ends the pool, which listens for no error: wait for its connections to close.
      close: () => endPool(pool, () => db.destroy()),
    };
  },
};

/** Every client on PostgreSQL. */
export const clients = [pgClient, drizzleClient, typeOrmClient, prismaClient, kyselyClient];

/** Drizzle on PGlite: PostgreSQL in-process, one connection, so every transaction waits for the one before it. */
export async function openPglite(): Promise<Client & { pglite: PGlite }> {
  const pglite = new PGlite();
  const db = drizzlePglite(pglite);
  await db.execute(ORDERS_DDL);
  return {
    name: 'fromDrizzle (PGlite)',
    pglite,
    executor: fromDrizzle(db),
    root: db,
    transaction: (work, isolation) => db.transaction(work, isolation ? { isolationLevel: isolation } : undefined),
    insertOrder: async (tx, id) => {
      await (tx as typeof db).insert(drizzleOrders).values({ id, status: 'placed' });
    },
    close: () => pglite.close(),
  };
}

export interface TestDatabase {
  url: string;
  /** A client for looking at the database from outside the store. */
  admin: pg.Pool;
}

/**
 * A database of this test file on PostgreSQL (with `orders`), dropped after the file; `null`, with the reason, where
 * there's no PostgreSQL. Tests that need it skip with the reason.
 */
export async function testDatabase(name: string): Promise<{ database: TestDatabase; reason?: undefined } | { database: null; reason: string }> {
  const { postgres, reason } = await startPostgres();
  if (!postgres) {
    return { database: null, reason: `no PostgreSQL: ${reason}` };
  }

  const url = await postgres.createDatabase(name);
  const admin = new pg.Pool({ connectionString: url, max: 2 });
  await admin.query(ORDERS_DDL);
  afterAll(async () => {
    await endPool(admin);
    await postgres.stop();
  });
  return { database: { url, admin } };
}

/** Empties the store's tables in `schema`, as the contract wants a store on empty tables. */
export async function truncate(executor: SqlExecutor, schema: string): Promise<void> {
  const tables = ['instances', 'journal', 'waits', 'signals', 'schedules', 'rate_limits'].map((table) => `"${schema}".${table}`);
  await executor.query(`TRUNCATE ${tables.join(', ')} RESTART IDENTITY`);
}

/**
 * The `WorkflowStore` contract on PostgresWorkflowStore through `client`, in a schema of its own, with the concurrency
 * cases and the ORM's own transactions.
 */
export function describeContract(label: string, client: () => Promise<Client | null>, schema: string, skipReason?: string): void {
  describe(label, () => {
    let opened: Client | null = null;
    beforeAll(async () => {
      opened = await client();
      if (opened) {
        await new PostgresWorkflowStore({ executor: opened.executor, schema }).migrate();
      }
    });
    afterAll(() => opened?.close());
    if (skipReason) {
      beforeEach((context) => context.skip(skipReason));
    }

    const cases = workflowStoreContract(
      async () => {
        await truncate(opened!.executor, schema);
        return new PostgresWorkflowStore({ executor: opened!.executor, schema, migrate: false });
      },
      { concurrent: true, transaction: (work) => opened!.transaction(work) },
    );
    for (const c of cases) {
      it(c.name, c.run);
    }
  });
}
