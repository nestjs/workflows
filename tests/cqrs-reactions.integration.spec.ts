/**
 * What still reacts to a mapped event in memory once `WorkflowsCqrsModule` wraps the publisher,
 * on every store: a saga and an events handler run once, after the workflow started, and inside a
 * transaction before it commits (so they see events that then roll back). CQRS's `AsyncContext`
 * reaches request-scoped handlers through the wrapped publisher, and is never taken for a
 * transaction, whether it comes alone or next to a dispatcher context.
 */
import { Inject, Scope } from '@nestjs/common';
import { REQUEST } from '@nestjs/core';
import { AsyncContext, CommandBus, CqrsModule, EventBus, EventsHandler, type IEventHandler } from '@nestjs/cqrs';
import { sql } from 'drizzle-orm';
import { WorkflowsCqrsModule } from '../lib/cqrs/index.js';
import { ManualWorkflowClock, type WorkflowStore } from '../lib/index.js';
import { cqrsProviders, fulfilmentId, Ledger, OrderFulfilmentWorkflow, OrderPlacedEvent, PlaceOrderCommand } from './cqrs-app.js';
import type { Database } from './fixtures/database/drizzle.js';
import { boot, connect, orders, storeKind, tempDb, waitFor, type Node, type TestDb } from './support.js';

/**
 * The request payload of a CQRS request scope: an `AsyncContext`, or a class that extends it, as the
 * cqrs docs require. `EventBus` registers it as `REQUEST` itself from @nestjs/cqrs 12.1.1 on; the
 * tests register it too, for earlier versions.
 */
class TenantContext extends AsyncContext {
  constructor(readonly tenant: string) {
    super();
  }
}

/** What the request-scoped handler saw: the tenant its context carried, and which instance of it ran. */
class Tenants {
  readonly seen: Array<{ orderId: string; tenant: string | undefined; handler: number }> = [];
  handlers = 0;
}

@EventsHandler(OrderPlacedEvent, { scope: Scope.REQUEST })
class TenantOrdersHandler implements IEventHandler<OrderPlacedEvent> {
  private readonly handler: number;

  constructor(
    // For an event published without a TenantContext, cqrs makes a new AsyncContext: no tenant
    // (and before @nestjs/cqrs 12.1.1, no request at all).
    @Inject(REQUEST) private readonly request: Partial<TenantContext> | undefined,
    @Inject(Tenants) private readonly tenants: Tenants,
  ) {
    this.handler = ++tenants.handlers;
  }

  handle(event: OrderPlacedEvent) {
    this.tenants.seen.push({ orderId: event.orderId, tenant: this.request?.tenant, handler: this.handler });
  }
}

describe('sagas, events handlers and async contexts next to the workflows', () => {
  let db: TestDb;
  let clock: ManualWorkflowClock;
  let ledger: Ledger;
  let tenants: Tenants;
  const nodes: Node[] = [];

  beforeEach(async () => {
    db = await tempDb();
    clock = new ManualWorkflowClock();
    ledger = new Ledger();
    ledger.lookUpInstances = false;
    tenants = new Tenants();
    if (storeKind !== 'memory') {
      const connection = connect(db);
      await (connection.db as Database).execute(sql`DELETE FROM orders`);
      await connection.close();
    }
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    for (const node of nodes.splice(0)) {
      await node.close();
    }
    db.cleanup();
  });

  const start = async () => {
    const node = await boot({
      db,
      clock,
      imports: [CqrsModule.forRoot(), WorkflowsCqrsModule],
      workflows: [OrderFulfilmentWorkflow],
      providers: [...cqrsProviders, TenantOrdersHandler, { provide: Ledger, useValue: ledger }, { provide: Tenants, useValue: tenants }],
    });
    nodes.push(node);
    return { ...node, commandBus: node.moduleRef.get(CommandBus), eventBus: node.moduleRef.get(EventBus) };
  };

  /** Records how far the in-memory reactions had got when the store finished each start. */
  const watchStarts = (store: WorkflowStore) => {
    const starts: Array<{ transaction: boolean; saga: number; handled: number }> = [];
    for (const method of ['create', 'createInTransaction'] as const) {
      const original = store[method]!.bind(store) as (...args: unknown[]) => ReturnType<WorkflowStore['create']>;
      vi.spyOn(store, method).mockImplementation(async (...args: unknown[]) => {
        const result = await original(...args);
        starts.push({ transaction: method === 'createInTransaction', saga: ledger.saga.length, handled: ledger.handled.length });
        return result;
      });
    }
    return starts;
  };

  /** Long enough for a stray second reaction to show up. */
  const settle = () => new Promise((resolve) => setTimeout(resolve, 30));

  it('runs the saga and the events handler once per mapped event, after the workflow started', async () => {
    const node = await start();
    const starts = watchStarts(node.store);

    await node.commandBus.execute(new PlaceOrderCommand('o-1', 2499));
    await node.eventBus.publishAll([new OrderPlacedEvent('o-2', 1599)]);
    await waitFor(() => ledger.handled.length === 2);
    await settle();

    expect(starts).toEqual([
      { transaction: storeKind !== 'memory', saga: 0, handled: 0 },
      { transaction: false, saga: 1, handled: 1 },
    ]);
    expect(ledger.saga).toEqual(['o-1', 'o-2']);
    expect(ledger.handled.map(({ orderId }) => orderId)).toEqual(['o-1', 'o-2']);
  });

  it.runIf(storeKind !== 'memory')('has run the saga and the events handler by the time the transaction rolls back', async () => {
    const node = await start();

    await expect(node.commandBus.execute(new PlaceOrderCommand('o-1', 2499, 'the warehouse is closed'))).rejects.toThrow('the warehouse is closed');
    // They reacted inside the transaction, to an event that never happened: the documented trade-off.
    expect(ledger.saga).toEqual(['o-1']);
    expect(ledger.handled.map(({ orderId }) => orderId)).toEqual(['o-1']);
    expect(await node.client.list()).toEqual([]);
  });

  it('hands an AsyncContext on to request-scoped handlers, and never takes it for a transaction', async () => {
    const node = await start();
    const starts = watchStarts(node.store);
    const cats = new TenantContext('cats');
    node.moduleRef.registerRequestByContextId(cats, cats.id);
    const dogs = new TenantContext('dogs');
    node.moduleRef.registerRequestByContextId(dogs, dogs.id);

    await node.eventBus.publish(new OrderPlacedEvent('o-1', 2499), cats);
    await node.eventBus.publishAll([new OrderPlacedEvent('o-2', 1599)], cats);
    await node.eventBus.publish(new OrderPlacedEvent('o-3', 999), { source: 'import' }, dogs);
    await waitFor(() => tenants.seen.length === 3);

    expect(starts.map(({ transaction }) => transaction)).toEqual([false, false, false]);
    expect((await node.client.list()).map(({ id }) => id)).toEqual([fulfilmentId('o-1'), fulfilmentId('o-2'), fulfilmentId('o-3')]);
    // Handlers are resolved asynchronously, so they may run in any order. One handler per context:
    // the events published with `cats` share one.
    const byOrder = Object.fromEntries(tenants.seen.map((seen) => [seen.orderId, seen]));
    expect(byOrder).toMatchObject({ 'o-1': { tenant: 'cats' }, 'o-2': { tenant: 'cats' }, 'o-3': { tenant: 'dogs' } });
    expect(byOrder['o-2']!.handler).toBe(byOrder['o-1']!.handler);
    expect(byOrder['o-3']!.handler).not.toBe(byOrder['o-1']!.handler);
  });

  it.runIf(storeKind !== 'memory')('reads the transaction from the dispatcher context next to an AsyncContext', async () => {
    const node = await start();
    const starts = watchStarts(node.store);
    const cats = new TenantContext('cats');
    node.moduleRef.registerRequestByContextId(cats, cats.id);
    const connection = connect(db);
    const database = connection.db as Database;

    const place = (orderId: string, fail?: string) =>
      database.transaction(async (tx) => {
        await tx.insert(orders).values({ id: orderId, userId: 'u_42', items: [], total: 2499, status: 'placed' });
        await node.eventBus.publish(new OrderPlacedEvent(orderId, 2499), { transaction: tx }, cats);
        if (fail) {
          throw new Error(fail);
        }
      });
    try {
      await place('o-1');
      await expect(place('o-2', 'the warehouse is closed')).rejects.toThrow('the warehouse is closed');
      await waitFor(() => tenants.seen.length === 2);

      expect(starts.map(({ transaction }) => transaction)).toEqual([true, true]);
      expect((await node.client.list()).map(({ id }) => id)).toEqual([fulfilmentId('o-1')]);
      expect(tenants.seen.map(({ orderId, tenant }) => `${orderId}:${tenant}`).sort()).toEqual(['o-1:cats', 'o-2:cats']);
    } finally {
      await connection.close();
    }
  });
});
