/**
 * `@nestjs/workflows/cqrs` on the in-memory store (vitest.config.ts runs it once): the startup
 * checks, the decorators' arguments and types, how the publisher wraps the one before it, which
 * versions and declarations an event reaches, and what happens to an aggregate's `commit()`.
 */
import { Injectable, Logger, type OnModuleInit, type Provider, type Type } from '@nestjs/common';
import {
  AggregateRoot,
  AsyncContext,
  CqrsModule,
  EventBus,
  EventPublisher,
  UnhandledExceptionBus,
  type IEvent,
  type IEventPublisher,
  type UnhandledExceptionInfo,
} from '@nestjs/cqrs';
import { Test, type TestingModule } from '@nestjs/testing';
import { SignalOn, StartOn, WorkflowsCqrsModule, type WorkflowDispatcherContext } from '../lib/cqrs/index.js';
import {
  InMemoryWorkflowStore,
  Workflow,
  WorkflowClient,
  WorkflowIdConflictError,
  WorkflowSignal,
  WorkflowsModule,
  WorkflowStorage,
  WorkflowWorker,
  type WorkflowContext,
} from '../lib/index.js';
import {
  cqrsProviders,
  fulfilmentId,
  Ledger,
  OrderFulfilmentWorkflow,
  OrderPlacedEvent,
  OrderReadyEvent,
  PaymentCapturedEvent,
  paymentCaptured,
} from './cqrs-app.js';
import { waitFor } from './support.js';

interface App {
  moduleRef: TestingModule;
  store: InMemoryWorkflowStore;
  client: WorkflowClient;
  worker: WorkflowWorker;
  eventBus: EventBus;
  close(): Promise<void>;
}

const apps: TestingModule[] = [];

afterEach(async () => {
  for (const app of apps.splice(0)) {
    await app.close();
  }
});

async function compile(options: { workflows: Type<unknown>[]; providers?: Provider[]; cqrs?: boolean; eventPublisher?: IEventPublisher }) {
  const moduleRef = await Test.createTestingModule({
    imports: [
      CqrsModule.forRoot(options.eventPublisher ? { eventPublisher: options.eventPublisher } : {}),
      WorkflowsModule.forRoot({ worker: false }),
      ...(options.cqrs === false ? [] : [WorkflowsCqrsModule]),
    ],
    providers: [...options.workflows, ...(options.providers ?? [])],
  }).compile();
  moduleRef.useLogger(false);
  apps.push(moduleRef);

  const store = new InMemoryWorkflowStore();
  moduleRef.get(WorkflowStorage).registerSource(store);
  return { moduleRef, store };
}

async function boot(options: Parameters<typeof compile>[0]): Promise<App> {
  const { moduleRef, store } = await compile(options);
  await moduleRef.init();
  return {
    moduleRef,
    store,
    client: moduleRef.get(WorkflowClient),
    worker: moduleRef.get(WorkflowWorker),
    eventBus: moduleRef.get(EventBus),
    close: () => moduleRef.close(),
  };
}

/** Its init() rejects, and so would its close(), which awaits the initialization first. */
async function expectStartupError(moduleRef: TestingModule, message: string) {
  apps.splice(apps.indexOf(moduleRef), 1);
  await expect(moduleRef.init()).rejects.toThrow(message);
}

const signalsSent = (store: InMemoryWorkflowStore, name: string, key: string | null) =>
  store.signals({ name, key, afterId: 0, upToId: Number.MAX_SAFE_INTEGER });

describe('startup checks', () => {
  it('fails when a workflow maps events but WorkflowsCqrsModule is not imported', async () => {
    const { moduleRef } = await compile({ workflows: [OrderFulfilmentWorkflow], cqrs: false });

    await expectStartupError(
      moduleRef,
      'Workflow OrderFulfilmentWorkflow is started or signalled by CQRS events (@StartOn(), @SignalOn()), but ' +
        'WorkflowsCqrsModule is not imported',
    );
  });

  it('fails for @StartOn() on a provider without @Workflow()', async () => {
    @Injectable()
    @StartOn(OrderPlacedEvent, { id: (event) => event.orderId })
    class NotAWorkflow {
      async run() {}
    }

    const { moduleRef } = await compile({ workflows: [NotAWorkflow] });
    await expectStartupError(moduleRef, 'NotAWorkflow has @StartOn() or @SignalOn() but no @Workflow().');
  });

  it('fails for the same event twice on one workflow', async () => {
    @Workflow('twice-started')
    @StartOn(OrderPlacedEvent, { id: (event) => `a-${event.orderId}` })
    @StartOn(OrderPlacedEvent, { id: (event) => `b-${event.orderId}` })
    class TwiceStarted {
      async run() {}
    }

    @Workflow('twice-signalled')
    @SignalOn(PaymentCapturedEvent, { signal: paymentCaptured, key: (event) => event.orderId, payload: (event) => event })
    @SignalOn(PaymentCapturedEvent, { signal: paymentCaptured, key: (event) => event.chargeId, payload: (event) => event })
    class TwiceSignalled {
      async run() {}
    }

    await expectStartupError(
      (await compile({ workflows: [TwiceStarted] })).moduleRef,
      'TwiceStarted has @StartOn(OrderPlacedEvent) twice. An event starts one instance of a workflow: keep one.',
    );
    await expectStartupError(
      (await compile({ workflows: [TwiceSignalled] })).moduleRef,
      'TwiceSignalled has @SignalOn(PaymentCapturedEvent) for the signal "payment.captured" twice: keep one.',
    );
  });

  it('fails when something replaces EventBus.publisher after the module installed its own', async () => {
    @Injectable()
    class KafkaSetup implements OnModuleInit {
      constructor(private readonly eventBus: EventBus) {}

      onModuleInit() {
        this.eventBus.publisher = new (class KafkaPublisher {
          publish() {}
        })();
      }
    }

    const { moduleRef } = await compile({ workflows: [OrderFulfilmentWorkflow], providers: [KafkaSetup, ...shared()] });
    await expectStartupError(
      moduleRef,
      'WorkflowsCqrsModule: EventBus.publisher was replaced (by KafkaPublisher) after WorkflowsCqrsModule installed its publisher',
    );
  });

  it('warns when no registered workflow maps an event: the process would start and signal nothing', async () => {
    const warn = vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
    try {
      await boot({ workflows: [] });
      expect(warn).toHaveBeenCalledWith(
        'WorkflowsCqrsModule is imported, but no registered workflow maps an event with @StartOn() or @SignalOn(), so ' +
          'events start and signal no workflow in this process. Register the workflow classes in every process that ' +
          'publishes their events, with worker: false in WorkflowsModule.forRoot() if the process runs none.',
      );

      warn.mockClear();
      await boot({ workflows: [OrderFulfilmentWorkflow], providers: shared() });
      expect(warn).not.toHaveBeenCalled();
    } finally {
      warn.mockRestore();
    }
  });

  it('checks the decorators’ arguments at once', () => {
    const noId = { input: (event: OrderPlacedEvent) => event } as never;
    expect(() => StartOn(OrderPlacedEvent, noId)).toThrow(
      new TypeError('@StartOn(OrderPlacedEvent): `id` must be a function of the event, such as (event) => event.orderId.'),
    );
    expect(() => StartOn('OrderPlacedEvent' as never, { id: () => 'x' })).toThrow(
      new TypeError('@StartOn() takes the event class first, got string.'),
    );
    expect(() => SignalOn(PaymentCapturedEvent, { signal: '' as string, payload: (event) => event })).toThrow(TypeError);
    expect(() => SignalOn(PaymentCapturedEvent, { signal: paymentCaptured, id: 'chargeId' as never, payload: (event) => event })).toThrow(
      new TypeError('@SignalOn(PaymentCapturedEvent): `id` must be a function of the event, such as (event) => event.orderId, or left out.'),
    );
    expect(() => SignalOn(PaymentCapturedEvent, { signal: paymentCaptured, key: 'orderId' as never, payload: (event) => event })).toThrow(
      new TypeError(
        '@SignalOn(PaymentCapturedEvent): `key` must be a function of the event, such as (event) => event.orderId, or left out.',
      ),
    );
    expect(() => StartOn(OrderPlacedEvent, { id: (event) => event.orderId, priority: 1.5 })).toThrow(
      new TypeError('@StartOn(OrderPlacedEvent): invalid priority 1.5. Use an integer from 1 (first) to 2097151, or a function of the event.'),
    );
    expect(() => StartOn(OrderPlacedEvent, { id: (event) => event.orderId, concurrencyKey: '' })).toThrow(
      new TypeError('@StartOn(OrderPlacedEvent): `concurrencyKey` must be a non-empty string, or a function of the event, such as (event) => event.warehouseId.'),
    );
    expect(() => StartOn(OrderPlacedEvent, { id: (event) => event.orderId, rateLimitKey: 42 as never })).toThrow(
      new TypeError('@StartOn(OrderPlacedEvent): `rateLimitKey` must be a non-empty string, or a function of the event, such as (event) => event.warehouseId.'),
    );
  });

  it("fails for a @StartOn() key its workflow has no limit per key to count under", async () => {
    @Workflow('unkeyed')
    @StartOn(OrderPlacedEvent, { id: (event) => event.orderId, concurrencyKey: 'warehouse-1' })
    class Unkeyed {
      async run() {}
    }

    @Workflow('unmetered', { concurrency: { limit: 1, key: () => 'k' } })
    @StartOn(OrderPlacedEvent, { id: (event) => event.orderId, rateLimitKey: (event) => event.orderId })
    class Unmetered {
      async run() {}
    }

    await expectStartupError(
      (await compile({ workflows: [Unkeyed] })).moduleRef,
      'Unkeyed has @StartOn(OrderPlacedEvent) with a concurrencyKey, but workflow "unkeyed" has no concurrency limit per key, so the key ' +
        'would count for nothing. Declare one with @Workflow(name, { concurrency: { limit, key } }).',
    );
    await expectStartupError(
      (await compile({ workflows: [Unmetered] })).moduleRef,
      'Unmetered has @StartOn(OrderPlacedEvent) with a rateLimitKey, but workflow "unmetered" has no rate limit per key, so the key ' +
        'would count for nothing. Declare one with @Workflow(name, { rateLimit: { max, duration, key } }).',
    );
  });
});

/** The CQRS app's providers other than the workflow, over a fresh ledger. */
const shared = (ledger = new Ledger()): Provider[] => [...cqrsProviders, { provide: Ledger, useValue: ledger }];

describe('the publisher', () => {
  it('wraps the publisher from CqrsModule.forRoot({ eventPublisher }), handing it every event after the workflows’', async () => {
    const received: Array<{ event: string; context: unknown; async: boolean; instance: Promise<unknown> }> = [];
    let app!: App;
    const broker: IEventPublisher = {
      publish(event: IEvent, context?: unknown, asyncContext?: AsyncContext) {
        received.push({
          event: event.constructor.name,
          context,
          async: asyncContext instanceof AsyncContext,
          // Read now, when the broker gets the event: the in-memory store answers from its state at the call.
          instance: app.store.get(fulfilmentId((event as OrderPlacedEvent).orderId)),
        });
        return 'sent';
      },
    };
    app = await boot({ workflows: [OrderFulfilmentWorkflow], providers: shared(), eventPublisher: broker });

    // An event no workflow maps goes straight through, synchronously, with the broker's result.
    expect(app.eventBus.publish(new OrderReadyEvent('o-1'), { tenant: 'cats' })).toBe('sent');

    const asyncContext = new AsyncContext();
    await expect(app.eventBus.publish(new OrderPlacedEvent('o-1', 2499), { tenant: 'cats' }, asyncContext)).resolves.toBe('sent');
    expect(await received[1].instance).not.toBeNull();
    expect(received.map(({ event, context, async }) => ({ event, context, async }))).toEqual([
      { event: 'OrderReadyEvent', context: { tenant: 'cats' }, async: false },
      { event: 'OrderPlacedEvent', context: { tenant: 'cats' }, async: true },
    ]);

    // publishAll() without the broker's own publishAll(): one publish() per event, after every write.
    await app.eventBus.publishAll([new OrderPlacedEvent('o-2', 1599), new OrderReadyEvent('o-2')]);
    expect(received.slice(2).map(({ event }) => event)).toEqual(['OrderPlacedEvent', 'OrderReadyEvent']);
    expect(await received[2].instance).not.toBeNull();
  });

  it('starts only the highest registered version, and signals the instances of every version once', async () => {
    @Workflow('order-fulfilment', { version: 2 })
    @StartOn(OrderPlacedEvent, {
      id: (event) => fulfilmentId(event.orderId),
      input: (event) => ({ orderId: event.orderId, total: event.total }),
    })
    @SignalOn(PaymentCapturedEvent, {
      signal: paymentCaptured,
      key: (event) => event.orderId,
      id: (event) => event.chargeId,
      payload: (event) => ({ chargeId: event.chargeId, amount: event.amount }),
    })
    class OrderFulfilmentWorkflowV2 extends OrderFulfilmentWorkflow {}

    const app = await boot({ workflows: [OrderFulfilmentWorkflow, OrderFulfilmentWorkflowV2], providers: shared() });
    // An instance of version 1, started before version 2 was deployed, waits for its payment.
    await app.client.start(OrderFulfilmentWorkflow, { orderId: 'o-1', total: 2499 }, { id: fulfilmentId('o-1'), version: 1 });
    await app.worker.drain();

    await app.eventBus.publish(new OrderPlacedEvent('o-2', 1599));
    expect(await app.client.list()).toMatchObject([
      { id: fulfilmentId('o-1'), version: 1 },
      { id: fulfilmentId('o-2'), version: 2 },
    ]);

    await app.eventBus.publish(new PaymentCapturedEvent('o-1', 'ch_1', 2499));
    expect(await signalsSent(app.store, 'payment.captured', 'o-1')).toHaveLength(1);
    // Redelivered: the charge id is the signal's id, so nothing new is stored.
    await app.eventBus.publish(new PaymentCapturedEvent('o-1', 'ch_1', 2499));
    expect(await signalsSent(app.store, 'payment.captured', 'o-1')).toHaveLength(1);
    await app.worker.drain();
    expect(await app.client.getStatus(fulfilmentId('o-1'))).toMatchObject({ version: 1, status: 'completed' });
  });

  it('sends a signal two workflows map once, and rejects publish() when they disagree on its payload or id', async () => {
    @Workflow('receipt')
    @SignalOn(PaymentCapturedEvent, {
      signal: paymentCaptured,
      key: (event) => event.orderId,
      id: (event) => event.chargeId,
      payload: (event) => ({ amount: event.amount, chargeId: event.chargeId }),
    })
    class ReceiptWorkflow {
      async run(ctx: WorkflowContext) {
        await ctx.waitForSignal('await-payment', paymentCaptured, { key: ctx.workflowId });
      }
    }

    @Workflow('invoice')
    @SignalOn(PaymentCapturedEvent, { signal: paymentCaptured, key: (event) => event.orderId, payload: (event) => ({ amount: 0, chargeId: '' }) })
    class InvoiceWorkflow {
      async run() {}
    }

    @Workflow('ledger')
    @SignalOn(PaymentCapturedEvent, {
      signal: paymentCaptured,
      key: (event) => event.orderId,
      payload: (event) => ({ chargeId: event.chargeId, amount: event.amount }),
    })
    class LedgerWorkflow {
      async run() {}
    }

    const agreeing = await boot({ workflows: [OrderFulfilmentWorkflow, ReceiptWorkflow], providers: shared() });
    await agreeing.eventBus.publish(new PaymentCapturedEvent('o-1', 'ch_1', 2499));
    expect(await signalsSent(agreeing.store, 'payment.captured', 'o-1')).toMatchObject([{ payload: { chargeId: 'ch_1', amount: 2499 } }]);

    const disagreeing = await boot({ workflows: [OrderFulfilmentWorkflow, InvoiceWorkflow], providers: shared() });
    await expect(disagreeing.eventBus.publish(new PaymentCapturedEvent('o-1', 'ch_1', 2499))).rejects.toThrow(
      'PaymentCapturedEvent maps to two different payloads for the signal "payment.captured" (key "o-1"): ' +
        '@SignalOn() on order-fulfilment and on invoice.',
    );
    expect(await signalsSent(disagreeing.store, 'payment.captured', 'o-1')).toEqual([]);

    // Without an id, a redelivery would store a second signal for the instances the other declaration dedupes for.
    const noId = await boot({ workflows: [OrderFulfilmentWorkflow, LedgerWorkflow], providers: shared() });
    await expect(noId.eventBus.publish(new PaymentCapturedEvent('o-1', 'ch_1', 2499))).rejects.toThrow(
      'PaymentCapturedEvent maps to two different ids for the signal "payment.captured" (key "o-1"): ' +
        '@SignalOn() on order-fulfilment and on ledger.',
    );
    expect(await signalsSent(noId.store, 'payment.captured', 'o-1')).toEqual([]);
  });

  it('rejects publish() and writes nothing when a mapping throws, returns a key that is not a string, or an empty id', async () => {
    class ParcelLabelledEvent {}

    @Workflow('packing')
    @StartOn(OrderPlacedEvent, { id: (event) => `packing-${event.orderId}` })
    @StartOn(OrderReadyEvent, {
      id: (event) => {
        throw new Error(`No packing slip for ${event.orderId}.`);
      },
    })
    @SignalOn(PaymentCapturedEvent, { signal: 'packing.paid', key: (event) => event.amount as unknown as string })
    @SignalOn(ParcelLabelledEvent, { signal: 'packing.labelled', id: () => '' })
    class PackingWorkflow {
      async run() {}
    }

    const app = await boot({ workflows: [PackingWorkflow] });
    await expect(app.eventBus.publishAll([new OrderPlacedEvent('o-1', 2499), new OrderReadyEvent('o-1')])).rejects.toThrow(
      'No packing slip for o-1.',
    );
    await expect(app.eventBus.publish(new PaymentCapturedEvent('o-1', 'ch_1', 2499))).rejects.toThrow(
      '@SignalOn(PaymentCapturedEvent) on packing: `key` returned number, not a string.',
    );
    await expect(app.eventBus.publish(new ParcelLabelledEvent())).rejects.toThrow(
      '@SignalOn(ParcelLabelledEvent) on packing: `id` returned an empty string. Derive it from the event, such as event.deliveryId.',
    );
    expect(await app.client.list()).toEqual([]);
    expect(await signalsSent(app.store, 'packing.paid', null)).toEqual([]);
    expect(await signalsSent(app.store, 'packing.labelled', null)).toEqual([]);
  });

  it("rejects publish() and starts nothing when a priority or key @StartOn() computes can't be used", async () => {
    @Workflow('stock-check', { concurrency: { limit: 2, key: () => 'k' } })
    @StartOn(OrderPlacedEvent, { id: (event) => `check-${event.orderId}`, priority: (event) => event.total / 1_000 })
    @StartOn(OrderReadyEvent, { id: (event) => `ready-${event.orderId}`, concurrencyKey: (event) => event.orderId.length as never })
    class StockCheck {
      async run() {}
    }

    const app = await boot({ workflows: [StockCheck] });
    await expect(app.eventBus.publish(new OrderPlacedEvent('o-1', 2499))).rejects.toThrow(
      new TypeError('@StartOn(OrderPlacedEvent) on stock-check: `priority` returned 2.499. Return an integer from 1 (first) to 2097151, or undefined for none.'),
    );
    await expect(app.eventBus.publishAll([new OrderPlacedEvent('o-2', 3000), new OrderReadyEvent('o-2')])).rejects.toThrow(
      new TypeError('@StartOn(OrderReadyEvent) on stock-check: `concurrencyKey` returned number, not a string. Return a non-empty string, or undefined for the computed one.'),
    );
    expect(await app.client.list()).toEqual([]);
  });

  it('starts the workflow from an aggregate’s commit(), warns once per event class, and reports failures on the UnhandledExceptionBus', async () => {
    class Order extends AggregateRoot {
      /** An aggregate may have a field of this name: it is never taken for the dispatcher context's. */
      readonly transaction = 'not a transaction';

      constructor(readonly id: string) {
        super();
      }

      place(total: number) {
        this.apply(new OrderPlacedEvent(this.id, total));
      }
    }

    const app = await boot({ workflows: [OrderFulfilmentWorkflow], providers: shared() });
    const warnings: string[] = [];
    const errors: string[] = [];
    app.moduleRef.useLogger({
      log() {},
      error: (message: string) => void errors.push(message),
      warn: (message: string) => void warnings.push(message),
    });
    const createInTransaction = vi.spyOn(app.store, 'createInTransaction');
    const unhandled: UnhandledExceptionInfo<unknown>[] = [];
    app.moduleRef.get(UnhandledExceptionBus).subscribe((info) => unhandled.push(info));
    const publisher = app.moduleRef.get(EventPublisher);

    const order = publisher.mergeObjectContext(new Order('o-1'));
    order.place(2499);
    order.commit(); // not awaited (before @nestjs/cqrs 12.1, it returns nothing to await)
    await waitFor(async () => (await app.client.getStatus(fulfilmentId('o-1'))) !== null);
    expect(createInTransaction).not.toHaveBeenCalled();

    // The same order placed again with another total: the start conflicts, and nobody awaits commit().
    const again = publisher.mergeObjectContext(new Order('o-1'));
    again.place(9999);
    again.commit();
    await waitFor(() => unhandled.length === 1);
    expect(unhandled[0].cause).toBeInstanceOf(OrderPlacedEvent);
    expect(unhandled[0].exception).toBeInstanceOf(WorkflowIdConflictError);
    expect(errors).toEqual([
      "Publishing OrderPlacedEvent from an aggregate's commit() without a transaction failed. To handle the failure in " +
        'your command handler, and roll its transaction back, await aggregate.commit({ transaction }) (@nestjs/cqrs 12.1 ' +
        'or later). With older versions, await eventBus.publishAll(aggregate.getUncommittedEvents(), { transaction }), then ' +
        'call aggregate.uncommit().',
    ]);

    // An event no workflow maps is none of the module's business.
    const ready = publisher.mergeObjectContext(new Order('o-2'));
    ready.apply(new OrderReadyEvent('o-2'));
    ready.commit();
    expect(warnings).toEqual([
      "OrderPlacedEvent starts or signals workflows, and an aggregate's commit() published it without a transaction: the " +
        "start or signal is written outside your transaction, so it outlives a rollback of your writes, and a crash before it's " +
        'written loses it. Pass your transaction and await the result: await aggregate.commit({ transaction }) (@nestjs/cqrs ' +
        "12.1 or later). With older versions, publish the aggregate's events with eventBus.publishAll(" +
        'aggregate.getUncommittedEvents(), { transaction }), then call aggregate.uncommit(). (Logged once per event class.)',
    ]);
  });

  it('reads the transaction from a plain dispatcher context, and from nothing else', async () => {
    const app = await boot({ workflows: [OrderFulfilmentWorkflow], providers: shared() });
    const createInTransaction = vi.spyOn(app.store, 'createInTransaction');
    const tx = { name: 'a Drizzle tx' };

    await app.eventBus.publish(new OrderPlacedEvent('o-1', 2499), { transaction: tx } satisfies WorkflowDispatcherContext);
    await app.eventBus.publish(new OrderPlacedEvent('o-2', 2499), { tenant: 'cats' });

    // An aggregate's events, published by the command handler in its transaction instead of commit().
    class Order extends AggregateRoot {
      place(id: string, total: number) {
        this.apply(new OrderPlacedEvent(id, total));
      }
    }
    const order = app.moduleRef.get(EventPublisher).mergeObjectContext(new Order());
    order.place('o-3', 1599);
    await app.eventBus.publishAll(order.getUncommittedEvents(), { transaction: tx });
    order.uncommit();

    expect(createInTransaction.mock.calls.map(([transaction, instance]) => [transaction, instance.id])).toEqual([
      [tx, fulfilmentId('o-1')],
      [tx, fulfilmentId('o-3')],
    ]);
    expect(order.getUncommittedEvents()).toEqual([]);
  });

  it('rejects publishAll() in a transaction with the first failed write once every write has settled, also one that throws', async () => {
    const app = await boot({ workflows: [OrderFulfilmentWorkflow], providers: shared() });
    const tx = { name: 'a Drizzle tx' };
    await app.eventBus.publish(new OrderPlacedEvent('o-1', 2499), { transaction: tx });
    const unhandled: unknown[] = [];
    const record = (reason: unknown) => void unhandled.push(reason);
    process.on('unhandledRejection', record);

    try {
      // The first start rejects once the store answers (its id is taken with another input), and the second throws
      // before it returns, as start() does in a transaction (a BigInt in its input): the first is still waited for.
      await expect(
        app.eventBus.publishAll([new OrderPlacedEvent('o-1', 1599), new OrderPlacedEvent('o-2', 10n as never)], { transaction: tx }),
      ).rejects.toThrow(WorkflowIdConflictError);
      // Node reports a rejection nobody handled once the microtasks have run.
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(unhandled).toEqual([]);
    } finally {
      process.off('unhandledRejection', record);
    }
  });
});

// Type checks only (tsc runs over tests/): never called.
export function decoratorTypes() {
  const receipt = new WorkflowSignal<{ receipt: number }>('receipt');

  @Workflow('typed')
  // @ts-expect-error the input is an OrderPlacedEvent, and run() takes a number
  @StartOn(OrderPlacedEvent, { id: (event) => event.orderId })
  // @ts-expect-error payload is required: a PaymentCapturedEvent isn't the signal's payload type
  @SignalOn(PaymentCapturedEvent, { signal: receipt, key: (event) => event.orderId })
  class Typed {
    async run(_ctx: WorkflowContext, _input: number) {}
  }

  @Workflow('inferred')
  @StartOn(OrderPlacedEvent, { id: (event) => event.orderId, input: (event) => ({ orderId: event.orderId, total: event.total }) })
  @SignalOn(PaymentCapturedEvent, { signal: paymentCaptured, key: (event) => event.orderId })
  class Inferred {
    async run(_ctx: WorkflowContext, _input: { orderId: string; total: number }) {}
  }

  return [Typed, Inferred];
}
