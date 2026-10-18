/**
 * `WorkflowClient` at its edges: idempotent starts compared by value, what start() and
 * signal() do to their payloads, the views getStatus() and cancel() return, and the errors
 * for calls the store or the arguments can't serve.
 */
import { Test } from '@nestjs/testing';
import {
  InMemoryWorkflowStore,
  ManualWorkflowClock,
  Workflow,
  WorkflowClient,
  WorkflowIdConflictError,
  WorkflowsModule,
  WorkflowStorage,
  type NewWorkflowInstance,
  type NewWorkflowSignal,
  type WorkflowContext,
  type WorkflowStore,
} from '../lib/index.js';
import { boot, openStore, storeKind, tempDb, type Node, type TestDb } from './support.js';

let db: TestDb;
let clock: ManualWorkflowClock;
const nodes: Node[] = [];

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock();
});

afterEach(async () => {
  for (const node of nodes.splice(0)) {
    await node.close();
  }
  db.cleanup();
});

const start = async (workflows: any[]) => {
  const node = await boot({ db, clock, workflows });
  nodes.push(node);
  return node;
};

@Workflow('echo')
class Echo {
  async run(_ctx: WorkflowContext, input: unknown) {
    return { input, keys: input && typeof input === 'object' ? Object.keys(input) : null };
  }
}

@Workflow('listen')
class Listen {
  async run(ctx: WorkflowContext) {
    const received: unknown[] = [];
    for (const key of ['zero', 'false', 'empty', 'date']) {
      received.push(await ctx.waitForSignal(`wait-${key}`, 'value', { key }));
    }
    return received;
  }
}

/** Sends four signals from a step whose first attempt fails after sending them. */
@Workflow('notify')
class Notify {
  static results: boolean[][] = [];

  constructor(private readonly workflowClient: WorkflowClient) {}

  async run(ctx: WorkflowContext) {
    await ctx.step(
      'notify',
      async ({ attempt }) => {
        const sent = [
          await this.workflowClient.signal('ping', `one: attempt ${attempt}`, { key: 'k' }),
          await this.workflowClient.signal('ping', `two: attempt ${attempt}`, { key: 'k' }),
          await this.workflowClient.signal('ping', `three: attempt ${attempt}`, { key: 'j' }),
          await this.workflowClient.signal('pong', `explicit: attempt ${attempt}`, { id: 'pong-1' }),
        ];
        Notify.results.push(sent.map((result) => result.created));
        if (attempt === 1) {
          throw new Error('the mail server hung up');
        }
      },
      { retry: { attempts: 2, backoff: { delay: '1s' } } },
    );
  }
}

/** Starts three instances from a step whose first attempt fails after starting them. */
@Workflow('spawn')
class Spawn {
  static results: Array<Array<{ id: string; created: boolean }>> = [];

  constructor(private readonly workflowClient: WorkflowClient) {}

  async run(ctx: WorkflowContext) {
    await ctx.step(
      'spawn',
      async ({ attempt }) => {
        const started = [
          await this.workflowClient.start(Echo, { part: 1, attempt }),
          await this.workflowClient.start(Echo, { part: 2, attempt }),
          await this.workflowClient.start(Echo, { part: 3 }, { id: 'explicit-part-3' }),
        ];
        Spawn.results.push(started.map(({ id, created }) => ({ id, created })));
        if (attempt === 1) {
          throw new Error('the warehouse hung up');
        }
      },
      { retry: { attempts: 2, backoff: { delay: '1s' } } },
    );
  }
}

beforeEach(() => {
  Notify.results = [];
  Spawn.results = [];
});

describe('start()', () => {
  it('treats the same input with its keys in another order as the same start', async () => {
    const node = await start([Echo]);
    const first = await node.client.start(Echo, { a: 1, b: { c: 1, d: [1, 2] } }, { id: 'e-1' });
    const again = await node.client.start(Echo, { b: { d: [1, 2], c: 1 }, a: 1 }, { id: 'e-1' });

    expect(first).toMatchObject({ created: true });
    expect(again).toEqual({ id: 'e-1', workflow: 'echo', version: 1, created: false, status: 'pending' });

    // Array order is part of the value.
    await expect(node.client.start(Echo, { a: 1, b: { c: 1, d: [2, 1] } }, { id: 'e-1' })).rejects.toThrow(
      'Instance "e-1" of "echo" already exists with a different input.',
    );
  });

  it('refuses an id taken by another workflow, whatever the input', async () => {
    @Workflow('other')
    class Other {
      async run() {}
    }

    const node = await start([Echo, Other]);
    await node.client.start(Echo, { a: 1 }, { id: 'shared' });

    const attempt = node.client.start(Other, { a: 1 }, { id: 'shared' });
    await expect(attempt).rejects.toBeInstanceOf(WorkflowIdConflictError);
    await expect(attempt).rejects.toThrow('Instance "shared" already exists for workflow "echo", not "other".');
  });

  it('returns the current status and version of an existing instance, also once it finished', async () => {
    const node = await start([Echo]);
    await node.client.start(Echo, undefined, { id: 'done' });
    await node.worker.drain();

    // An undefined input matches itself, although a store may read it back as null.
    expect(await node.client.start(Echo, undefined, { id: 'done' })).toEqual({
      id: 'done',
      workflow: 'echo',
      version: 1,
      created: false,
      status: 'completed',
    });
  });

  it('hands run() the input as JSON: dates as strings, undefined fields gone', async () => {
    const node = await start([Echo]);
    await node.client.start(Echo, { at: new Date(0), note: undefined, tags: ['x', undefined] }, { id: 'json' });
    await node.worker.drain();

    expect(await node.client.getStatus('json')).toMatchObject({
      status: 'completed',
      input: { at: '1970-01-01T00:00:00.000Z', tags: ['x', null] },
      output: { input: { at: '1970-01-01T00:00:00.000Z', tags: ['x', null] }, keys: ['at', 'tags'] },
    });
  });

  it('refuses an input that is not JSON before creating anything', async () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;

    const node = await start([Echo]);
    await expect(node.client.start(Echo, { amount: 10n }, { id: 'big' })).rejects.toThrow(TypeError);
    await expect(node.client.start(Echo, circular, { id: 'loop' })).rejects.toThrow(TypeError);
    expect(await node.client.list()).toEqual([]);
  });

  it('generates a fresh id for each start without one', async () => {
    const node = await start([Echo]);
    const a = await node.client.start(Echo, { same: true });
    const b = await node.client.start(Echo, { same: true });

    expect(a.id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(b.id).not.toBe(a.id);
    expect([a.created, b.created]).toEqual([true, true]);
    expect(await node.client.list()).toHaveLength(2);
  });
});

describe('start() inside a step', () => {
  it('derives the id from the step: a retried step gets its instances back, two starts get two, and an explicit id wins', async () => {
    const node = await start([Spawn, Echo]);
    await node.client.start(Spawn, undefined, { id: 's-1' });
    await node.worker.drain();
    clock.advance('1s');
    await node.worker.drain();

    const [first, retried] = Spawn.results;
    expect(first!.map((s) => s.created)).toEqual([true, true, true]);
    expect(retried).toEqual(first!.map((s) => ({ ...s, created: false })));
    expect(first!.map((s) => s.id)).toEqual([JSON.stringify(['s-1:spawn', 'echo', 1]), JSON.stringify(['s-1:spawn', 'echo', 2]), 'explicit-part-3']);

    // The retry's input differs (attempt 2): the first one wins, instead of a conflict failing the step.
    expect(await node.client.getStatus('s-1')).toMatchObject({ status: 'completed' });
    const echoes = await node.client.list({ workflow: 'echo' });
    expect(echoes.map((i) => i.input).sort((a: any, b: any) => a.part - b.part)).toEqual([{ part: 1, attempt: 1 }, { part: 2, attempt: 1 }, { part: 3 }]);

    // Outside a step, the same call starts another instance.
    expect(await node.client.start(Echo, { part: 1, attempt: 1 })).toMatchObject({ created: true });
    expect(await node.client.list({ workflow: 'echo' })).toHaveLength(4);
  });
});

describe('getStatus() and cancel()', () => {
  it('returns null for an unknown id, and the journal only on request', async () => {
    const node = await start([Echo]);
    await node.client.start(Echo, { a: 1 }, { id: 'e-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('missing')).toBeNull();
    expect(await node.client.getStatus('missing', { journal: true })).toBeNull();

    const plain = await node.client.getStatus('e-1');
    expect(plain).not.toHaveProperty('journal');
    expect(plain).toMatchObject({ id: 'e-1', workflow: 'echo', version: 1, status: 'completed', waits: [] });
    expect(await node.client.getStatus('e-1', { journal: true })).toMatchObject({ journal: [] });
  });

  it('returns the instance without its waits or journal, and names a cancel without reason "Cancelled."', async () => {
    const node = await start([Listen]);
    await node.client.start(Listen, undefined, { id: 'l-1' });
    await node.worker.drain();

    const result = await node.client.cancel('l-1');
    expect(result).toMatchObject({ id: 'l-1', accepted: true, cancelRequested: true, cancelReason: null, status: 'suspended' });
    expect(result).not.toHaveProperty('waits');
    expect(result).not.toHaveProperty('journal');

    await node.worker.drain();
    expect(await node.client.getStatus('l-1')).toMatchObject({
      status: 'cancelled',
      error: { name: 'WorkflowCancelledError', message: 'Cancelled.' },
    });
  });
});

describe('signal()', () => {
  it('delivers falsy payloads as themselves, and payloads as JSON', async () => {
    const node = await start([Listen]);
    await node.client.start(Listen, undefined, { id: 'l-1' });
    await node.worker.drain();

    await node.client.signal('value', 0, { key: 'zero' });
    await node.client.signal('value', false, { key: 'false' });
    await node.client.signal('value', '', { key: 'empty' });
    await node.client.signal('value', { at: new Date(0), dropped: undefined }, { key: 'date' });
    await node.worker.drain();

    expect(await node.client.getStatus('l-1')).toMatchObject({
      status: 'completed',
      output: [0, false, '', { at: '1970-01-01T00:00:00.000Z' }],
    });
    expect((await node.client.getStatus('l-1'))!.output).toEqual([0, false, '', { at: '1970-01-01T00:00:00.000Z' }]);
  });

  it('returns increasing signal ids, and counts only the instances it wakes', async () => {
    const node = await start([Listen]);
    await node.client.start(Listen, undefined, { id: 'l-1' });
    await node.client.start(Listen, undefined, { id: 'l-2' });
    await node.worker.drain();

    const first = await node.client.signal('value', 1, { key: 'zero' });
    expect(first.woken).toBe(2);

    // Both are due already: a second matching signal wakes nobody new.
    const second = await node.client.signal('value', 2, { key: 'zero' });
    expect(second.signalId).toBeGreaterThan(first.signalId);
    expect(second.woken).toBe(0);
  });

  it('stores a signal with an id once per name, returns the first one for a repeat, and refuses the id for another key', async () => {
    const node = await start([Listen]);
    await node.client.start(Listen, undefined, { id: 'l-1' });
    await node.worker.drain();

    const first = await node.client.signal('value', 0, { key: 'zero', id: 'evt-1' });
    expect(first).toMatchObject({ woken: 1, created: true });
    // A redelivery with another payload: the first one stays.
    expect(await node.client.signal('value', 99, { key: 'zero', id: 'evt-1' })).toEqual({ signalId: first.signalId, woken: 0, created: false });
    await expect(node.client.signal('value', 1, { key: 'false', id: 'evt-1' })).rejects.toThrow(
      new WorkflowIdConflictError('Signal id "evt-1" of "value" was already used with key "zero", not key "false".'),
    );
    await expect(node.client.signal('value', 1, { id: 'evt-1' })).rejects.toThrow('was already used with key "zero", not no key.');
    expect(await node.client.signal('other', 1, { key: 'zero', id: 'evt-1' })).toMatchObject({ created: true });

    await node.worker.drain();
    expect(await node.store.signals({ name: 'value', key: 'zero', afterId: 0, upToId: Number.MAX_SAFE_INTEGER })).toEqual([
      expect.objectContaining({ id: first.signalId, payload: 0 }),
    ]);
    expect(await node.store.signals({ name: 'value', key: 'false', afterId: 0, upToId: Number.MAX_SAFE_INTEGER })).toEqual([]);
    expect(await node.client.getStatus('l-1')).toMatchObject({ status: 'suspended', waits: [{ signal: 'value', key: 'false' }] });
  });

  it('rejects an empty id before writing anything', async () => {
    const node = await start([]);
    await expect(node.client.signal('value', 1, { id: '' })).rejects.toThrow(
      new TypeError('Invalid signal id "". Use a non-empty string, such as the id of the event that causes it.'),
    );
    expect(await node.store.signals({ name: 'value', key: null, afterId: 0, upToId: Number.MAX_SAFE_INTEGER })).toEqual([]);
  });

  it('derives the id inside a step: a retried step stores its signals once, and several with one name and key apart', async () => {
    const node = await start([Notify]);
    await node.client.start(Notify, undefined, { id: 'n-1' });
    await node.worker.drain();
    // The first attempt sent its signals, then failed.
    expect(await node.client.getStatus('n-1', { journal: true })).toMatchObject({ journal: [{ name: 'notify', status: 'pending', attempts: 1 }] });

    clock.advance('1s');
    await node.worker.drain();
    expect(await node.client.getStatus('n-1')).toMatchObject({ status: 'completed' });

    const read = (name: string, key: string | null) => node.store.signals({ name, key, afterId: 0, upToId: Number.MAX_SAFE_INTEGER });
    expect((await read('ping', 'k')).map((s) => s.payload)).toEqual(['one: attempt 1', 'two: attempt 1']);
    expect((await read('ping', 'j')).map((s) => s.payload)).toEqual(['three: attempt 1']);
    expect((await read('pong', null)).map((s) => s.payload)).toEqual(['explicit: attempt 1']);
    expect(Notify.results).toEqual([
      [true, true, true, true],
      [false, false, false, false],
    ]);

    // Outside a step, the same call stores another signal.
    await node.client.signal('ping', 'from a controller', { key: 'k' });
    expect(await read('ping', 'k')).toHaveLength(3);
  });

  it('rejects an empty signal name before writing anything', async () => {
    const node = await start([]);
    await expect(node.client.signal('', { any: 1 })).rejects.toThrow('Invalid signal name "". Use a non-empty string such as "shipment.delivered".');
  });
});

describe('purge()', () => {
  it('deletes what finished longer ago than olderThan, batch by batch, and the signals nobody can take', async () => {
    const node = await start([Echo, Listen]);
    for (const id of ['old-1', 'old-2', 'old-3']) {
      await node.client.start(Echo, { id }, { id });
    }
    await node.client.start(Listen, undefined, { id: 'listening' });
    await node.worker.drain();
    await node.client.signal('value', 0, { key: 'zero' }); // taken by 'listening'
    await node.worker.drain();
    await node.client.signal('unheard', 1);

    clock.advance('31d');
    await node.client.start(Echo, { id: 'recent' }, { id: 'recent' });
    await node.worker.drain();
    await node.client.signal('unheard', 2);

    expect(await node.client.purge({ olderThan: '30d', batchSize: 2 })).toEqual({ instances: 3, signals: 0, rateLimits: 0 });
    expect((await node.client.list()).map((i) => i.id)).toEqual(['listening', 'recent']);

    // Once the listener is gone too, its signals go, except the newest.
    await node.client.cancel('listening');
    await node.worker.drain();
    clock.advance('31d');
    expect(await node.client.purge({ olderThan: '30d' })).toEqual({ instances: 2, signals: 2, rateLimits: 0 });
    expect(await node.store.signals({ name: 'unheard', key: null, afterId: 0, upToId: Number.MAX_SAFE_INTEGER })).toMatchObject([{ payload: 2 }]);
    expect(await node.client.list()).toEqual([]);
  });

  it('keeps compensation_failed instances unless asked, and refuses unfinished statuses', async () => {
    const node = await start([Echo]);
    await node.store.create({ id: 'stuck', workflow: 'echo', version: 1, input: null, deadline: null, now: clock.now() });
    const [claimed] = (await node.store.claim({ owner: 'w', token: 't', now: clock.now(), leaseUntil: clock.now() + 1_000, limit: 1, workflows: [{ name: 'echo', version: 1 }] })).instances;
    await node.store.write(claimed!.id, 't', { now: clock.now(), entries: [], status: 'compensation_failed', error: { name: 'Error', message: 'x' }, release: { wakeAt: null, waits: [], signalCursor: 0 } });
    clock.advance('1d');

    expect(await node.client.purge({ olderThan: '1h' })).toEqual({ instances: 0, signals: 0, rateLimits: 0 });
    expect(await node.client.purge({ olderThan: '1h', status: 'compensation_failed' })).toEqual({ instances: 1, signals: 0, rateLimits: 0 });

    await expect(node.client.purge({ olderThan: '1h', status: ['completed', 'running'] })).rejects.toThrow(
      new TypeError('purge(): status must list finished statuses (completed, failed, cancelled, compensation_failed), not running. Cancel an unfinished instance first.'),
    );
    await expect(node.client.purge({ olderThan: '1h', batchSize: 0 })).rejects.toThrow('purge(): batchSize (0) must be a positive integer.');
    await expect(node.client.purge({ olderThan: 'a month' as never })).rejects.toThrow(TypeError);
  });
});

// No store involved: once.
describe.runIf(storeKind === 'memory')('transactions on a store without the optional methods', () => {
  /** A store that implements only the required methods. */
  class BareStore implements WorkflowStore {
    private readonly inner = new InMemoryWorkflowStore();
    create = this.inner.create.bind(this.inner);
    get = this.inner.get.bind(this.inner);
    list = this.inner.list.bind(this.inner);
    requestCancel = this.inner.requestCancel.bind(this.inner);
    reopen = this.inner.reopen.bind(this.inner);
    delete = this.inner.delete.bind(this.inner);
    signal = this.inner.signal.bind(this.inner);
    signals = this.inner.signals.bind(this.inner);
    purge = this.inner.purge.bind(this.inner);
    saveSchedule = this.inner.saveSchedule.bind(this.inner);
    getSchedule = this.inner.getSchedule.bind(this.inner);
    listSchedules = this.inner.listSchedules.bind(this.inner);
    deleteSchedule = this.inner.deleteSchedule.bind(this.inner);
    claimSchedules = this.inner.claimSchedules.bind(this.inner);
    writeSchedule = this.inner.writeSchedule.bind(this.inner);
    claim = this.inner.claim.bind(this.inner);
    renew = this.inner.renew.bind(this.inner);
    write = this.inner.write.bind(this.inner);
  }

  it('refuses start() and signal() with { transaction }, and writes nothing', async () => {
    const moduleRef = await Test.createTestingModule({ imports: [WorkflowsModule.forRoot({ clock, worker: false })], providers: [Echo] }).compile();
    const store = new BareStore();
    moduleRef.get(WorkflowStorage).registerSource(store);
    await moduleRef.init();
    const client = moduleRef.get(WorkflowClient);

    try {
      expect(() => client.start(Echo, {}, { id: 'tx', transaction: {} })).toThrow(
        'start() with { transaction } needs a WorkflowStore on your database that implements createInTransaction(); ' +
          'BareStore has none. See https://docs.nestjs.com/reliability/workflows#the-store-contract.',
      );
      expect(() => client.signal('value', 1, { transaction: {} })).toThrow(
        'signal() with { transaction } needs a WorkflowStore on your database that implements signalInTransaction(); BareStore has none.',
      );

      expect(await store.get('tx')).toBeNull();
      expect(await store.signals({ name: 'value', key: null, afterId: 0, upToId: Number.MAX_SAFE_INTEGER })).toEqual([]);

      // Without a transaction, the same store serves both.
      expect(await client.start(Echo, {}, { id: 'plain' })).toMatchObject({ created: true });
    } finally {
      await moduleRef.close();
    }
  });
});

// A store of the test's own: once.
describe.runIf(storeKind === 'memory')('transactions on a synchronous driver, such as better-sqlite3', () => {
  /**
   * The application's database, with better-sqlite3's transactions: `transaction(fn)` runs `fn` at once, and what it
   * wrote commits if it returns, or rolls back if it throws. A `fn` that returns a promise is refused, as it would
   * commit before the promise settles: `start()` and `signal()` aren't awaited in it, so their failure rolls the
   * transaction back only when they throw it before they return.
   */
  class Database {
    orders = new Set<string>();
    instances = new Map<string, NewWorkflowInstance>();
    signals: Array<NewWorkflowSignal & { id: number }> = [];

    transaction(fn: (tx: Database) => unknown): void {
      const tx = Object.assign(new Database(), { orders: new Set(this.orders), instances: new Map(this.instances), signals: [...this.signals] });
      if (fn(tx) instanceof Promise) {
        throw new TypeError('Transaction function cannot return a promise');
      }
      Object.assign(this, tx);
    }
  }

  /** The test database's store, joining such a transaction as a store on that driver does: its writes return at once. */
  const joinsSynchronously = () =>
    Object.assign(openStore(db).store, {
      createInTransaction(tx: Database, instance: NewWorkflowInstance) {
        const stored = tx.instances.get(instance.id) ?? instance;
        tx.instances.set(instance.id, stored);
        // What start() reads of the stored instance.
        return { instance: { ...stored, status: 'pending' }, created: stored === instance };
      },
      signalInTransaction(tx: Database, signal: NewWorkflowSignal) {
        const earlier = tx.signals.find((s) => signal.dedupeId !== null && s.name === signal.name && s.dedupeId === signal.dedupeId);
        if (earlier) {
          return { id: earlier.id, woken: 0, created: false, key: earlier.key };
        }
        const id = tx.signals.length + 1;
        tx.signals.push({ ...signal, id });
        return { id, woken: 0, created: true, key: signal.key };
      },
    });

  /** An application on that store, and its database with order "o-1" placed: its instance started, its payment signalled. */
  const placed = async () => {
    joinsSynchronously();
    const node = await start([Echo]);
    const database = new Database();
    const results: Array<Promise<unknown>> = [];
    database.transaction((tx) => {
      tx.orders.add('o-1');
      results.push(node.client.start(Echo, { orderId: 'o-1' }, { id: 'order-o-1', transaction: tx }));
      results.push(node.client.signal('order.paid', { amount: 2499 }, { key: 'o-1', id: 'charge-1', transaction: tx }));
    });
    return { node, database, results: Promise.all(results) };
  };

  /**
   * Places order "o-2" with `write` in one transaction: `write` must throw `error` before it returns, so the order rolls
   * back, and leave no rejection behind for nobody to handle.
   */
  const rollsBack = async (database: Database, write: (tx: Database) => unknown, error: string | Error) => {
    const unhandled: unknown[] = [];
    const record = (reason: unknown) => void unhandled.push(reason);
    process.on('unhandledRejection', record);
    try {
      expect(() =>
        database.transaction((tx) => {
          tx.orders.add('o-2');
          write(tx);
        }),
      ).toThrow(error);
      expect(database.orders.has('o-2')).toBe(false);
      // Node reports a rejection nobody handled once the microtasks have run.
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(unhandled).toEqual([]);
    } finally {
      process.off('unhandledRejection', record);
    }
  };

  it('runs start() and signal() in the transaction, so what they write commits with its rows', async () => {
    const { database, results } = await placed();

    expect(await results).toEqual([
      { id: 'order-o-1', workflow: 'echo', version: 1, created: true, status: 'pending' },
      { signalId: 1, woken: 0, created: true },
    ]);
    expect([...database.orders]).toEqual(['o-1']);
    expect([...database.instances.keys()]).toEqual(['order-o-1']);
    expect(database.signals).toMatchObject([{ id: 1, name: 'order.paid', key: 'o-1', dedupeId: 'charge-1' }]);
  });

  const full = new Error('SQLITE_FULL: database or disk is full');
  it.each([
    {
      call: 'start() with an input that is not JSON',
      write: (node: Node, tx: Database) => node.client.start(Echo, { amount: 10n }, { id: 'order-o-2', transaction: tx }),
      error: new TypeError('Do not know how to serialize a BigInt'),
    },
    {
      call: 'signal() with a payload that is not JSON',
      write: (node: Node, tx: Database) => node.client.signal('order.paid', { amount: 10n }, { key: 'o-2', transaction: tx }),
      error: new TypeError('Do not know how to serialize a BigInt'),
    },
    {
      call: 'start() with a conflicting id',
      write: (node: Node, tx: Database) => node.client.start(Echo, { orderId: 'o-2' }, { id: 'order-o-1', transaction: tx }),
      error: new WorkflowIdConflictError('Instance "order-o-1" of "echo" already exists with a different input.'),
    },
    {
      call: 'signal() with a conflicting id',
      write: (node: Node, tx: Database) => node.client.signal('order.paid', { amount: 2499 }, { key: 'o-2', id: 'charge-1', transaction: tx }),
      error: new WorkflowIdConflictError('Signal id "charge-1" of "order.paid" was already used with key "o-1", not key "o-2".'),
    },
    {
      call: 'start() on a full disk',
      write: (node: Node, tx: Database) => {
        vi.spyOn(node.store, 'createInTransaction').mockImplementationOnce(() => {
          throw full;
        });
        return node.client.start(Echo, { orderId: 'o-2' }, { id: 'order-o-2', transaction: tx });
      },
      error: full,
    },
    {
      call: 'signal() on a full disk',
      write: (node: Node, tx: Database) => {
        vi.spyOn(node.store, 'signalInTransaction').mockImplementationOnce(() => {
          throw full;
        });
        return node.client.signal('order.paid', { amount: 2499 }, { key: 'o-2', transaction: tx });
      },
      error: full,
    },
  ])('$call throws before it returns, so the transaction rolls back', async ({ write, error }) => {
    const { node, database } = await placed();

    await rollsBack(database, (tx) => write(node, tx), error);
    expect([...database.instances.keys()]).toEqual(['order-o-1']);
    expect(database.signals).toHaveLength(1);
  });

  it("throws before start() and signal() return when the store can't join the transaction, so it rolls back", async () => {
    Object.assign(openStore(db).store, { createInTransaction: undefined, signalInTransaction: undefined });
    const node = await start([Echo]);
    const database = new Database();

    await rollsBack(database, (tx) => node.client.start(Echo, {}, { id: 'order-o-2', transaction: tx }), 'implements createInTransaction()');
    await rollsBack(database, (tx) => node.client.signal('order.paid', 1, { transaction: tx }), 'implements signalInTransaction()');
  });
});
