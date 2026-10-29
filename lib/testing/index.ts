import { randomUUID } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { absent, equal, expect, jitter, rejects } from './assertions.js';
import { scheduleCases } from './schedule-store.contract.js';
import { workflowScheduleStore } from '../utils/schedule-store.util.js';
import type { WorkflowJournalEntry } from '../interfaces/workflow-instance.interface.js';
import type {
  WorkflowClaim,
  WorkflowConcurrencyLimit,
  WorkflowRateLimitRule,
  WorkflowStore,
  WorkflowWrite,
} from '../interfaces/workflow-store.interface.js';

export { scheduleStoreContract, type ScheduleStoreContractCase, type ScheduleStoreContractOptions } from './schedule-store.contract.js';

export interface WorkflowStoreContractOptions {
  /**
   * Also run the concurrency cases: many calls in flight at once, racing claims, signals,
   * suspensions and cancels. Set it for a store on a connection pool (they are the cases a
   * store without the right locks fails); a single connection serializes them, which they
   * must survive too.
   */
  concurrent?: boolean;
  /**
   * Opens an application transaction and runs `work` in it, such as
   * `(work) => db.transaction(work)`: the cases for `createInTransaction()` and
   * `signalInTransaction()` run when it is set and the store has them.
   */
  transaction?: <T>(work: (transaction: unknown) => Promise<T>) => Promise<T>;
  /** Called after each case with the store `createStore` returned (close it, drop its tables). */
  dispose?: (store: WorkflowStore) => unknown;
}

export interface WorkflowStoreContractCase {
  name: string;
  /** Throws on failure. */
  run(): Promise<void>;
}

/**
 * The `WorkflowStore` contract as test cases, for any test runner:
 *
 * ```ts
 * const cases = workflowStoreContract(async () => new DrizzleWorkflowStore(await freshDb(), new WorkflowStorage()), {
 *   concurrent: true,
 *   transaction: (work) => db.transaction(work),
 * });
 * for (const c of cases) it(c.name, c.run);
 * ```
 *
 * `createStore` is called once per case and must return a store on empty tables.
 */
export function workflowStoreContract(
  createStore: () => WorkflowStore | Promise<WorkflowStore>,
  options: WorkflowStoreContractOptions = {},
): WorkflowStoreContractCase[] {
  const cases: WorkflowStoreContractCase[] = [];
  const add = (name: string, run: (t: Harness) => Promise<void>) =>
    cases.push({
      name,
      run: async () => {
        const store = await createStore();
        try {
          await run(new Harness(store));
        } finally {
          await options.dispose?.(store);
        }
      },
    });

  const transaction = options.transaction;

  // ---------------------------------------------------------------- instances

  add('create() inserts a pending instance once, and returns the stored one for an existing id', async (t) => {
    const input = { title: "O'Reilly — Designing Data-Intensive Applications ü 🚀", qty: 2, nested: [null, { a: true }], empty: '' };
    const first = await t.store.create({ id: 'order-1', workflow: 'order-fulfilment', version: 3, input, deadline: null, now: 1_000 });
    expect(first, {
      created: true,
      instance: {
        id: 'order-1',
        workflow: 'order-fulfilment',
        version: 3,
        parentId: null,
        parentClose: null,
        concurrencyKey: null,
        rateLimitKey: null,
        priority: 0,
        scheduleId: null,
        scheduledAt: null,
        status: 'pending',
        input,
        error: null,
        wakeAt: 1_000,
        leaseOwner: null,
        leaseUntil: null,
        cancelRequested: false,
        terminateRequested: false,
        cancelReason: null,
        deadline: null,
        customStatus: null,
        signalCursor: 0,
        runs: 0,
        createdAt: 1_000,
        updatedAt: 1_000,
      },
    }, 'the new instance');
    absent(first.instance.output, 'output of a new instance', { orNull: true });

    const again = await t.store.create({ id: 'order-1', workflow: 'other', version: 1, input: 'different', deadline: null, now: 2_000 });
    expect(again, { created: false, instance: first.instance }, 'the existing instance, unchanged');
    expect(await t.store.get('order-1'), { ...first.instance, waits: [] }, 'get()');
    equal(await t.store.get('missing'), null, 'get() of an unknown id');

    equal((await t.store.create({ id: 'null-input', workflow: 'w', version: 1, input: null, deadline: null, now: 0 })).instance.input, null, 'a null input');
    absent((await t.store.create({ id: 'no-input', workflow: 'w', version: 1, input: undefined, deadline: null, now: 0 })).instance.input, 'an undefined input', { orNull: true });
    equal((await t.store.get('order-1', { journal: true }))?.journal, [], 'the journal of a new instance');

    const timed = await t.store.create({ id: 'timed', workflow: 'w', version: 1, input: null, deadline: 86_401_000, now: 1_000 });
    expect(timed.instance, { deadline: 86_401_000 }, 'a deadline');
    expect(await t.store.get('timed'), { deadline: 86_401_000 }, 'a deadline, read back');
  });

  add('create() starts the signal cursor at the last signal sent before the instance', async (t) => {
    await t.store.signal({ name: 's', key: null, dedupeId: null, payload: 1, now: 0 });
    const { id } = await t.store.signal({ name: 's', key: null, dedupeId: null, payload: 2, now: 0 });
    equal((await t.create('a')).instance.signalCursor, id, 'signalCursor');
    equal((await t.claim(0)).lastSignalId, id, 'claim().lastSignalId');
  });

  add('list() filters by status, workflow and version, oldest first, a page at a time', async (t) => {
    await t.create('c', 3);
    await t.create('a', 1);
    await t.create('b', 2, 'order-fulfilment', 2);
    await t.create('d', 4, 'invoice-batch');
    await t.create('e', 3); // same createdAt as 'c': ordered by id
    await t.claim(1, { limit: 1 }); // 'a' is now running

    const ids = async (query: Partial<Parameters<WorkflowStore['list']>[0]>) =>
      (await t.store.list({ limit: 100, offset: 0, ...query })).map((i) => i.id);
    equal(await ids({}), ['a', 'b', 'c', 'e', 'd'], 'every instance');
    equal(await ids({ status: ['running'] }), ['a'], 'by status');
    equal(await ids({ status: ['pending', 'running'], limit: 2, offset: 1 }), ['b', 'c'], 'a page');
    equal(await ids({ workflow: 'order-fulfilment', version: 1 }), ['a', 'c', 'e'], 'by workflow and version');
    equal(await ids({ workflow: 'invoice-batch' }), ['d'], 'by workflow');
    equal(await ids({ status: ['completed'] }), [], 'nothing matches');
  });

  add('create() keeps the parent link, and list() finds the children by parentId', async (t) => {
    await t.create('order-1');
    const child = (id: string, parentId: string, parentClose: 'cancel' | 'terminate' | 'abandon', now: number) =>
      t.store.create({ id, workflow: 'shipping', version: 1, input: { id }, deadline: null, parentId, parentClose, now });
    expect(await child('order-1/shipping#1', 'order-1', 'cancel', 5), { created: true, instance: { parentId: 'order-1', parentClose: 'cancel' } }, 'a child');
    await child('order-1/shipping#2', 'order-1', 'abandon', 6);
    await child('order-2/shipping#1', 'order-2', 'terminate', 4);

    expect(await t.store.get('order-1/shipping#2'), { parentId: 'order-1', parentClose: 'abandon' }, 'read back');
    expect(await t.store.get('order-1'), { parentId: null, parentClose: null }, 'no parent');
    const children = async (parentId: string, extra: Partial<Parameters<WorkflowStore['list']>[0]> = {}) =>
      (await t.store.list({ limit: 100, offset: 0, parentId, ...extra })).map((i) => i.id);
    equal(await children('order-1'), ['order-1/shipping#1', 'order-1/shipping#2'], "order-1's children, oldest first");
    equal(await children('order-1', { status: ['pending'], limit: 1, offset: 1 }), ['order-1/shipping#2'], 'a page, with other filters');
    equal(await children('order-2'), ['order-2/shipping#1'], "order-2's");
    equal(await children('order-1/shipping#1'), [], 'none');
  });

  add("create() keeps the schedule link, and list() finds a schedule's instances by scheduleId", async (t) => {
    const scheduled = (id: string, scheduleId: string, scheduledAt: number, now: number) =>
      t.store.create({ id, workflow: W.name, version: 1, input: null, deadline: null, scheduleId, scheduledAt, now });
    expect(await scheduled('digest@1', 'digest', 1_000, 5), { created: true, instance: { scheduleId: 'digest', scheduledAt: 1_000 } }, 'an occurrence');
    await scheduled('digest@2', 'digest', 2_000, 6);
    await scheduled('report@1', 'report', 1_000, 4);
    await t.create('manual', 3);
    expect(await t.store.get('digest@2'), { scheduleId: 'digest', scheduledAt: 2_000 }, 'read back');

    const ids = async (scheduleId: string, extra: Partial<Parameters<WorkflowStore['list']>[0]> = {}) =>
      (await t.store.list({ limit: 100, offset: 0, scheduleId, ...extra })).map((i) => i.id);
    equal(await ids('digest'), ['digest@1', 'digest@2'], "digest's, oldest first");
    equal(await ids('digest', { status: ['pending'], limit: 1, offset: 1 }), ['digest@2'], 'a page, with other filters');
    equal(await ids('none'), [], 'none');
  });

  // ---------------------------------------------------------------- claims and leases

  add('claim() leases due, unleased instances of the given versions, most overdue first', async (t) => {
    await t.create('late', 50);
    await t.create('early', 10);
    await t.create('v2', 5, 'order-fulfilment', 2);
    await t.create('future', 5_000);

    const first = await t.claim(100, { owner: 'w1', token: 't1' });
    equal(first.instances.map((i) => i.id), ['early', 'late'], 'claimed, by wakeAt');
    expect(first.instances[0], { status: 'running', leaseOwner: 'w1', leaseUntil: 1_100, runs: 1, updatedAt: 100 }, 'a claimed instance');
    equal((await t.claim(1_100, { owner: 'w2' })).instances, [], 'leased until 1100 inclusive');

    const retaken = await t.claim(1_101, { owner: 'w2', token: 't2' });
    equal(retaken.instances.map((i) => [i.id, i.leaseOwner, i.runs]), [['early', 'w2', 2], ['late', 'w2', 2]], 'claimed again after the lease expired');
    equal(await t.store.write('early', 't1', t.journalWrite([entry('x')])), false, 'a write under the first token');
    equal(await t.store.write('early', 't2', t.journalWrite([entry('x')])), true, 'a write under the new token');

    const v2 = await t.claim(2_000, { owner: 'w3', limit: 1, workflows: [{ name: 'order-fulfilment', version: 2 }, { name: 'invoice-batch', version: 1 }] });
    equal(v2.instances.map((i) => i.id), ['v2'], 'only the versions the worker runs');
    equal((await t.claim(6_000, { owner: 'w4', limit: 1 })).instances.map((i) => i.id), ['early'], 'at most limit');
  });

  add("claim() under a workflow's limit leases no more than it allows, counting the leases still live", async (t) => {
    for (const [i, id] of ['a', 'b', 'c', 'd'].entries()) {
      await t.keyed(id, null, i);
    }
    await t.create('x', 0, 'invoice-batch');
    const limits = [{ workflow: W.name, limit: 2, perKey: null }];
    const claim = (now: number, token: string) =>
      t.claim(now, { token, limits, leaseUntil: now + 100, workflows: [W, { name: 'invoice-batch', version: 1 }] });

    equal((await claim(10, 't1')).instances.map((i) => i.id), ['a', 'x', 'b'], 'two of the limited workflow, and the unlimited one');
    equal((await claim(20, 't2')).instances.map((i) => i.id), [], 'both slots held');
    equal(await t.release('a', 't1'), true, "a's lease ends");
    equal((await claim(30, 't3')).instances.map((i) => i.id), ['c'], 'the slot it freed');
    // b's lease (until 110) expired: b is due again, holds no slot, and goes before d. So is x's.
    equal((await claim(111, 't4')).instances.map((i) => [i.id, i.runs]), [['x', 2], ['b', 2]], 'the slot an expired lease freed');
    equal((await t.claim(111, { token: 't5' })).instances.map((i) => i.id), ['d'], 'a claim without limits');
  });

  add('claim() keeps at most perKey of a key leased, passes over a full key, and takes each key in order', async (t) => {
    await t.keyed('c1-a', 'customer-1', 0);
    await t.keyed('c1-b', 'customer-1', 1);
    await t.keyed('c2-a', 'customer-2', 2);
    await t.keyed('none', null, 3);
    await t.keyed('c1-c', 'customer-1', 4);
    await t.keyed('c3-a', 'customer-3', 5);
    expect(await t.store.get('c1-a'), { concurrencyKey: 'customer-1' }, 'the key, stored');
    const perKey = [{ workflow: W.name, limit: null, perKey: 1 }];

    equal((await t.claim(10, { token: 't1', limits: perKey, limit: 3 })).instances.map((i) => i.id), ['c1-a', 'c2-a', 'none'], 'one per key, and one without a key');
    equal((await t.claim(10, { token: 't2', limits: perKey })).instances.map((i) => i.id), ['c3-a'], 'the next free key');
    equal((await t.claim(10, { token: 't3', limits: perKey })).instances.map((i) => i.id), [], 'every key full');
    await t.release('c1-a', 't1');
    equal((await t.claim(10, { token: 't4', limits: perKey })).instances.map((i) => i.id), ['c1-b'], "the key's next, not its last");

    // Both limits: the workflow's counts every key's.
    const both = [{ workflow: W.name, limit: 5, perKey: 1 }];
    await t.keyed('c4-a', 'customer-4', 6);
    await t.keyed('c5-a', 'customer-5', 7);
    equal((await t.claim(10, { token: 't5', limits: both })).instances.map((i) => i.id), ['c4-a'], 'the fifth slot');
    equal((await t.claim(10, { token: 't6', limits: both })).instances.map((i) => i.id), [], 'the workflow full');
  });

  add('claim() takes the lowest priority first (none before any), then the most overdue, with or without limits', async (t) => {
    const instances: Array<[string, number, number]> = [
      ['late-none', 50, 0],
      ['p5', 0, 5],
      ['early-none', 10, 0],
      ['p1-late', 40, 1],
      ['p1-early', 20, 1],
    ];
    for (const [id, now, priority] of instances) {
      await t.prioritized(id, now, priority === 0 ? {} : { priority });
    }
    expect(await t.store.get('p5'), { priority: 5, rateLimitKey: null }, 'the priority, stored');
    expect(await t.store.get('late-none'), { priority: 0 }, 'none given: 0');

    const order = ['early-none', 'late-none', 'p1-early', 'p1-late', 'p5'];
    equal(await t.claimAndFinish(100, { limit: 3 }), order.slice(0, 3), 'the first three');
    equal(await t.claimAndFinish(100), order.slice(3), 'the rest');

    // The same order through the limited path, one at a time.
    for (const [id, now, priority] of instances) {
      await t.prioritized(`k-${id}`, now, { priority });
    }
    const limits = [{ workflow: W.name, limit: 1, perKey: null }];
    const taken: string[] = [];
    for (let i = 0; i < order.length; i++) {
      const [instance] = (await t.claim(200, { token: `l${i}`, limits, leaseUntil: 9_000 })).instances;
      taken.push(instance?.id ?? 'nothing');
      await t.release(instance?.id ?? 'nothing', `l${i}`);
    }
    equal(taken, order.map((id) => `k-${id}`), 'with a concurrency limit');
  });

  add("claim() under a workflow's rate limit starts at most max per window, and opens the next window at the first claim after it ended", async (t) => {
    for (let i = 0; i < 7; i++) {
      await t.prioritized(`r${i}`, i);
    }
    await t.create('x', 0, 'invoice-batch');
    const rateLimits = [{ workflow: W.name, limit: { max: 3, duration: 1_000 }, perKey: null }];
    const claim = (now: number, o: { limit?: number } = {}) =>
      t.claimAndFinish(now, { rateLimits, workflows: [W, { name: 'invoice-batch', version: 1 }], ...o });

    equal(await claim(100, { limit: 2 }), ['r0', 'x'], 'two, of which one of the unlimited workflow');
    equal(await claim(200), ['r1', 'r2'], 'the rest of the window: three in all');
    equal(await claim(300), [], 'the window is full');
    equal(await claim(1_099), [], 'until it ends');
    equal(await claim(1_100), ['r3', 'r4', 'r5'], 'a new window opens at the first claim after it ended');
    equal(await claim(2_099), [], 'and ends a duration later');
    equal(await claim(2_100), ['r6'], 'the next one');
  });

  add('claim() under per-key rate limits keeps each key to its window, passes over a full key, and counts keyless instances only toward the workflow', async (t) => {
    await t.prioritized('a1', 0, { rateLimitKey: 'customer-a' });
    await t.prioritized('a2', 1, { rateLimitKey: 'customer-a' });
    await t.prioritized('b1', 2, { rateLimitKey: 'customer-b' });
    await t.prioritized('none', 3);
    await t.prioritized('a3', 4, { rateLimitKey: 'customer-a' });
    await t.prioritized('c1', 5, { rateLimitKey: 'customer-c' });
    expect(await t.store.get('a1'), { rateLimitKey: 'customer-a' }, 'the key, stored');
    const perKey = [{ workflow: W.name, limit: null, perKey: { max: 1, duration: 1_000 } }];

    equal(await t.claimAndFinish(10, { rateLimits: perKey, limit: 3 }), ['a1', 'b1', 'none'], 'one per key, and the keyless one');
    equal(await t.claimAndFinish(20, { rateLimits: perKey }), ['c1'], 'the next key with room');
    equal(await t.claimAndFinish(30, { rateLimits: perKey }), [], "customer-a's window is full");
    equal(await t.claimAndFinish(1_010, { rateLimits: perKey }), ['a2'], "customer-a's window reopens with its next instance");

    // Both: the workflow's window counts every key's claims.
    for (const [i, key] of ['d', 'd', 'e', 'f'].entries()) {
      await t.prioritized(`${key}${i}`, 100 + i, { rateLimitKey: `customer-${key}` });
    }
    const both = [{ workflow: W.name, limit: { max: 2, duration: 1_000 }, perKey: { max: 1, duration: 1_000 } }];
    equal(await t.claimAndFinish(5_000, { rateLimits: both }), ['a3', 'd0'], 'two in all, one per key');
    equal(await t.claimAndFinish(5_500, { rateLimits: both }), [], 'the workflow window is full');
    equal(await t.claimAndFinish(6_000, { rateLimits: both }), ['d1', 'e2'], 'the next window');
  });

  add('claim() applies rate limits and concurrency limits together, in stages', async (t) => {
    const keys: Array<[string, string, string]> = [
      ['i0', 'k0', 'r0'],
      ['i1', 'k0', 'r1'],
      ['i2', 'k1', 'r0'],
      ['i3', 'k1', 'r1'],
      ['i4', 'k2', 'r2'],
    ];
    for (const [n, [id, concurrencyKey, rateLimitKey]] of keys.entries()) {
      await t.store.create({ id, workflow: W.name, version: 1, input: null, deadline: null, now: n, concurrencyKey, rateLimitKey });
    }
    const limits = [{ workflow: W.name, limit: 3, perKey: 1 }];
    const rateLimits = [{ workflow: W.name, limit: { max: 4, duration: 1_000 }, perKey: { max: 1, duration: 1_000 } }];
    const claim = (now: number) => t.claim(now, { limits, rateLimits, leaseUntil: now + 10 }).then((c) => c.instances.map((i) => i.id));

    // k1 keeps i2, its first, which r0 then drops for i0: i3 would fit, but waits for the next claim.
    equal(await t.claimAndFinish(10, { limits, rateLimits }), ['i0', 'i4'], 'in stages');
    // i2 has no room at all (r0 is full): passed over. k0 keeps i1, k1 keeps i3, and r1 keeps i1, the first.
    equal(await t.claimAndFinish(20, { limits, rateLimits }), ['i1'], 'a full rate key passed over');
    equal(await t.claimAndFinish(30, { limits, rateLimits }), [], 'r0 and r1 are full');
    equal(await claim(1_010), ['i2'], 'in the next windows, k1 keeps i2');
    equal(await claim(1_015), [], "k1's slot is held by i2's live lease");
    equal(await claim(1_021), ['i3'], "i2's lease ended, but its r0 window is full; i3 takes k1's slot");
    equal(await t.claimAndFinish(1_030, { limits, rateLimits }), [], "i2 waits for r0's next window, at 2010");
  });

  add('claim() keeps a compensating instance compensating, and never claims a finished one', async (t) => {
    await t.create('a');
    await t.create('b');
    const { instances } = await t.claim(1, { token: 't1' });
    equal(instances.length, 2, 'claimed');

    await t.store.write('a', 't1', { now: 2, entries: [], status: 'compensating', error: { name: 'StepFailedError', message: 'no' } });
    expect(await t.store.get('a'), { status: 'compensating', error: { name: 'StepFailedError', message: 'no' }, updatedAt: 2, leaseOwner: 'w1' }, 'compensating, lease kept');
    await t.store.write('a', 't1', { now: 3, entries: [], release: { wakeAt: 4, waits: [], signalCursor: 0 } });
    expect((await t.claim(4, { token: 't2' })).instances[0], { id: 'a', status: 'compensating', runs: 2 }, 'claimed again, still compensating');

    await t.store.write('b', 't1', { now: 3, entries: [], status: 'completed', output: 1, error: null, release: { wakeAt: null, waits: [], signalCursor: 0 } });
    equal((await t.claim(FAR, { token: 't3' })).instances.map((i) => i.id), ['a'], "'b' finished: never claimed again");
  });

  add('renew() extends the lease and reads the cancel flags while the token is current, and returns null after', async (t) => {
    await t.create('a');
    await t.claim(1, { token: 't1' });

    equal(await t.store.renew('a', 't1', 5_000), { cancelRequested: false, terminateRequested: false }, 'renewed');
    expect(await t.store.get('a'), { leaseUntil: 5_000 }, 'the new lease');
    equal(await t.store.requestCancel('a', { reason: 'stop', now: 2, terminate: false }), true, 'cancel requested');
    equal(await t.store.renew('a', 't1', 6_000), { cancelRequested: true, terminateRequested: false }, 'renew reads the flag');
    equal(await t.store.requestCancel('a', { reason: 'now', now: 3, terminate: true }), true, 'terminate requested');
    equal(await t.store.renew('a', 't1', 6_000), { cancelRequested: true, terminateRequested: true }, 'renew reads both flags');
    equal(await t.store.renew('a', 'other', 7_000), null, 'another token');
    equal(await t.store.renew('missing', 't1', 7_000), null, 'an unknown id');
    expect(await t.store.get('a'), { leaseUntil: 6_000 }, 'unchanged by a stale renew');
  });

  add('write() changes nothing under a stale token', async (t) => {
    await t.create('a');
    await t.claim(1, { token: 'stale' });
    await t.claim(5_000, { owner: 'w2', token: 'current' }); // the first lease expired, w2 took over

    const writes: WorkflowWrite[] = [
      t.journalWrite([entry('x')]),
      { now: 1, entries: [], status: 'compensating', error: { name: 'Error', message: 'boom' } },
      { now: 1, entries: [entry('y')], status: 'suspended', release: { wakeAt: 9, waits: [{ signal: 's', key: null }], signalCursor: 0 } },
      { now: 1, entries: [], status: 'completed', output: 1, error: null, release: { wakeAt: null, waits: [], signalCursor: 0 } },
      { now: 1, entries: [entry('z')], release: { wakeAt: 1, waits: [], signalCursor: 0 } },
    ];
    for (const [i, write] of writes.entries()) {
      equal(await t.store.write('a', 'stale', write), false, `stale write #${i + 1}`);
    }

    equal(await t.store.write('missing', 'current', t.journalWrite([entry('x')])), false, 'a write to an unknown id');
    expect(await t.store.get('a', { journal: true }), { status: 'running', leaseOwner: 'w2', leaseUntil: 6_000, error: null, waits: [], journal: [] }, 'nothing written');
  });

  add('write() keeps journal entries in first-write order, replaces them whole, and keeps null apart from undefined', async (t) => {
    await t.create('a');
    await t.claim(1, { token: 't' });

    const progress = { cursor: 4, note: "it's 100% ü" };
    await t.store.write('a', 't', t.journalWrite([entry('one', { status: 'pending', attempts: 1, wakeAt: null, progress })]));
    await t.store.write('a', 't', t.journalWrite([
      entry('two', { kind: 'signal', status: 'pending', attempts: 0, wakeAt: 99, data: { signal: 's', key: null } }),
      entry('one', { status: 'failed', attempts: 2, error: { name: 'Error', message: 'x' }, wakeAt: 50 }),
      entry('three', { kind: 'now', result: 123 }),
    ]));
    await t.store.write('a', 't', t.journalWrite([
      entry('four', { result: null }),
      entry('two', { kind: 'signal', status: 'completed', attempts: 0, wakeAt: 99, result: { signalId: 7, payload: null }, data: { signal: 's', key: null } }),
    ]));

    const journal = (await t.store.get('a', { journal: true }))!.journal!;
    equal(journal.map((e) => e.name), ['one', 'two', 'three', 'four'], 'first-write order');
    expect(journal[0], { name: 'one', kind: 'step', status: 'failed', error: { name: 'Error', message: 'x' }, attempts: 2, wakeAt: 50, updatedAt: 1 }, "'one', replaced");
    absent(journal[0]!.progress, "'one'.progress, dropped by the replacement");
    expect(journal[1], { name: 'two', status: 'completed', result: { signalId: 7, payload: null }, data: { signal: 's', key: null } }, "'two'");
    equal(journal[3]!.result, null, 'a null result stays null');
    absent(journal[2]!.error, 'an undefined error stays undefined');
    equal((await t.store.get('a'))!.journal, undefined, 'no journal unless asked');
  });

  add('write() with release parks, finishes or hands back the instance', async (t) => {
    for (const id of ['parked', 'done', 'failed', 'back']) {
      await t.create(id);
    }
    await t.claim(1, { token: 't' });

    const waits = [{ signal: 'x', key: 'k' }, { signal: 'y', key: null }, { signal: 'a', key: 'z' }];
    equal(await t.store.write('parked', 't', { now: 2, entries: [entry('wait', { kind: 'signal', status: 'pending' })], status: 'suspended', release: { wakeAt: 500, waits, signalCursor: 0 } }), true, 'suspend');
    expect(await t.store.get('parked', { journal: true }), { status: 'suspended', wakeAt: 500, leaseUntil: null, leaseOwner: 'w1', updatedAt: 2, waits, journal: [{ name: 'wait' }] }, 'parked with its waits, in order');
    equal(await t.store.write('parked', 't', t.journalWrite([entry('late')])), false, 'the lease ended with the release');
    const [again] = (await t.claim(500, { token: 't2' })).instances;
    equal(again?.id, 'parked', 'due again at wakeAt');
    await t.store.write('parked', 't2', { now: 501, entries: [], status: 'suspended', release: { wakeAt: 900, waits: [{ signal: 'q', key: 'r' }], signalCursor: 0 } });
    equal((await t.store.get('parked'))!.waits, [{ signal: 'q', key: 'r' }], 'waits replaced');

    await t.store.write('done', 't', { now: 3, entries: [entry('last')], status: 'completed', output: { ok: [1, 'two'] }, error: null, release: { wakeAt: null, waits: [], signalCursor: 0 } });
    expect(await t.store.get('done'), { status: 'completed', output: { ok: [1, 'two'] }, error: null, wakeAt: null, leaseOwner: 'w1', leaseUntil: null, updatedAt: 3, waits: [] }, 'finished');
    await t.store.write('failed', 't', { now: 4, entries: [], status: 'failed', output: null, error: { name: 'WorkflowFailedError', message: 'no' }, release: { wakeAt: null, waits: [], signalCursor: 0 } });
    expect(await t.store.get('failed'), { status: 'failed', output: null, error: { name: 'WorkflowFailedError', message: 'no' } }, 'a null output stays null');

    await t.store.write('back', 't', { now: 5, entries: [entry('partial', { status: 'pending' })], release: { wakeAt: 5, waits: [], signalCursor: 0 } });
    expect(await t.store.get('back'), { status: 'running', wakeAt: 5, leaseUntil: null, error: null }, 'handed back: due, status unchanged');
    absent((await t.store.get('back'))!.output, 'output, never set', { orNull: true });
    equal((await t.claim(5, { owner: 'w2', token: 't3' })).instances.map((i) => i.id), ['back'], 'claimable at once');
  });

  add('write() sets the custom status under the lease, and leaves it as it is when not given', async (t) => {
    await t.create('a');
    await t.claim(1, { token: 't' });

    const status = { stage: 'packing', items: [{ sku: 'kibble-2kg', qty: 2 }], note: "it's 100% ü", none: null };
    equal(await t.store.write('a', 't', { now: 2, entries: [], customStatus: status }), true, 'set');
    expect(await t.store.get('a'), { customStatus: status, updatedAt: 2 }, 'the status');
    await t.store.write('a', 't', { now: 3, entries: [entry('x')], status: 'running' });
    expect(await t.store.get('a'), { customStatus: status }, 'a write without it leaves it');
    await t.store.write('a', 't', { now: 4, entries: [], customStatus: 'shipped' });
    expect((await t.store.list({ limit: 10, offset: 0 }))[0], { customStatus: 'shipped' }, 'a string, in list()');
    await t.store.write('a', 't', { now: 5, entries: [], customStatus: 0 });
    equal((await t.store.get('a'))!.customStatus, 0, 'a falsy value');
    await t.store.write('a', 't', { now: 6, entries: [], customStatus: null, release: { wakeAt: 50, waits: [], signalCursor: 0 } });
    equal((await t.store.get('a'))!.customStatus, null, 'cleared, with a release');

    equal(await t.store.write('a', 't', { now: 7, entries: [], customStatus: 'stale' }), false, 'after the release');
    equal((await t.store.get('a'))!.customStatus, null, 'not written under a stale token');
  });

  // ---------------------------------------------------------------- signals

  add('signal() matches keys exactly (null, empty and other keys all differ) and wakes suspended instances only', async (t) => {
    for (const id of ['none', 'empty', 'k1', 'running']) {
      await t.create(id);
    }
    await t.claim(1, { token: 't' });
    const keys: Record<string, string | null> = { none: null, empty: '', k1: 'k1' };
    for (const id of Object.keys(keys)) {
      await t.store.write(id, 't', { now: 2, entries: [], status: 'suspended', release: { wakeAt: null, waits: [{ signal: 'shipment.delivered', key: keys[id]! }], signalCursor: 0 } });
    }

    const s1 = await t.store.signal({ name: 'shipment.delivered', key: 'k2', dedupeId: null, payload: 'x', now: 3 });
    equal(s1.woken, 0, 'another key');
    const s2 = await t.store.signal({ name: 'other', key: null, dedupeId: null, payload: 'x', now: 3 });
    equal(s2.woken, 0, 'another name');
    const s3 = await t.store.signal({ name: 'shipment.delivered', key: '', dedupeId: null, payload: { e: '' }, now: 4 });
    equal(s3.woken, 1, "the '' key");
    expect(await t.store.get('empty'), { wakeAt: 4, updatedAt: 4 }, "woken: the '' wait");
    expect(await t.store.get('none'), { wakeAt: null }, 'not woken: the wait without a key');

    const s4 = await t.store.signal({ name: 'shipment.delivered', key: null, dedupeId: null, payload: null, now: 5 });
    equal(s4.woken, 1, 'no key');
    const s5 = await t.store.signal({ name: 'shipment.delivered', key: 'k1', dedupeId: null, payload: [1], now: 6 });
    const s6 = await t.store.signal({ name: 'shipment.delivered', key: 'k1', dedupeId: null, payload: [2], now: 7 });
    equal([s5.woken, s6.woken], [1, 0], 'an instance already due is not woken again');

    const ids = [s1, s2, s3, s4, s5, s6].map((s) => s.id);
    equal([...ids].sort((a, b) => a - b), ids, 'ids increase');
    equal(new Set(ids).size, 6, 'ids are unique');

    const read = (key: string | null, afterId = 0, upToId = s6.id) => t.store.signals({ name: 'shipment.delivered', key, afterId, upToId });
    equal(await read(null), [{ id: s4.id, name: 'shipment.delivered', key: null, payload: null, createdAt: 5 }], 'signals() without a key');
    equal(await read(''), [{ id: s3.id, name: 'shipment.delivered', key: '', payload: { e: '' }, createdAt: 4 }], "signals() with ''");
    equal((await read('k1')).map((s) => s.id), [s5.id, s6.id], 'signals() by id');
    equal((await read('k1', s5.id)).map((s) => s.id), [s6.id], 'afterId is exclusive');
    equal((await read('k1', 0, s5.id)).map((s) => s.id), [s5.id], 'upToId is inclusive');
    equal((await t.claim(0)).lastSignalId, s6.id, 'the last signal id');
    expect(await t.store.get('running'), { status: 'running' }, 'a running instance is left to write()');
  });

  add('write() keeps an instance due when a signal for its new waits arrived after its cursor', async (t) => {
    await t.create('a');
    await t.create('b');
    const { lastSignalId: cursor } = await t.claim(1, { token: 't' });
    await t.store.signal({ name: 'go', key: 'a', dedupeId: null, payload: 1, now: 2 }); // while a and b were executing

    const suspend = (id: string) =>
      t.store.write(id, 't', { now: 3, entries: [], status: 'suspended', release: { wakeAt: FAR, waits: [{ signal: 'go', key: id }], signalCursor: cursor } });
    equal(await suspend('a'), true, 'suspend a');
    equal(await suspend('b'), true, 'suspend b');
    expect(await t.store.get('a'), { status: 'suspended', wakeAt: 3, leaseUntil: null }, 'a saw the signal: due now');
    expect(await t.store.get('b'), { status: 'suspended', wakeAt: FAR, waits: [{ signal: 'go', key: 'b' }] }, 'b: another key, parked');

    const [c] = (await t.claim(3, { token: 't2' })).instances;
    equal(c?.id, 'a', 'a is claimable');
    await t.store.write('a', 't2', { now: 4, entries: [], status: 'suspended', release: { wakeAt: FAR, waits: [{ signal: 'go', key: 'a' }], signalCursor: (await t.claim(0)).lastSignalId } });
    expect(await t.store.get('a'), { wakeAt: FAR }, 'a signal at or below the cursor was seen: parked');
  });

  add('write() with a signal records it as signal() does, only when the write lands', async (t) => {
    await t.create('parent', 0);
    await t.create('child', 1);
    equal((await t.claim(1, { token: 'p', limit: 1 })).instances.map((i) => i.id), ['parent'], 'the parent, claimed');
    const parked = { wakeAt: FAR, waits: [{ signal: 'child.ended', key: 'child' }], signalCursor: 0 };
    equal(await t.store.write('parent', 'p', { now: 2, entries: [], status: 'suspended', release: parked }), true, 'the parent, parked');
    equal((await t.claim(3, { token: 'stale', leaseUntil: 4 })).instances.map((i) => i.id), ['child'], 'the child, claimed under a lease that ends at 4');
    equal((await t.claim(5, { token: 'c' })).instances.map((i) => i.id), ['child'], 'the child, claimed again once that lease ended');
    const ended = { name: 'child.ended', key: 'child', dedupeId: 'child', payload: { status: 'completed', output: { label: 'LBL-1' } }, now: 6 };
    const finish = (token: string, now: number) =>
      t.store.write('child', token, { now, entries: [], status: 'completed', output: 1, error: null, release: { wakeAt: null, waits: [], signalCursor: 0 }, signal: { ...ended, now } });

    equal(await finish('stale', 6), false, 'a stale write');
    equal((await t.claim(0)).lastSignalId, 0, 'no signal from the stale write');
    expect(await t.store.get('parent'), { wakeAt: FAR }, 'nothing woken');

    equal(await finish('c', 7), true, 'the lease holder ends the child');
    const { lastSignalId } = await t.claim(0);
    equal(
      await t.store.signals({ name: 'child.ended', key: 'child', afterId: 0, upToId: lastSignalId }),
      [{ id: lastSignalId, name: 'child.ended', key: 'child', payload: ended.payload, createdAt: 7 }],
      'the signal, recorded',
    );
    expect(await t.store.get('parent'), { status: 'suspended', wakeAt: 7, updatedAt: 7 }, 'the parent, woken');
    expect(await t.store.get('child'), { status: 'completed', output: 1 }, 'the child, ended');

    // Deduplicated as signal() does: a retried child that ends again stores nothing more.
    await t.store.signal({ name: 'other', key: null, dedupeId: null, payload: null, now: 8 });
    await t.claim(0, { token: 'again' }); // nothing is due; a claim of a finished child never happens
    expect(await t.store.signal({ ...ended, now: 9 }), { id: lastSignalId, created: false, woken: 0 }, 'the same dedupe id');
  });

  add('signal() with a dedupeId stores the signal once per name, and a repeat writes and wakes nothing', async (t) => {
    await t.create('a');
    await t.claim(1, { token: 't' });
    const waits = [{ signal: 'payment.captured', key: 'o1' }];
    await t.store.write('a', 't', { now: 1, entries: [], status: 'suspended', release: { wakeAt: FAR, waits, signalCursor: 0 } });

    const send = (name: string, key: string | null, dedupeId: string | null, payload: unknown, now: number) =>
      t.store.signal({ name, key, dedupeId, payload, now });
    const first = await send('payment.captured', 'o1', 'ch_1', { amount: 2499 }, 2);
    expect(first, { created: true, woken: 1, key: 'o1' }, 'the first signal');

    // Parked again on the same wait, having seen the first signal.
    await t.claim(2, { token: 't2' });
    await t.store.write('a', 't2', { now: 3, entries: [], status: 'suspended', release: { wakeAt: FAR, waits, signalCursor: first.id } });
    equal(await send('payment.captured', 'o1', 'ch_1', { amount: 1 }, 4), { id: first.id, woken: 0, created: false, key: 'o1' }, 'a repeat');
    expect(await t.store.get('a'), { status: 'suspended', wakeAt: FAR, updatedAt: 3 }, 'not woken by the repeat');
    equal(await send('payment.captured', 'o2', 'ch_1', 'x', 5), { id: first.id, woken: 0, created: false, key: 'o1' }, 'the same id with another key');

    const otherName = await send('refund.issued', 'o1', 'ch_1', 'y', 6);
    expect(otherName, { created: true, key: 'o1' }, 'the same id under another name');
    const plain = [await send('payment.captured', 'o1', null, 'p', 7), await send('payment.captured', 'o1', null, 'p', 8)];
    expect(plain, [{ created: true }, { created: true }], 'signals without a dedupeId');
    equal(new Set([first.id, otherName.id, ...plain.map((s) => s.id)]).size, 4, 'distinct ids');

    const read = (name: string, key: string) => t.store.signals({ name, key, afterId: 0, upToId: Number.MAX_SAFE_INTEGER });
    equal((await read('payment.captured', 'o1')).map((s) => s.payload), [{ amount: 2499 }, 'p', 'p'], 'the first payload, stored once');
    equal(await read('payment.captured', 'o2'), [], 'nothing under the other key');
    equal((await t.claim(0)).lastSignalId, plain[1]!.id, 'the last signal id');
  });

  // ---------------------------------------------------------------- retention

  add('purge() deletes old finished instances of the given statuses, oldest first, with their journals', async (t) => {
    const finish = async (id: string, status: 'completed' | 'failed' | 'cancelled' | 'compensation_failed', at: number) => {
      await t.create(id);
      await t.claim(0, { token: `t-${id}`, leaseUntil: FAR });
      await t.store.write(id, `t-${id}`, { now: at, entries: [entry('only')], status, error: null, release: { wakeAt: null, waits: [], signalCursor: 0 } });
    };
    await finish('done-2', 'completed', 200);
    await finish('done-1', 'completed', 100);
    await finish('failed', 'failed', 150);
    await finish('stuck', 'compensation_failed', 50);
    await finish('recent', 'completed', 1_000);
    await t.create('pending');
    await t.create('parked');
    await t.claim(0, { token: 'p', leaseUntil: FAR, limit: 10 });
    await t.store.write('parked', 'p', { now: 10, entries: [entry('wait', { kind: 'signal', status: 'pending' })], status: 'suspended', release: { wakeAt: null, waits: [{ signal: 's', key: 'k' }], signalCursor: 0 } });

    const statuses = ['completed', 'failed', 'cancelled'] as const;
    equal(await t.store.purge({ statuses: [...statuses], before: 500, limit: 2 }), { instances: 2, signals: 0, rateLimits: 0 }, 'the first batch');
    equal(await t.store.get('done-1'), null, 'the oldest, gone');
    equal(await t.store.get('failed'), null, 'the next oldest, gone');
    expect(await t.store.get('done-2', { journal: true }), { status: 'completed', journal: [{ name: 'only' }] }, 'past the limit: kept');

    equal(await t.store.purge({ statuses: [...statuses], before: 500, limit: 2 }), { instances: 1, signals: 0, rateLimits: 0 }, 'the next batch');
    equal(await t.store.purge({ statuses: [...statuses], before: 500, limit: 2 }), { instances: 0, signals: 0, rateLimits: 0 }, 'nothing left');
    const left = (await t.store.list({ limit: 100, offset: 0 })).map((i) => i.id).sort();
    equal(left, ['parked', 'pending', 'recent', 'stuck'], 'unfinished, too recent, and another status: kept');
    expect(await t.store.get('parked', { journal: true }), { waits: [{ signal: 's', key: 'k' }], journal: [{ name: 'wait' }] }, 'an unfinished instance, untouched');

    equal(await t.store.purge({ statuses: ['compensation_failed'], before: 500, limit: 10 }), { instances: 1, signals: 0, rateLimits: 0 }, 'compensation_failed, when asked for');
    // A purged id can be started again.
    expect(await t.create('done-1'), { created: true, instance: { status: 'pending' } }, 'the id, free again');
  });

  add('purge() deletes signals no instance can take any more, and never the newest', async (t) => {
    const send = (payload: number, now: number, dedupeId: string | null = null) => t.store.signal({ name: 's', key: 'k', dedupeId, payload, now });
    await send(1, 10, 'evt-1');
    const s2 = await send(2, 20);
    await t.create('waiting'); // takes signals above s2 only
    const s3 = await send(3, 30);
    const s4 = await send(4, 40);
    const all = () => t.store.signals({ name: 's', key: 'k', afterId: 0, upToId: FAR });

    equal(await t.store.purge({ statuses: ['completed'], before: 1_000, limit: 1 }), { instances: 0, signals: 1, rateLimits: 0 }, 'one at a time');
    equal((await all()).map((s) => s.id), [s2.id, s3.id, s4.id], 'the lowest id first');
    equal(await t.store.purge({ statuses: ['completed'], before: 1_000, limit: 10 }), { instances: 0, signals: 1, rateLimits: 0 }, 'up to the cursor');
    equal((await all()).map((s) => s.id), [s3.id, s4.id], "the signals an unfinished instance can still take: kept");

    await t.claim(0, { token: 't', leaseUntil: FAR });
    await t.store.write('waiting', 't', { now: 50, entries: [], status: 'completed', error: null, release: { wakeAt: null, waits: [], signalCursor: s4.id } });
    equal(await t.store.purge({ statuses: ['completed'], before: 35, limit: 10 }), { instances: 0, signals: 1, rateLimits: 0 }, 'only the old enough');
    equal((await all()).map((s) => s.id), [s4.id], 's3 is old enough, s4 too recent');
    equal(await t.store.purge({ statuses: ['completed'], before: 1_000, limit: 10 }), { instances: 1, signals: 0, rateLimits: 0 }, 'the newest stays');
    equal((await t.claim(0)).lastSignalId, s4.id, 'the last signal id');
    expect(await t.create('later'), { instance: { signalCursor: s4.id } }, "a new instance's cursor");
    expect(await send(5, 60), { created: true }, 'the next signal');
    expect(await send(1, 70, 'evt-1'), { created: true }, 'a dedupe id, stored again once its signal was purged');
  });

  add('purge() deletes the rate-limit windows that ended before `before`, oldest first, and keeps the open ones', async (t) => {
    for (const key of ['a', 'b', 'c']) {
      await t.prioritized(key, 0, { rateLimitKey: key });
    }
    const rateLimits = (duration: number) => [{ workflow: W.name, limit: null, perKey: { max: 1, duration } }];
    await t.claim(10, { rateLimits: rateLimits(100), limit: 1, leaseUntil: 11 }); // a: window ends at 110
    await t.claim(20, { rateLimits: rateLimits(50), limit: 1, leaseUntil: 21 }); // a is full; b: ends at 70
    await t.claim(30, { rateLimits: rateLimits(5_000), limit: 1, leaseUntil: 31 }); // c: ends at 5030

    equal(await t.store.purge({ statuses: ['completed'], before: 200, limit: 1 }), { instances: 0, signals: 0, rateLimits: 1 }, "b's, which ended first");
    equal(await t.store.purge({ statuses: ['completed'], before: 200, limit: 10 }), { instances: 0, signals: 0, rateLimits: 1 }, "a's");
    equal(await t.store.purge({ statuses: ['completed'], before: 200, limit: 10 }), { instances: 0, signals: 0, rateLimits: 0 }, "c's is open");
    equal((await t.claim(40, { rateLimits: rateLimits(5_000), leaseUntil: 41 })).instances.map((i) => i.id), ['a', 'b'], 'the purged windows, as good as new');
  });

  // ---------------------------------------------------------------- cancel

  add('requestCancel() accepts once, for pending, running and suspended instances only', async (t) => {
    for (const id of ['pending', 'running', 'suspended', 'compensating', 'done']) {
      await t.create(id, 0);
    }
    await t.create('pending-later', 50);
    await t.claim(1, { token: 't' });

    const park = { now: 1, entries: [], release: { wakeAt: FAR, waits: [], signalCursor: 0 } };
    await t.store.write('pending', 't', { ...park, status: 'pending' });
    await t.store.write('suspended', 't', { ...park, status: 'suspended' });
    await t.store.write('compensating', 't', { now: 1, entries: [], status: 'compensating', error: { name: 'E', message: 'x' } });
    await t.store.write('done', 't', { now: 1, entries: [], status: 'completed', error: null, release: { wakeAt: null, waits: [], signalCursor: 0 } });

    equal(await t.store.requestCancel('suspended', { reason: 'Changed my mind.', now: 10, terminate: false }), true, 'suspended');
    expect(await t.store.get('suspended'), { status: 'suspended', cancelRequested: true, cancelReason: 'Changed my mind.', wakeAt: 10, updatedAt: 10 }, 'due now');
    equal(await t.store.requestCancel('suspended', { reason: 'Again.', now: 11, terminate: false }), false, 'a repeated request');
    expect(await t.store.get('suspended'), { cancelReason: 'Changed my mind.', wakeAt: 10 }, 'the first reason stays');

    equal(await t.store.requestCancel('pending', { reason: null, now: 12, terminate: false }), true, 'pending');
    expect(await t.store.get('pending'), { cancelRequested: true, cancelReason: null, wakeAt: 12 }, 'pending, due now');
    equal(await t.store.requestCancel('pending-later', { reason: null, now: 12, terminate: false }), true, 'pending, not yet due');
    expect(await t.store.get('pending-later'), { wakeAt: 12 }, 'due now');

    equal(await t.store.requestCancel('running', { reason: 'r', now: 13, terminate: false }), true, 'running');
    expect(await t.store.get('running'), { cancelRequested: true, wakeAt: 0 }, 'an instance already due keeps its wakeAt');

    equal(await t.store.requestCancel('compensating', { reason: 'x', now: 14, terminate: false }), false, 'compensating');
    equal(await t.store.requestCancel('done', { reason: 'x', now: 15, terminate: false }), false, 'finished');
    equal(await t.store.requestCancel('missing', { reason: 'x', now: 16, terminate: false }), false, 'unknown');
    expect(await t.store.get('done'), { status: 'completed', cancelRequested: false }, 'finished: unchanged');
  });

  add('requestCancel() with terminate accepts once, also after a cancel and for a compensating instance', async (t) => {
    for (const id of ['suspended', 'cancelled-first', 'compensating', 'done']) {
      await t.create(id, 0);
    }
    await t.claim(1, { token: 't' });
    await t.store.write('suspended', 't', { now: 1, entries: [], status: 'suspended', release: { wakeAt: FAR, waits: [], signalCursor: 0 } });
    await t.store.write('compensating', 't', { now: 1, entries: [], status: 'compensating', error: { name: 'E', message: 'x' }, release: { wakeAt: FAR, waits: [], signalCursor: 0 } });
    await t.store.write('done', 't', { now: 1, entries: [], status: 'completed', error: null, release: { wakeAt: null, waits: [], signalCursor: 0 } });
    const terminate = (id: string, reason: string | null, now: number) => t.store.requestCancel(id, { reason, now, terminate: true });

    equal(await terminate('suspended', 'Stuck on a retired carrier.', 10), true, 'suspended');
    expect(
      await t.store.get('suspended'),
      { status: 'suspended', cancelRequested: true, terminateRequested: true, cancelReason: 'Stuck on a retired carrier.', wakeAt: 10, updatedAt: 10 },
      'terminate requested, due now',
    );
    equal(await terminate('suspended', 'Again.', 11), false, 'a repeated terminate');
    equal(await t.store.requestCancel('suspended', { reason: 'Later.', now: 12, terminate: false }), false, 'a cancel after it');
    expect(await t.store.get('suspended'), { cancelReason: 'Stuck on a retired carrier.' }, 'the reason stays');

    equal(await t.store.requestCancel('cancelled-first', { reason: 'Cancel.', now: 13, terminate: false }), true, 'a cancel first');
    expect(await t.store.get('cancelled-first'), { cancelRequested: true, terminateRequested: false }, 'cancelled, not terminated');
    equal(await terminate('cancelled-first', 'Terminate.', 14), true, 'a terminate after a cancel');
    expect(await t.store.get('cancelled-first'), { status: 'running', terminateRequested: true, cancelReason: 'Terminate.' }, 'its reason replaced');

    equal(await terminate('compensating', 'Stop undoing.', 15), true, 'compensating');
    expect(await t.store.get('compensating'), { status: 'compensating', cancelRequested: true, terminateRequested: true, wakeAt: 15 }, 'due now');
    equal(await terminate('done', 'x', 16), false, 'finished');
    equal(await terminate('missing', 'x', 17), false, 'unknown');
    expect(await t.store.get('done'), { status: 'completed', cancelRequested: false, terminateRequested: false }, 'finished: unchanged');
  });

  add('write() keeps a suspending instance with a pending cancel due, but not a compensating one', async (t) => {
    await t.create('a');
    await t.claim(1, { token: 't' });
    await t.store.requestCancel('a', { reason: null, now: 2, terminate: false });
    await t.store.write('a', 't', { now: 20, entries: [], status: 'suspended', release: { wakeAt: FAR, waits: [], signalCursor: 0 } });
    expect(await t.store.get('a'), { status: 'suspended', wakeAt: 20 }, 'suspended with a cancel: due now');

    await t.claim(20, { token: 't2' });
    await t.store.write('a', 't2', { now: 21, entries: [], status: 'compensating', error: { name: 'WorkflowCancelledError', message: 'Cancelled.' }, release: { wakeAt: 500, waits: [], signalCursor: 0 } });
    expect(await t.store.get('a'), { status: 'compensating', wakeAt: 500 }, 'compensating: its own wakeAt');
  });

  // ---------------------------------------------------------------- operator actions

  add('reopen() applies once, only while the instance is unleased and as the engine read it', async (t) => {
    await t.create('a', 0);
    await t.claim(1, { token: 't' });
    const failure = { name: 'StepFailedError', message: 'no' };
    await t.store.write('a', 't', { now: 2, entries: [entry('charge', { status: 'failed', attempts: 3, error: failure })], status: 'failed', error: failure, release: { wakeAt: null, waits: [], signalCursor: 0 } });

    const reopen = (expect: { status: 'failed' | 'completed'; runs: number }, now: number) =>
      t.store.reopen('a', {
        expect,
        status: 'pending',
        error: null,
        deadline: 9_000,
        entries: [entry('charge', { status: 'pending', attempts: 0, wakeAt: null, error: failure }), entry('$retry:1', { kind: 'retry', attempts: 0, data: { from: 'failed' } })],
        now,
      });
    equal(await reopen({ status: 'completed', runs: 1 }, 3), false, 'another status');
    equal(await reopen({ status: 'failed', runs: 2 }, 3), false, 'another runs count');
    expect(await t.store.get('a', { journal: true }), { status: 'failed', error: failure, updatedAt: 2, deadline: null, journal: [{ name: 'charge', status: 'failed' }] }, 'unchanged');

    equal(await reopen({ status: 'failed', runs: 1 }, 5), true, 'as read');
    expect(await t.store.get('a', { journal: true }), {
      status: 'pending',
      error: null,
      wakeAt: 5,
      updatedAt: 5,
      deadline: 9_000,
      journal: [{ name: 'charge', status: 'pending', attempts: 0 }, { name: '$retry:1', kind: 'retry', data: { from: 'failed' } }],
    }, 'reopened');
    equal(await reopen({ status: 'failed', runs: 1 }, 6), false, 'a second retry of the same read');

    const [claimed] = (await t.claim(5, { token: 't2' })).instances;
    expect(claimed, { id: 'a', status: 'running', runs: 2 }, 'claimable');
    await t.store.write('a', 't2', { now: 7, entries: [], status: 'compensation_failed', error: failure });
    equal(await t.store.reopen('a', { expect: { status: 'compensation_failed', runs: 2 }, status: 'compensating', error: failure, entries: [], now: 8 }), false, 'leased');
    await t.store.write('a', 't2', { now: 9, entries: [], release: { wakeAt: null, waits: [], signalCursor: 0 } });
    equal(await t.store.reopen('a', { expect: { status: 'compensation_failed', runs: 2 }, status: 'compensating', error: failure, entries: [], now: 10 }), true, 'unleased');
    expect(await t.store.get('a'), { status: 'compensating', wakeAt: 10, deadline: 9_000 }, 'the deadline, left as it was');
  });

  add('delete() removes an instance with its journal and waits, only in the given statuses', async (t) => {
    await t.create('done');
    await t.create('parked');
    await t.claim(1, { token: 't' });
    await t.store.write('done', 't', { now: 2, entries: [entry('x')], status: 'completed', error: null, release: { wakeAt: null, waits: [], signalCursor: 0 } });
    await t.store.write('parked', 't', { now: 2, entries: [entry('w', { kind: 'signal', status: 'pending' })], status: 'suspended', release: { wakeAt: null, waits: [{ signal: 's', key: 'k' }], signalCursor: 0 } });

    equal(await t.store.delete('parked', ['completed', 'failed']), false, 'another status');
    equal(await t.store.delete('done', ['completed', 'failed']), true, 'deleted');
    equal(await t.store.get('done'), null, 'gone');
    equal(await t.store.delete('done', ['completed']), false, 'already gone');
    equal(await t.store.delete('missing', ['completed']), false, 'unknown');

    await t.claim(3, { token: 't2' }); // not due: nothing claimed
    equal(await t.store.delete('parked', ['suspended']), true, 'an unfinished one, when asked');
    equal(await t.store.list({ limit: 10, offset: 0 }), [], 'none left');
    expect(await t.create('done'), { created: true }, 'the id, free again');
    expect(await t.store.signal({ name: 's', key: 'k', dedupeId: null, payload: 1, now: 4 }), { woken: 0 }, "the deleted instance's waits are gone");

    await t.claim(5, { token: 't3' });
    equal(await t.store.delete('done', ['running']), true, 'a leased one');
    equal(await t.store.write('done', 't3', t.journalWrite([entry('late')])), false, "its worker's next write");
    equal(await t.store.renew('done', 't3', 100), null, 'and renewal');
  });

  // ---------------------------------------------------------------- schedules (the core's ScheduleStore contract)

  scheduleCases((name, run) => add(name, (t) => run(workflowScheduleStore(() => t.store))), 'base');

  // ---------------------------------------------------------------- the application's transaction

  if (transaction) {
    add("createInTransaction() and signalInTransaction() commit and roll back with the application's transaction", async (t) => {
      const { createInTransaction, signalInTransaction } = requireTransactionMethods(t.store);
      await rejects(
        transaction(async (tx) => {
          expect(await createInTransaction(tx, { id: 'rolled-back', workflow: 'order-fulfilment', version: 1, input: 1, deadline: null, now: 1 }), { created: true }, 'created in the transaction');
          await signalInTransaction(tx, { name: 'go', key: 'x', dedupeId: null, payload: 1, now: 1 });
          throw new Error('payment declined');
        }),
        'payment declined',
      );
      equal(await t.store.get('rolled-back'), null, 'rolled back: no instance');
      equal((await t.claim(0)).lastSignalId, 0, 'rolled back: no signal');

      await t.create('waiting');
      await t.claim(1, { token: 't' });
      await t.store.write('waiting', 't', { now: 1, entries: [], status: 'suspended', release: { wakeAt: null, waits: [{ signal: 'go', key: 'w' }], signalCursor: 0 } });
      const results = await transaction(async (tx) => [
        await createInTransaction(tx, { id: 'committed', workflow: 'order-fulfilment', version: 1, input: { a: 1 }, deadline: 5_000, now: 2 }),
        await createInTransaction(tx, { id: 'committed', workflow: 'order-fulfilment', version: 1, input: { a: 2 }, deadline: null, now: 3 }),
        await signalInTransaction(tx, { name: 'go', key: 'w', dedupeId: null, payload: 'p', now: 4 }),
      ] as const);

      expect(results[0], { created: true, instance: { id: 'committed', status: 'pending', input: { a: 1 }, signalCursor: 0 } }, 'created');
      expect(results[1], { created: false, instance: { id: 'committed', input: { a: 1 } } }, 'the row written earlier in the transaction');
      equal(results[2].woken, 1, 'woken');
      expect(await t.store.get('committed'), { status: 'pending', wakeAt: 2 }, 'committed');
      expect(await t.store.get('waiting'), { wakeAt: 4 }, 'woken with the commit');
      expect(await t.store.signals({ name: 'go', key: 'w', afterId: 0, upToId: results[2].id }), [{ id: results[2].id, payload: 'p' }], 'the signal');
    });
  }

  if (transaction) {
    add('signalInTransaction() stores a dedupeId once per commit, and a rolled-back signal leaves its id free', async (t) => {
      const { signalInTransaction } = requireTransactionMethods(t.store);
      const data = (payload: unknown, now: number) => ({ name: 'payment.captured', key: 'o1', dedupeId: 'ch_1', payload, now });
      await rejects(
        transaction(async (tx) => {
          expect(await signalInTransaction(tx, data('rolled back', 1)), { created: true }, 'stored in the transaction');
          throw new Error('payment declined');
        }),
        'payment declined',
      );

      const [first, again] = await transaction(async (tx) => [await signalInTransaction(tx, data('committed', 2)), await signalInTransaction(tx, data('again', 3))] as const);
      expect(first, { created: true }, 'the id is free after the rollback');
      equal(again, { id: first.id, woken: 0, created: false, key: 'o1' }, 'a repeat in the same transaction');
      expect(await t.store.signal(data('outside', 4)), { id: first.id, created: false }, 'a repeat after the commit');
      equal(
        (await t.store.signals({ name: 'payment.captured', key: 'o1', afterId: 0, upToId: Number.MAX_SAFE_INTEGER })).map((s) => s.payload),
        ['committed'],
        'one signal',
      );
    });
  }

  // ---------------------------------------------------------------- concurrency

  if (options.concurrent) {
    add('concurrent claims never return the same instance twice', async (t) => {
      for (let i = 0; i < 60; i++) {
        await t.create(`i${String(i).padStart(2, '0')}`, i);
      }

      const owners = new Map<string, string>();
      const claimer = async (owner: string) => {
        for (;;) {
          const { instances } = await t.claim(1_000, { owner, token: randomUUID(), limit: 7, leaseUntil: FAR });
          if (instances.length === 0) {
            return;
          }

          for (const instance of instances) {
            if (owners.has(instance.id)) {
              throw new Error(`${instance.id} was claimed by ${owners.get(instance.id)} and ${owner}`);
            }
            owners.set(instance.id, owner);
          }
          await jitter();
        }
      };

      await Promise.all(['c1', 'c2', 'c3', 'c4', 'c5'].map(claimer));
      equal(owners.size, 60, 'every instance claimed');
      const all = await t.store.list({ limit: 100, offset: 0 });
      equal(all.filter((i) => i.runs !== 1 || i.leaseOwner !== owners.get(i.id)).map((i) => i.id), [], 'each claimed once, by its owner');
    });

    add('concurrent claims never lease more than a limit allows, and fill every free slot', async (t) => {
      for (let i = 0; i < 60; i++) {
        await t.keyed(`k${i % 6}-${String(i).padStart(2, '0')}`, `k${i % 6}`, i);
      }

      const limits = [{ workflow: W.name, limit: 4, perKey: 1 }];
      const tokens = new Map<string, string>();
      for (let round = 0; round < 5; round++) {
        const now = 1_000 + round;
        await Promise.all(
          ['c1', 'c2', 'c3', 'c4', 'c5', 'c6'].map(async (owner) => {
            await jitter();
            const token = randomUUID();
            for (const instance of (await t.claim(now, { owner, token, limit: 3, leaseUntil: FAR, limits })).instances) {
              tokens.set(instance.id, token);
            }
          }),
        );

        const leased = (await t.store.list({ limit: 100, offset: 0 })).filter((i) => i.leaseUntil !== null && i.leaseUntil >= now);
        equal(leased.length, 4, `round ${round}: every slot filled, none more`);
        equal(new Set(leased.map((i) => i.concurrencyKey)).size, 4, `round ${round}: one per key`);
        await Promise.all(leased.map((i) => jitter().then(() => t.store.write(i.id, tokens.get(i.id)!, { now, entries: [], status: 'completed', error: null, release: { wakeAt: null, waits: [], signalCursor: 0 } }))));
      }
    });

    add('concurrent claims never start more than a rate limit allows in a window, and fill its room', async (t) => {
      for (let i = 0; i < 90; i++) {
        await t.prioritized(`r${String(i).padStart(2, '0')}`, i, { rateLimitKey: `k${i % 3}` });
      }

      // Per key: 2 per window of 1000; overall: 5. Each round is a new window. Concurrent claims of one workflow
      // may leave room (a claim passes over the instances another is taking): claims after them take the rest.
      const rateLimits = [{ workflow: W.name, limit: { max: 5, duration: 1_000 }, perKey: { max: 2, duration: 1_000 } }];
      const started = new Set<string>();
      for (let round = 0; round < 5; round++) {
        const now = 10_000 * (round + 1);
        const claim = async (owner: string) => {
          const token = randomUUID();
          return (await t.claim(now, { owner, token, limit: 2, leaseUntil: now + 1, rateLimits })).instances.map((instance) => ({ instance, token }));
        };
        const claimed = (await Promise.all(['c1', 'c2', 'c3', 'c4', 'c5', 'c6'].map((owner) => jitter().then(() => claim(owner))))).flat();
        for (let more = await claim('c7'); more.length > 0; more = await claim('c7')) {
          claimed.push(...more);
        }

        equal(claimed.length, 5, `round ${round}: the window's room filled, no more`);
        const perKey = new Map<string, number>();
        for (const { instance } of claimed) {
          perKey.set(instance.rateLimitKey!, (perKey.get(instance.rateLimitKey!) ?? 0) + 1);
          if (started.has(instance.id)) {
            throw new Error(`${instance.id} was claimed twice`);
          }
          started.add(instance.id);
        }
        equal([...perKey.values()].every((n) => n <= 2), true, `round ${round}: at most 2 per key`);
        await Promise.all(
          claimed.map(({ instance, token }) =>
            t.store.write(instance.id, token, { now, entries: [], status: 'completed', error: null, release: { wakeAt: null, waits: [], signalCursor: 0 } }),
          ),
        );
      }
    });

    scheduleCases((name, run) => add(name, (t) => run(workflowScheduleStore(() => t.store))), 'concurrent');

    add('purge() racing claims that reuse ended rate-limit windows neither deadlocks nor lets a window overflow', async (t) => {
      for (let i = 0; i < 40; i++) {
        await t.prioritized(`w${String(i).padStart(2, '0')}`, i, { rateLimitKey: `k${i % 8}` });
      }

      // Every round's windows (500ms) have ended by the next round, when purges delete them (in the order they ended,
      // not by key, as the claimers open them a millisecond apart) while claims reuse them (by key).
      const rateLimits = [{ workflow: W.name, limit: null, perKey: { max: 1, duration: 500 } }];
      for (let round = 0; round < 5; round++) {
        const now = 1_000 * (round + 1);
        const claims = ['c1', 'c2', 'c3', 'c4'].map(async (owner, i) => {
          const token = randomUUID();
          await jitter();
          return (await t.claim(now + i, { owner, token, limit: 3, leaseUntil: now + 10, rateLimits })).instances.map((instance) => ({ instance, token }));
        });
        const purges = [1, 2].map(async () => {
          await jitter();
          return t.store.purge({ statuses: ['completed'], before: now, limit: 2 });
        });
        const claimed = (await Promise.all([...claims, ...purges.map((purge) => purge.then(() => []))])).flat();

        const keys = claimed.map(({ instance }) => instance.rateLimitKey);
        equal(new Set(keys).size, keys.length, `round ${round}: at most one per key's window`);
        await Promise.all(
          claimed.map(({ instance, token }) =>
            t.store.write(instance.id, token, { now, entries: [], status: 'completed', error: null, release: { wakeAt: null, waits: [], signalCursor: 0 } }),
          ),
        );
      }
    });

    add('a signal racing a suspension never loses the wake-up', async (t) => {
      const n = 60;
      for (let i = 0; i < n; i++) {
        await t.create(`r${i}`);
      }

      const { instances } = await t.claim(1, { limit: n, leaseUntil: FAR, token: 't' });
      equal(instances.length, n, 'claimed');

      await Promise.all(
        instances.map(async (instance, i) => {
          const { lastSignalId: cursor } = await t.claim(0); // what the execution saw when it started
          const suspend = async () => {
            await jitter();
            const ok = await t.store.write(instance.id, 't', {
              now: 5,
              entries: [entry('wait', { kind: 'signal', status: 'pending', attempts: 0, wakeAt: FAR })],
              status: 'suspended',
              release: { wakeAt: FAR, waits: [{ signal: 'go', key: instance.id }], signalCursor: cursor },
            });
            if (!ok) {
              throw new Error(`suspending ${instance.id} was refused`);
            }
          };

          const signal = async () => {
            await jitter();
            const data = { name: 'go', key: instance.id, dedupeId: null, payload: i, now: 6 };
            if (i % 2 === 0 || !transaction || !t.store.signalInTransaction) {
              return t.store.signal(data);
            }

            // In the application's transaction, which holds the lock a little longer.
            return transaction(async (tx) => {
              const result = await t.store.signalInTransaction!(tx, data);
              await jitter();
              return result;
            });
          };

          await Promise.all([suspend(), signal()]);
        }),
      );

      const parked = (await t.store.list({ limit: n, offset: 0 })).filter((i) => i.wakeAt === FAR).map((i) => i.id);
      equal(parked, [], 'every instance was woken by its signal (wakeAt 6) or saw it while suspending (5)');
    });

    add("a child's ending write racing its parent's suspension never loses the wake-up", async (t) => {
      const n = 30;
      for (let i = 0; i < n; i++) {
        await t.create(`parent-${i}`);
        await t.create(`child-${i}`);
      }

      const { instances } = await t.claim(1, { limit: 2 * n, leaseUntil: FAR, token: 't' });
      equal(instances.length, 2 * n, 'claimed');
      await Promise.all(
        Array.from({ length: n }, async (_, i) => {
          const { lastSignalId: cursor } = await t.claim(0);
          const suspend = async () => {
            await jitter();
            const release = { wakeAt: FAR, waits: [{ signal: 'child.ended', key: `child-${i}` }], signalCursor: cursor };
            if (!(await t.store.write(`parent-${i}`, 't', { now: 5, entries: [], status: 'suspended', release }))) {
              throw new Error(`suspending parent-${i} was refused`);
            }
          };
          const end = async () => {
            await jitter();
            const signal = { name: 'child.ended', key: `child-${i}`, dedupeId: `child-${i}`, payload: i, now: 6 };
            const release = { wakeAt: null, waits: [], signalCursor: cursor };
            if (!(await t.store.write(`child-${i}`, 't', { now: 6, entries: [], status: 'completed', output: i, error: null, release, signal }))) {
              throw new Error(`ending child-${i} was refused`);
            }
          };
          await Promise.all([suspend(), end()]);
        }),
      );

      const parked = (await t.store.list({ limit: 2 * n, offset: 0 })).filter((i) => i.wakeAt === FAR).map((i) => i.id);
      equal(parked, [], "every parent was woken by its child's end (wakeAt 6) or saw it while suspending (5)");
    });

    add('signal ids become visible in id order: nothing below a cursor shows up later', async (t) => {
      const probes: Array<{ cursor: number; seen: number[] }> = [];
      let sending = true;
      const probe = async () => {
        while (sending) {
          const { lastSignalId: cursor } = await t.claim(0);
          const seen = (await t.store.signals({ name: 's', key: null, afterId: 0, upToId: cursor })).map((s) => s.id);
          probes.push({ cursor, seen });
          await jitter();
        }
      };

      const send = async (i: number) => {
        await jitter();
        if (i % 3 !== 0 || !transaction || !t.store.signalInTransaction) {
          return t.store.signal({ name: 's', key: null, dedupeId: null, payload: i, now: 1 });
        }
        return transaction(async (tx) => {
          const result = await t.store.signalInTransaction!(tx, { name: 's', key: null, dedupeId: null, payload: i, now: 1 });
          await jitter();
          return result;
        });
      };

      const probing = [probe(), probe()];
      const sent = await Promise.all(Array.from({ length: 60 }, (_, i) => send(i)));
      sending = false;
      await Promise.all(probing);

      equal(new Set(sent.map((s) => s.id)).size, 60, 'unique ids');
      const all = (await t.store.signals({ name: 's', key: null, afterId: 0, upToId: Number.MAX_SAFE_INTEGER })).map((s) => s.id);
      for (const { cursor, seen } of probes) {
        equal(all.filter((id) => id <= cursor), seen, `the signals at or below cursor ${cursor}`);
      }
    });

    add('concurrent signals with one dedupeId store it once, and every call returns its id', async (t) => {
      const send = async (i: number) => {
        await jitter();
        const data = { name: 'payment.captured', key: 'o1', dedupeId: 'ch_1', payload: i, now: i };
        if (i % 2 === 0 || !transaction || !t.store.signalInTransaction) {
          return t.store.signal(data);
        }

        return transaction(async (tx) => {
          const result = await t.store.signalInTransaction!(tx, data);
          await jitter();
          return result;
        });
      };

      const results = await Promise.all(Array.from({ length: 12 }, (_, i) => send(i)));
      equal(results.filter((r) => r.created).length, 1, 'created once');
      equal(new Set(results.map((r) => r.id)).size, 1, 'one id for every call');
      const stored = await t.store.signals({ name: 'payment.captured', key: 'o1', afterId: 0, upToId: Number.MAX_SAFE_INTEGER });
      equal(stored.map((s) => s.id), [results[0]!.id], 'one signal stored');
    });

    add('a cancel racing a suspension leaves the instance due', async (t) => {
      const n = 40;
      for (let i = 0; i < n; i++) {
        await t.create(`c${i}`);
      }

      const { instances } = await t.claim(1, { limit: n, leaseUntil: FAR, token: 't' });
      await Promise.all(
        instances.map(async (instance) => {
          await Promise.all([
            jitter().then(() => t.store.requestCancel(instance.id, { reason: 'stop', now: 3, terminate: false })),
            jitter().then(() =>
              t.store.write(instance.id, 't', { now: 4, entries: [], status: 'suspended', release: { wakeAt: FAR, waits: [], signalCursor: 0 } }),
            ),
          ]);
        }),
      );

      const all = await t.store.list({ limit: n, offset: 0 });
      equal(all.filter((i) => !i.cancelRequested || i.wakeAt === FAR).map((i) => i.id), [], 'cancelled and due (wakeAt 3 or 4)');
    });

    add('concurrent create(), requestCancel() and terminates of one id: one wins', async (t) => {
      const created = await Promise.all(Array.from({ length: 8 }, (_, i) => jitter().then(() => t.store.create({ id: 'same', workflow: 'w', version: 1, input: i, deadline: null, now: i }))));
      equal(created.filter((r) => r.created).length, 1, 'created once');
      const winner = created.find((r) => r.created)!.instance;
      equal(created.filter((r) => !isDeepStrictEqual(r.instance.input, winner.input)).length, 0, 'every call returns the stored instance');

      const accepted = await Promise.all(Array.from({ length: 8 }, (_, i) => jitter().then(() => t.store.requestCancel('same', { reason: `r${i}`, now: 10, terminate: false }))));
      equal(accepted.filter(Boolean).length, 1, 'one cancel accepted');
      const terminated = await Promise.all(Array.from({ length: 8 }, (_, i) => jitter().then(() => t.store.requestCancel('same', { reason: `t${i}`, now: 11, terminate: true }))));
      equal(terminated.filter(Boolean).length, 1, 'one terminate accepted');
    });

    add('purge() racing finishing, starting and signalling instances deletes only what was finished and unreachable', async (t) => {
      const n = 30;
      for (let i = 0; i < n; i++) {
        await t.create(`p${i}`);
      }
      await t.claim(1, { limit: n, token: 't', leaseUntil: FAR });
      for (let i = 0; i < 10; i++) {
        await t.store.signal({ name: 'old', key: null, dedupeId: null, payload: i, now: 0 });
      }

      let purging = true;
      const purger = async () => {
        while (purging) {
          await t.store.purge({ statuses: ['completed'], before: 1_000, limit: 3 });
          await jitter();
        }
      };
      const work = Array.from({ length: n }, async (_, i) => {
        await jitter();
        if (i % 2 === 0) {
          await t.store.write(`p${i}`, 't', { now: 10, entries: [], status: 'completed', error: null, release: { wakeAt: null, waits: [], signalCursor: 0 } });
        } else {
          await t.store.create({ id: `new${i}`, workflow: W.name, version: W.version, input: null, deadline: null, now: 2_000 });
          await t.store.signal({ name: 'new', key: null, dedupeId: null, payload: i, now: 2_000 });
        }
      });

      const purgers = [purger(), purger()];
      await Promise.all(work);
      purging = false;
      await Promise.all(purgers);
      while ((await t.store.purge({ statuses: ['completed'], before: 1_000, limit: 3 })).instances > 0) {
        // the rest of the finished ones
      }

      const left = (await t.store.list({ limit: 100, offset: 0 })).map((i) => i.id);
      equal(left.filter((id) => id.startsWith('p')).length, n / 2, 'every unfinished instance kept, every finished one purged');
      equal(left.filter((id) => id.startsWith('new')).length, n / 2, 'every new instance kept');
      equal((await t.store.signals({ name: 'new', key: null, afterId: 0, upToId: FAR })).length, n / 2, 'every recent signal kept');
    });

    add('concurrent reopens of one read accept one, and a purge racing them never deletes a reopened instance', async (t) => {
      const n = 20;
      for (let i = 0; i < n; i++) {
        await t.create(`r${i}`);
      }
      await t.claim(1, { limit: n, token: 't', leaseUntil: FAR });
      for (let i = 0; i < n; i++) {
        await t.store.write(`r${i}`, 't', { now: 2, entries: [], status: 'failed', error: null, release: { wakeAt: null, waits: [], signalCursor: 0 } });
      }

      const reopen = (id: string) => jitter().then(() => t.store.reopen(id, { expect: { status: 'failed', runs: 1 }, status: 'pending', error: null, entries: [], now: 3 }));
      const results = await Promise.all(
        Array.from({ length: n }, async (_, i) => {
          const [accepted] = await Promise.all([
            Promise.all([reopen(`r${i}`), reopen(`r${i}`)]),
            jitter().then(() => t.store.purge({ statuses: ['failed'], before: 100, limit: 1 })),
          ]);
          return accepted;
        }),
      );

      for (const [i, [first, second]] of results.entries()) {
        const instance = await t.store.get(`r${i}`);
        if (first && second) {
          throw new Error(`r${i} was reopened twice`);
        }
        if ((first || second) !== (instance?.status === 'pending')) {
          throw new Error(`r${i}: reopened ${first || second}, but ${instance ? `is ${instance.status}` : 'was purged'}`);
        }
      }
    });

    add("a stale lease holder's writes never land, however they interleave with the new holder's", async (t) => {
      const n = 20;
      for (let i = 0; i < n; i++) {
        await t.create(`f${i}`);
      }
      await t.claim(1, { limit: n, token: 'old', leaseUntil: 10 });
      await t.claim(11, { limit: n, token: 'new', owner: 'w2', leaseUntil: FAR });

      const results = await Promise.all(
        Array.from({ length: n }, (_, i) => `f${i}`).flatMap((id) => [
          jitter().then(() => t.store.write(id, 'old', { now: 12, entries: [entry('stale')], status: 'compensating', error: { name: 'E', message: 'stale' } })),
          jitter().then(() => t.store.write(id, 'new', t.journalWrite([entry('fresh')]))),
          jitter().then(() => t.store.write(id, 'old', { now: 12, entries: [], status: 'failed', release: { wakeAt: null, waits: [], signalCursor: 0 } })),
        ]),
      );

      equal(results.filter((ok, i) => ok !== (i % 3 === 1)).length, 0, 'only the new holder writes');
      for (let i = 0; i < n; i++) {
        expect(await t.store.get(`f${i}`, { journal: true }), { status: 'running', leaseUntil: FAR, error: null, journal: [{ name: 'fresh' }] }, `f${i}`);
      }
    });
  }

  return cases;
}

// ---------------------------------------------------------------- helpers

const FAR = 9_000_000_000_000; // a deadline nobody reaches in a test
const W = { name: 'order-fulfilment', version: 1 };

class Harness {
  constructor(readonly store: WorkflowStore) {}

  create(id: string, now = 0, workflow = 'order-fulfilment', version = 1) {
    return this.store.create({ id, workflow, version, input: { id }, deadline: null, now });
  }

  claim(
    now: number,
    o: {
      owner?: string;
      token?: string;
      limit?: number;
      leaseUntil?: number;
      workflows?: Array<{ name: string; version: number }>;
      limits?: WorkflowConcurrencyLimit[];
      rateLimits?: WorkflowRateLimitRule[];
    } = {},
  ): Promise<WorkflowClaim> {
    return this.store.claim({
      owner: o.owner ?? 'w1',
      token: o.token ?? randomUUID(),
      now,
      leaseUntil: o.leaseUntil ?? now + 1_000,
      limit: o.limit ?? 100,
      workflows: o.workflows ?? [W],
      ...(o.limits ? { limits: o.limits } : {}),
      ...(o.rateLimits ? { rateLimits: o.rateLimits } : {}),
    });
  }

  /** Claims at `now` and finishes what it claimed (so nothing comes back when its lease ends); returns the ids. */
  async claimAndFinish(now: number, o: Parameters<Harness['claim']>[1] = {}): Promise<string[]> {
    const token = randomUUID();
    const { instances } = await this.claim(now, { ...o, token });
    for (const instance of instances) {
      await this.store.write(instance.id, token, { now, entries: [], status: 'completed', error: null, release: { wakeAt: null, waits: [], signalCursor: 0 } });
    }
    return instances.map((instance) => instance.id);
  }

  /** An instance of `W` with a priority and a rate-limit key. */
  prioritized(id: string, now: number, o: { priority?: number; rateLimitKey?: string | null } = {}) {
    return this.store.create({ id, workflow: W.name, version: 1, input: { id }, deadline: null, now, ...o });
  }

  /** An instance of `W` with a concurrency key. */
  keyed(id: string, key: string | null, now: number, workflow = W.name) {
    return this.store.create({ id, workflow, version: 1, input: { id }, deadline: null, concurrencyKey: key, now });
  }

  /** Ends a lease: the instance is parked, and holds no slot. */
  release(id: string, token: string) {
    return this.store.write(id, token, { now: 1, entries: [], status: 'suspended', release: { wakeAt: FAR, waits: [], signalCursor: 0 } });
  }

  journalWrite(entries: WorkflowJournalEntry[]): WorkflowWrite {
    return { now: 1, entries };
  }
}

function entry(name: string, extra: Partial<WorkflowJournalEntry> = {}): WorkflowJournalEntry {
  return { name, kind: 'step', status: 'completed', attempts: 1, updatedAt: 1, ...extra };
}

function requireTransactionMethods(store: WorkflowStore) {
  const { createInTransaction, signalInTransaction } = store;
  if (typeof createInTransaction !== 'function' || typeof signalInTransaction !== 'function') {
    throw new Error('options.transaction is set, but the store lacks createInTransaction() or signalInTransaction().');
  }
  return { createInTransaction: createInTransaction.bind(store), signalInTransaction: signalInTransaction.bind(store) };
}
