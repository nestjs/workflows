import { Inject, Injectable, Logger } from '@nestjs/common';
import {
  ManualWorkflowClock,
  NonRetryableStepError,
  StepFailedError,
  Workflow,
  type WorkflowContext,
} from '../lib/index.js';
import { boot, deferred, forever, tempDb, type TestDb, World, type Node } from './support.js';

/** Test switchboard shared by both "processes": where to hang, and a signal when reached. */
@Injectable()
class Gate {
  hangAt: string | null = null;
  reached = deferred();
  /** Steps that honour the abort signal (graceful shutdown) instead of hanging. */
  abortable = false;
  /** A compensation that fails permanently. */
  failAt: string | null = null;
  /** A compensation whose first attempt fails. */
  flakyAt: string | null = null;
  readonly seen: unknown[] = [];

  async maybeHang(step: string, signal: AbortSignal) {
    if (this.hangAt !== step) {
      return;
    }

    this.reached.resolve();
    if (!this.abortable) {
      return forever();
    }
    await new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason), { once: true }));
  }
}

let db: TestDb;
let clock: ManualWorkflowClock;
let world: World;
let gate: Gate;
const nodes: Node[] = [];

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock();
  world = new World();
  gate = new Gate();
});

afterEach(async () => {
  for (const node of nodes.splice(0)) {
    await node.close();
  }
  db.cleanup();
});

async function start(workflows: any[]) {
  const node = await boot({
    db,
    clock,
    workflows,
    providers: [
      { provide: World, useValue: world },
      { provide: Gate, useValue: gate },
    ],
  });

  nodes.push(node);
  return node;
}

/** Simulates a crash: the process stops mid-step; its lease is left to expire. */
async function crash(node: Node) {
  await node.close();
  nodes.splice(nodes.indexOf(node), 1);
}

describe('crash recovery', () => {
  @Workflow('four-steps')
  class FourSteps {
    constructor(
      @Inject(World) private readonly world: World,
      @Inject(Gate) private readonly gate: Gate,
    ) {}

    async run(ctx: WorkflowContext) {
      const results: string[] = [];
      for (const name of ['one', 'two', 'three', 'four']) {
        results.push(
          await ctx.step(name, async (s) => {
            this.world.record(name, s.idempotencyKey, s.attempt);
            await this.gate.maybeHang(name, s.signal);
            return name.toUpperCase();
          }),
        );
      }

      return results;
    }
  }

  it('resumes at step 3 after a crash; steps 1 and 2 ran exactly once', async () => {
    gate.hangAt = 'three';
    const first = await start([FourSteps]);
    await first.client.start(FourSteps, undefined, { id: 'wf-1' });
    void first.worker.drain();
    await gate.reached.promise;
    await crash(first);

    gate.hangAt = null;
    const second = await start([FourSteps]);
    expect(await second.client.getStatus('wf-1')).toMatchObject({ status: 'running' });
    expect(await second.worker.drain()).toBe(0); // the dead worker's lease is still valid
    clock.advance('31s'); // default lease is 30s
    await second.worker.drain();

    expect(await second.client.getStatus('wf-1')).toMatchObject({
      status: 'completed',
      output: ['ONE', 'TWO', 'THREE', 'FOUR'],
      runs: 2,
    });
    expect(world.calls).toEqual([
      { op: 'one', key: 'wf-1:one', attempt: 1 },
      { op: 'two', key: 'wf-1:two', attempt: 1 },
      { op: 'three', key: 'wf-1:three', attempt: 1 }, // died mid-step
      { op: 'three', key: 'wf-1:three', attempt: 2 }, // same key: the effect can dedupe
      { op: 'four', key: 'wf-1:four', attempt: 1 },
    ]);
    expect(second.events.map((e) => e.type)).toEqual([
      'workflow-resumed',
      'step-completed',
      'step-completed',
      'workflow-completed',
    ]);
  });

  it('hands a running instance back at once on graceful shutdown, without using up an attempt', async () => {
    gate.hangAt = 'two';
    gate.abortable = true;
    const first = await start([FourSteps]);
    await first.client.start(FourSteps, undefined, { id: 'wf-1' });
    const draining = first.worker.drain();
    await gate.reached.promise;
    await first.close(); // the step sees signal.aborted and stops
    nodes.splice(0);
    await draining;

    gate.hangAt = null;
    const second = await start([FourSteps]);
    expect(await second.worker.drain()).toBe(1); // no lease to wait for
    expect(await second.client.getStatus('wf-1')).toMatchObject({ status: 'completed' });
    expect(world.calls.filter((c) => c.op === 'two').map((c) => c.attempt)).toEqual([1, 1]);
  });

  it.each(['successful journal read', 'failed journal read', 'hand-back read'] as const)(
    'leaves the lease to expire when a %s settles after shutdown',
    async (readHeld) => {
      const run = vi.fn();
      @Workflow('late-journal')
      class LateJournal {
        async run() {
          run();
        }
      }

      const node = await start([LateJournal]);
      await node.client.start(LateJournal, undefined, { id: 'late-journal' });
      const claimed = deferred();
      const releaseClaim = deferred();
      const releaseRead = deferred();
      const claim = node.store.claim.bind(node.store);
      const heldClaim = vi.spyOn(node.store, 'claim').mockImplementationOnce(async (request) => {
        const result = await claim(request);
        claimed.resolve();
        await releaseClaim.promise;
        return result;
      });
      const write = vi.spyOn(node.store, 'write').mockResolvedValue(true);
      const logged = vi.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
      const draining = node.worker.drain();
      const get = vi.spyOn(node.store, 'get');
      let stopping: Promise<void> | undefined;

      try {
        await claimed.promise;
        const details = await node.store.get('late-journal', { journal: true });
        expect(details?.leaseUntil).toBe(clock.now() + 30_000);
        get.mockClear();
        get.mockImplementation(async (_id, options) => {
          if (!options?.journal || readHeld !== 'hand-back read') {
            await releaseRead.promise;
          }
          if (options?.journal && readHeld !== 'successful journal read') {
            throw new Error('Cannot read the journal');
          }
          return details;
        });

        vi.useFakeTimers();
        const stopped = vi.fn();
        stopping = node.worker.shutdown().then(stopped);
        await vi.advanceTimersByTimeAsync(40);
        releaseClaim.resolve();
        await vi.advanceTimersByTimeAsync(0);
        expect(get).toHaveBeenNthCalledWith(1, 'late-journal', { journal: true });
        const readsBeforeDeadline = readHeld === 'hand-back read' ? 2 : 1;
        expect(get).toHaveBeenCalledTimes(readsBeforeDeadline);
        expect(stopped).not.toHaveBeenCalled();

        await vi.advanceTimersByTimeAsync(10);
        expect(stopped).toHaveBeenCalledOnce();
        releaseRead.resolve();
        await vi.advanceTimersByTimeAsync(0);

        expect(run).not.toHaveBeenCalled();
        expect(node.events.map((event) => event.type)).not.toContain('workflow-started');
        expect(get).toHaveBeenCalledTimes(readsBeforeDeadline);
        expect(write).not.toHaveBeenCalled();
      } finally {
        releaseClaim.resolve();
        releaseRead.resolve();
        vi.useRealTimers();
        await Promise.allSettled([draining, stopping]);
        heldClaim.mockRestore();
        get.mockRestore();
        write.mockRestore();
        logged.mockRestore();
      }
    },
  );

  it("applies an execution's journal writes in the order it issued them, however slow the store answers one", async () => {
    @Workflow('checkpointed')
    class Checkpointed {
      async run(ctx: WorkflowContext) {
        const result = await ctx.step('work', ({ heartbeat }) => {
          void heartbeat({ page: 1 }); // a checkpoint the step doesn't wait for
          world.record('work', 'wf-1:work');
          return 'done';
        });

        await ctx.step('slow', () => new Promise((resolve) => setTimeout(resolve, 100)));
        await ctx.sleep('nap', '1s');
        return result;
      }
    }

    const node = await start([Checkpointed]);
    const write = node.store.write.bind(node.store);
    vi.spyOn(node.store, 'write').mockImplementation(async (id, token, w) => {
      // The checkpoint's write gets the slow connection (a pool, a network hiccup).
      if (w.entries.some((e) => e.status === 'pending' && e.progress !== undefined)) {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      return write(id, token, w);
    });

    await node.client.start(Checkpointed, undefined, { id: 'wf-1' });
    await node.worker.drain();

    // The completion was issued last, so it's what the journal keeps...
    const [work] = (await node.client.getStatus('wf-1', { journal: true }))!.journal;
    expect(work).toMatchObject({ name: 'work', status: 'completed', result: 'done' });
    expect(work!.progress).toBeUndefined();

    clock.advance('1s');
    await node.worker.drain();
    // ...and the replay after the sleep doesn't run the step again.
    expect(await node.client.getStatus('wf-1')).toMatchObject({ status: 'completed', output: 'done' });
    expect(world.count('work')).toBe(1);
  });

  it('re-runs a step whose result could not be journaled, with the same key, once the lease expires', async () => {
    const node = await start([FourSteps]);
    const write = node.store.write.bind(node.store);
    vi.spyOn(node.store, 'write').mockImplementation(async (id, token, w) => {
      if (w.entries.some((e) => e.name === 'two' && e.status === 'completed') && world.count('two') === 1) {
        throw new Error('Connection terminated unexpectedly');
      }
      return write(id, token, w);
    });
    const logged = vi.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);

    await node.client.start(FourSteps, undefined, { id: 'wf-1' });
    await node.worker.drain();

    // Nothing more is recorded; the instance keeps its lease until it expires.
    expect(await node.client.getStatus('wf-1', { journal: true })).toMatchObject({
      status: 'running',
      journal: [{ name: 'one', status: 'completed' }, { name: 'two', status: 'pending', attempts: 1 }],
    });

    clock.advance('31s');
    await node.worker.drain();
    expect(await node.client.getStatus('wf-1')).toMatchObject({ status: 'completed', output: ['ONE', 'TWO', 'THREE', 'FOUR'] });
    expect(world.calls.filter((c) => c.op === 'two')).toEqual([
      { op: 'two', key: 'wf-1:two', attempt: 1 },
      { op: 'two', key: 'wf-1:two', attempt: 2 },
    ]);
    expect(logged).toHaveBeenCalledWith(
      'Store failed while executing "wf-1"; it is retried when its lease expires.',
      expect.objectContaining({ message: 'Connection terminated unexpectedly' }),
    );
    logged.mockRestore();
  });

  it('gives up on a step that keeps killing the process', async () => {
    gate.hangAt = 'two';
    for (let i = 0; i < 3; i++) {
      gate.reached = deferred();
      const node = await start([FourSteps]);
      if (i === 0) {
        await node.client.start(FourSteps, undefined, { id: 'wf-1' });
      }
      void node.worker.drain();
      await gate.reached.promise;
      await crash(node);
      clock.advance('31s');
    }

    const last = await start([FourSteps]);
    await last.worker.drain();

    const status = await last.client.getStatus('wf-1');
    expect(status).toMatchObject({
      status: 'failed',
      error: { name: 'StepFailedError', message: expect.stringContaining('Attempt 3 did not finish') },
    });
    expect(world.count('two')).toBe(3); // default attempts
  });
});

describe('compensation', () => {
  @Workflow('trip')
  class Trip {
    constructor(
      @Inject(World) private readonly world: World,
      @Inject(Gate) private readonly gate: Gate,
    ) {}

    private book(ctx: WorkflowContext, item: string) {
      return ctx.step(
        `book-${item}`,
        (s) => {
          this.world.record(`book-${item}`, s.idempotencyKey);
          return { ref: `${item}-123` };
        },
        {
          compensate: async (booking, s) => {
            this.world.record(`cancel-${item}`, s.idempotencyKey, s.attempt);
            this.gate.seen.push([booking.ref, s.reason.name]);
            await this.gate.maybeHang(`cancel-${item}`, s.signal);

            if (this.gate.failAt === `cancel-${item}`) {
              throw new NonRetryableStepError('provider down');
            }
            if (this.gate.flakyAt === `cancel-${item}` && s.attempt === 1) {
              throw new Error('provider timeout');
            }
          },
        },
      );
    }

    async run(ctx: WorkflowContext) {
      await this.book(ctx, 'flight');
      await this.book(ctx, 'hotel');
      await this.book(ctx, 'car');
      await ctx.step('charge-card', () => {
        throw new NonRetryableStepError('card declined');
      });
    }
  }

  it('undoes completed steps in reverse order, resuming the compensation after a crash', async () => {
    gate.hangAt = 'cancel-hotel';
    const first = await start([Trip]);
    await first.client.start(Trip, undefined, { id: 'trip-1' });
    void first.worker.drain();
    await gate.reached.promise;
    await crash(first);
    expect(world.ops()).toEqual(['book-flight', 'book-hotel', 'book-car', 'cancel-car', 'cancel-hotel']);

    gate.hangAt = null;
    const second = await start([Trip]);
    expect(await second.client.getStatus('trip-1')).toMatchObject({ status: 'compensating' });
    clock.advance('31s');
    await second.worker.drain();

    expect(world.ops()).toEqual([
      'book-flight',
      'book-hotel',
      'book-car',
      'cancel-car',
      'cancel-hotel', // died here
      'cancel-hotel', // replayed the journal to rebuild the compensations, skipped cancel-car
      'cancel-flight',
    ]);

    const status = await second.client.getStatus('trip-1', { journal: true });
    expect(status).toMatchObject({
      status: 'failed',
      error: { name: 'StepFailedError', message: 'Step "charge-card" failed after 1 attempt(s): NonRetryableStepError: card declined' },
    });
    expect(status!.journal!.filter((e) => e.kind === 'compensation').map((e) => [e.name, e.status, e.attempts])).toEqual([
      ['$compensate:book-car', 'completed', 1],
      ['$compensate:book-hotel', 'completed', 2],
      ['$compensate:book-flight', 'completed', 1],
    ]);

    // Each compensation got the step's journaled result and the failure reason.
    expect(gate.seen).toEqual([
      ['car-123', 'StepFailedError'],
      ['hotel-123', 'StepFailedError'],
      ['hotel-123', 'StepFailedError'],
      ['flight-123', 'StepFailedError'],
    ]);
  });

  it('retries a compensation with backoff, and does not accept a cancel while it is already undoing', async () => {
    gate.flakyAt = 'cancel-hotel';
    const node = await start([Trip]);
    await node.client.start(Trip, undefined, { id: 'trip-1' });
    await node.worker.drain();
    const retryAt = clock.now() + 1_000; // default retry policy
    expect(await node.client.getStatus('trip-1')).toMatchObject({ status: 'compensating', wakeAt: retryAt });

    // Cancelling would undo the same steps and still end as failed: the call says so.
    expect(await node.client.cancel('trip-1')).toMatchObject({ accepted: false, cancelRequested: false });
    expect(await node.worker.drain()).toBe(0); // the backoff is left alone
    expect(await node.client.getStatus('trip-1')).toMatchObject({ status: 'compensating', wakeAt: retryAt });

    clock.advance('1s');
    await node.worker.drain();

    // The failure came first, so it stays the reason.
    expect(await node.client.getStatus('trip-1')).toMatchObject({ status: 'failed', error: { name: 'StepFailedError' } });
    expect(world.ops().slice(3)).toEqual(['cancel-car', 'cancel-hotel', 'cancel-hotel', 'cancel-flight']);
  });

  it('ends as compensation_failed when a compensation gives up', async () => {
    gate.failAt = 'cancel-hotel';
    const node = await start([Trip]);
    await node.client.start(Trip, undefined, { id: 'trip-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('trip-1')).toMatchObject({
      status: 'compensation_failed',
      error: {
        name: 'StepFailedError',
        compensation: { name: 'NonRetryableStepError', message: expect.stringContaining('provider down') },
      },
    });
    // cancel-flight never ran: an operator decides what to do with a half-undone saga.
    expect(world.ops().slice(3)).toEqual(['cancel-car', 'cancel-hotel']);
  });
});

describe('retries', () => {
  const attempts: number[] = [];

  @Workflow('flaky')
  class Flaky {
    constructor(@Inject(World) private readonly world: World) {}

    async run(ctx: WorkflowContext) {
      return ctx.step(
        'call-api',
        (s) => {
          this.world.record('call', s.idempotencyKey, s.attempt);
          attempts.push(clock.now());
          if (s.attempt < 3) {
            throw new Error('503 Service Unavailable');
          }
          return 'ok';
        },
        { retry: { attempts: 4, backoff: { delay: '1s', factor: 3 } } },
      );
    }
  }

  it('retries a step with durable exponential backoff', async () => {
    attempts.length = 0;
    const node = await start([Flaky]);
    const t0 = clock.now();
    await node.client.start(Flaky, undefined, { id: 'f-1' });

    await node.worker.drain();
    expect(await node.client.getStatus('f-1')).toMatchObject({ status: 'suspended', wakeAt: t0 + 1_000 });

    clock.advance('1s');
    await node.worker.drain();
    expect(await node.client.getStatus('f-1')).toMatchObject({ status: 'suspended', wakeAt: t0 + 4_000 });

    clock.advance(2_999);
    expect(await node.worker.drain()).toBe(0);
    clock.advance(1);
    await node.worker.drain();

    expect(await node.client.getStatus('f-1')).toMatchObject({ status: 'completed', output: 'ok' });
    expect(attempts).toEqual([t0, t0 + 1_000, t0 + 4_000]);
    expect(new Set(world.calls.map((c) => c.key))).toEqual(new Set(['f-1:call-api']));
    expect(node.events.filter((e) => e.type === 'step-failed')).toMatchObject([
      { attempt: 1, retryAt: t0 + 1_000, error: { message: '503 Service Unavailable' } },
      { attempt: 2, retryAt: t0 + 4_000 },
    ]);
  });

  @Workflow('fallback')
  class Fallback {
    constructor(@Inject(World) private readonly world: World) {}

    async run(ctx: WorkflowContext) {
      let via = 'primary';
      try {
        await ctx.step('primary', () => {
          this.world.record('primary', '');
          throw new NonRetryableStepError('410 Gone');
        });
      } catch (error) {
        if (!(error instanceof StepFailedError)) {
          throw error;
        }

        via = `fallback after ${error.cause.name}`;
        await ctx.step('secondary', () => this.world.record('secondary', ''));
      }

      await ctx.sleep('settle', '1m');
      return via;
    }
  }

  it('journals a failed step, so a workflow that handles the failure replays the same branch', async () => {
    const node = await start([Fallback]);
    await node.client.start(Fallback, undefined, { id: 'fb-1' });
    await node.worker.drain();
    clock.advance('1m');
    await node.worker.drain();

    expect(await node.client.getStatus('fb-1')).toMatchObject({ status: 'completed', output: 'fallback after NonRetryableStepError' });
    expect(world.ops()).toEqual(['primary', 'secondary']); // NonRetryableStepError: one attempt; replay did not re-run either
  });
});

describe('versioning', () => {
  @Workflow('billing')
  class BillingV1 {
    constructor(@Inject(World) private readonly world: World) {}

    async run(ctx: WorkflowContext) {
      await ctx.step('charge', (s) => this.world.record('v1:charge', s.idempotencyKey));
      await ctx.sleep('grace-period', '1d');
      await ctx.step('receipt', (s) => this.world.record('v1:receipt', s.idempotencyKey));
      return 'v1';
    }
  }

  /** The "deployed change": a renamed step, shipped without bumping the version. */
  @Workflow('billing')
  class BillingV1Edited {
    constructor(@Inject(World) private readonly world: World) {}

    async run(ctx: WorkflowContext) {
      await ctx.step('charge-card', (s) => this.world.record('edited:charge-card', s.idempotencyKey));
      await ctx.sleep('grace-period', '1d');
      await ctx.step('receipt', (s) => this.world.record('edited:receipt', s.idempotencyKey));
      return 'edited';
    }
  }

  /** The same change done right. */
  @Workflow('billing', { version: 2 })
  class BillingV2 {
    constructor(@Inject(World) private readonly world: World) {}

    async run(ctx: WorkflowContext) {
      await ctx.step('charge-card', (s) => this.world.record('v2:charge-card', s.idempotencyKey));
      await ctx.step('receipt', (s) => this.world.record('v2:receipt', s.idempotencyKey));
      return 'v2';
    }
  }

  it('fails an instance whose journal no longer matches the code, before re-running the renamed step', async () => {
    const first = await start([BillingV1]);
    await first.client.start('billing', undefined, { id: 'b-1' });
    await first.worker.drain();
    await first.close();
    nodes.splice(0);

    const second = await start([BillingV1Edited]);
    clock.advance('1d');
    await second.worker.drain();

    const status = await second.client.getStatus('b-1');
    expect(status).toMatchObject({ status: 'failed', error: { name: 'WorkflowNonDeterminismError' } });
    expect(status!.error!.message).toContain(
      '"charge", "grace-period" were recorded by an earlier run but not reached before the new step "charge-card"',
    );
    expect(world.ops()).toEqual(['v1:charge']); // no double charge
  });

  it('runs old instances on the old version and new ones on the new version', async () => {
    const first = await start([BillingV1]);
    await first.client.start('billing', undefined, { id: 'old' });
    await first.worker.drain();
    await first.close();
    nodes.splice(0);

    // A worker without v1 leaves the old instance alone.
    const v2only = await start([BillingV2]);
    clock.advance('1d');
    expect(await v2only.worker.drain()).toBe(0);
    await v2only.close();
    nodes.splice(0);

    const both = await start([BillingV1, BillingV2]);
    const handle = await both.client.start('billing', undefined, { id: 'new' });
    expect(handle.version).toBe(2);

    await both.worker.drain();
    expect(await both.client.getStatus('old')).toMatchObject({ version: 1, status: 'completed', output: 'v1' });
    expect(await both.client.getStatus('new')).toMatchObject({ version: 2, status: 'completed', output: 'v2' });
  });

  it('start(Class) starts the highest registered version of that name, unless pinned', async () => {
    const both = await start([BillingV1, BillingV2]);
    // Passing the version 1 class does not keep new instances on version 1 after version 2 ships.
    expect(await both.client.start(BillingV1, undefined, { id: 'by-v1-class' })).toMatchObject({ version: 2 });
    expect(await both.client.start(BillingV2, undefined, { id: 'by-v2-class' })).toMatchObject({ version: 2 });
    expect(await both.client.start(BillingV1, undefined, { id: 'pinned', version: 1 })).toMatchObject({ version: 1 });

    await both.worker.drain();
    expect(await both.client.getStatus('by-v1-class')).toMatchObject({ status: 'completed', output: 'v2' });
    expect(await both.client.getStatus('pinned')).toMatchObject({ version: 1, status: 'suspended' }); // v1 sleeps a day
    await both.close();
    nodes.splice(0);

    // A process that registers no version of it (an API pod) falls back to the class's own version.
    const api = await start([]);
    expect(await api.client.start(BillingV2, undefined, { id: 'from-api' })).toMatchObject({ version: 2 });
    expect(await api.client.start(BillingV1, undefined, { id: 'from-api-v1' })).toMatchObject({ version: 1 });
  });

  @Workflow('loop')
  class DuplicateNames {
    async run(ctx: WorkflowContext) {
      for (let i = 0; i < 2; i++) {
        await ctx.step('remind', () => i);
      }
    }
  }

  it('rejects a step name used twice in one run', async () => {
    const node = await start([DuplicateNames]);
    await node.client.start(DuplicateNames, undefined, { id: 'd-1' });
    await node.worker.drain();

    const status = await node.client.getStatus('d-1');
    expect(status).toMatchObject({ status: 'failed', error: { name: 'WorkflowDefinitionError' } });
    expect(status!.error!.message).toContain('"remind" is used twice');
  });
});
