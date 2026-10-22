/**
 * The execution model in detail: what a replay returns for a journaled step, results that
 * can't be journaled, names the engine refuses, journals the code no longer matches, durable
 * timers and waits across executions, step timeouts, and the retry settings the other suites
 * don't reach (the delay cap, zero attempts, a compensation's inherited retry).
 */
import { Inject } from '@nestjs/common';
import {
  ManualWorkflowClock,
  NonRetryableStepError,
  StepFailedError,
  Workflow,
  WorkflowClient,
  WorkflowError,
  type WorkflowContext,
  type WorkflowStepOptions,
} from '../lib/index.js';
import { boot, forever, tempDb, type Node, type TestDb, World } from './support.js';

let db: TestDb;
let clock: ManualWorkflowClock;
let world: World;
const nodes: Node[] = [];

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock();
  world = new World();
});

afterEach(async () => {
  for (const node of nodes.splice(0)) {
    await node.close();
  }
  db.cleanup();
});

const start = async (workflows: any[], options: Omit<Parameters<typeof boot>[0], 'db' | 'workflows'> = {}) => {
  const node = await boot({ db, clock, workflows, providers: [{ provide: World, useValue: world }], ...options });
  nodes.push(node);
  return node;
};

/** Closes a node as a crash or deploy would end the process, and boots the next one on the same database. */
const restart = async (node: Node, workflows: any[]) => {
  await node.close();
  nodes.splice(nodes.indexOf(node), 1);
  return start(workflows);
};

describe('step memoization', () => {
  it('returns the same JSON value on the first run and on every replay, without running the step again', async () => {
    const seen: unknown[] = [];

    @Workflow('memo')
    class Memo {
      constructor(@Inject(World) private readonly world: World) {}

      async run(ctx: WorkflowContext) {
        const value = await ctx.step('load', (s) => {
          this.world.record('load', s.idempotencyKey);
          return { at: new Date(0), missing: undefined, list: [1, undefined], total: 3, format: () => 'x' };
        });
        seen.push(value);
        await ctx.sleep('pause', '1m');
        return value;
      }
    }

    const node = await start([Memo]);
    await node.client.start(Memo, undefined, { id: 'm-1' });
    await node.worker.drain();
    clock.advance('1m');
    await node.worker.drain();

    const json = { at: '1970-01-01T00:00:00.000Z', list: [1, null], total: 3 };
    expect(seen).toHaveLength(2);
    expect(seen[0]).toStrictEqual(json);
    expect(seen[1]).toStrictEqual(json);
    expect(world.ops()).toEqual(['load']);
    expect(await node.client.getStatus('m-1')).toMatchObject({ status: 'completed', output: json, runs: 2 });
  });

  it('never runs a step again that returned nothing', async () => {
    @Workflow('void-step')
    class VoidStep {
      constructor(@Inject(World) private readonly world: World) {}

      async run(ctx: WorkflowContext) {
        const sent = await ctx.step('notify', (s) => {
          this.world.record('notify', s.idempotencyKey);
        });
        await ctx.sleep('pause', '1m');
        return sent ?? 'nothing';
      }
    }

    const node = await start([VoidStep]);
    await node.client.start(VoidStep, undefined, { id: 'v-1' });
    await node.worker.drain();
    clock.advance('1m');
    await node.worker.drain();

    expect(await node.client.getStatus('v-1', { journal: true })).toMatchObject({
      status: 'completed',
      output: 'nothing',
      journal: [{ name: 'notify', status: 'completed', attempts: 1 }, { name: 'pause' }],
    });
    expect(world.ops()).toEqual(['notify']);
  });

  it('gives up at once on a result that is not JSON: retrying would produce another one', async () => {
    @Workflow('bigint-result')
    class BigintResult {
      constructor(@Inject(World) private readonly world: World) {}

      async run(ctx: WorkflowContext) {
        await ctx.step('fetch-balance', (s) => {
          this.world.record('fetch', s.idempotencyKey, s.attempt);
          return { balance: 10n };
        }, { retry: 5 });
      }
    }

    const node = await start([BigintResult]);
    await node.client.start(BigintResult, undefined, { id: 'b-1' });
    await node.worker.drain();

    const status = await node.client.getStatus('b-1', { journal: true });
    expect(status).toMatchObject({
      status: 'failed',
      error: {
        name: 'StepFailedError',
        message: expect.stringContaining('Step "fetch-balance" failed after 1 attempt(s): NonRetryableStepError: Result of "fetch-balance" is not JSON-serializable'),
      },
      journal: [{ name: 'fetch-balance', status: 'failed', attempts: 1 }],
    });
    expect(world.count('fetch')).toBe(1);
  });

  it('compensates when the output of run() is not JSON', async () => {
    @Workflow('bigint-output')
    class BigintOutput {
      constructor(@Inject(World) private readonly world: World) {}

      async run(ctx: WorkflowContext) {
        await ctx.step('reserve', () => 'r-1', { compensate: (r, s) => this.world.record(`release ${r}`, s.idempotencyKey) });
        return { total: 10n };
      }
    }

    const node = await start([BigintOutput]);
    await node.client.start(BigintOutput, undefined, { id: 'o-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('o-1')).toMatchObject({
      status: 'failed',
      error: { name: 'TypeError', message: expect.stringContaining('BigInt') },
    });
    expect(world.calls).toEqual([{ op: 'release r-1', key: 'o-1:$compensate:reserve', attempt: undefined }]);
  });

  it('hands a workflow that catches a failed step the step, the attempts and the original error by name', async () => {
    @Workflow('catcher')
    class Catcher {
      async run(ctx: WorkflowContext) {
        try {
          await ctx.step('charge', () => {
            throw new NonRetryableStepError('card declined');
          });
        } catch (error) {
          return {
            isStepFailed: error instanceof StepFailedError,
            isWorkflowError: error instanceof WorkflowError,
            isOriginal: error instanceof NonRetryableStepError,
            step: (error as StepFailedError).step,
            attempts: (error as StepFailedError).attempts,
            cause: { name: (error as StepFailedError).cause.name, message: (error as StepFailedError).cause.message },
            message: (error as Error).message,
          };
        }
      }
    }

    const node = await start([Catcher]);
    await node.client.start(Catcher, undefined, { id: 'c-1' });
    await node.worker.drain();

    expect((await node.client.getStatus('c-1'))!.output).toEqual({
      isStepFailed: true,
      isWorkflowError: true,
      isOriginal: false,
      step: 'charge',
      attempts: 1,
      cause: { name: 'NonRetryableStepError', message: 'card declined' },
      message: 'Step "charge" failed after 1 attempt(s): NonRetryableStepError: card declined',
    });
  });
});

describe('names', () => {
  it.each([
    ['an empty step name', (ctx: WorkflowContext) => ctx.step('', () => 1), 'Invalid step name "". Names cannot be empty or start with "$".'],
    ['a step name that starts with $', (ctx: WorkflowContext) => ctx.step('$now:1', () => 1), 'Invalid step name "$now:1". Names cannot be empty or start with "$".'],
    ['a sleep name that starts with $', (ctx: WorkflowContext) => ctx.sleep('$pause', '1s'), 'Invalid sleep name "$pause". Names cannot be empty or start with "$".'],
    ['a wait name that starts with $', (ctx: WorkflowContext) => ctx.waitForSignal('$wait', 'go'), 'Invalid signal name "$wait". Names cannot be empty or start with "$".'],
    ['a commit name that starts with $', async (ctx: WorkflowContext) => ctx.commit('$done'), 'Invalid commit name "$done". Names cannot be empty or start with "$".'],
  ])('fails the instance, without compensating, for %s', async (_label, call, message) => {
    @Workflow(`bad-name-${Math.random().toString(36).slice(2)}`)
    class BadName {
      constructor(@Inject(World) private readonly world: World) {}

      async run(ctx: WorkflowContext) {
        await ctx.step('first', () => 'ok', { compensate: () => this.world.record('undo-first', '') });
        await call(ctx);
      }
    }

    const node = await start([BadName]);
    await node.client.start(BadName, undefined, { id: 'n-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('n-1')).toMatchObject({ status: 'failed', error: { name: 'WorkflowDefinitionError', message } });
    expect(world.ops()).toEqual([]);
  });

  it('rejects a name used by a step and by a sleep in one run', async () => {
    @Workflow('shared-name')
    class SharedName {
      async run(ctx: WorkflowContext) {
        await ctx.step('remind', () => 1);
        await ctx.sleep('remind', '1h');
      }
    }

    const node = await start([SharedName]);
    await node.client.start(SharedName, undefined, { id: 's-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('s-1')).toMatchObject({
      status: 'failed',
      error: { name: 'WorkflowDefinitionError', message: expect.stringContaining('"remind" is used twice in one run of workflow "shared-name@1"') },
    });
  });

  it('fails the instance for a wait on an empty signal name', async () => {
    @Workflow('empty-signal')
    class EmptySignal {
      async run(ctx: WorkflowContext) {
        await ctx.waitForSignal('wait', '');
      }
    }

    const node = await start([EmptySignal]);
    await node.client.start(EmptySignal, undefined, { id: 'e-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('e-1')).toMatchObject({
      status: 'failed',
      error: { name: 'TypeError', message: 'Invalid signal name "". Use a non-empty string such as "shipment.delivered".' },
    });
  });
});

describe('journals the code no longer matches', () => {
  it('fails an instance that completes without reaching a journaled step, and runs no compensation', async () => {
    @Workflow('trim')
    class TrimV1 {
      constructor(@Inject(World) private readonly world: World) {}

      async run(ctx: WorkflowContext) {
        await ctx.step('reserve', () => 'r-1', { compensate: () => this.world.record('release', '') });
        await ctx.step('audit', () => this.world.record('audit', ''));
        await ctx.sleep('pause', '1h');
        return 'done';
      }
    }

    // Deployed without a version bump: the audit step is gone.
    @Workflow('trim')
    class TrimV1WithoutAudit {
      constructor(@Inject(World) private readonly world: World) {}

      async run(ctx: WorkflowContext) {
        await ctx.step('reserve', () => 'r-1', { compensate: () => this.world.record('release', '') });
        await ctx.sleep('pause', '1h');
        return 'done';
      }
    }

    const first = await start([TrimV1]);
    await first.client.start(TrimV1, undefined, { id: 't-1' });
    await first.worker.drain();

    const second = await restart(first, [TrimV1WithoutAudit]);
    clock.advance('1h');
    await second.worker.drain();

    expect(await second.client.getStatus('t-1')).toMatchObject({
      status: 'failed',
      error: {
        name: 'WorkflowNonDeterminismError',
        message:
          'Instance "t-1" of workflow "trim@1" does not match its journal: "audit" was recorded by an earlier run but never reached. ' +
          'Ship step removals as a new workflow version.',
      },
    });
    expect(world.ops()).toEqual(['audit']);
  });

  it('fails an instance whose journal recorded a name as a sleep that the code now calls as a step, before running it', async () => {
    @Workflow('kind-change')
    class Before {
      async run(ctx: WorkflowContext) {
        await ctx.sleep('cool-off', '1d');
      }
    }

    @Workflow('kind-change')
    class After {
      constructor(@Inject(World) private readonly world: World) {}

      async run(ctx: WorkflowContext) {
        await ctx.step('cool-off', () => this.world.record('cool-off', ''));
      }
    }

    const first = await start([Before]);
    await first.client.start(Before, undefined, { id: 'k-1' });
    await first.worker.drain();

    const second = await restart(first, [After]);
    clock.advance('1d');
    await second.worker.drain();

    expect(await second.client.getStatus('k-1')).toMatchObject({
      status: 'failed',
      error: {
        name: 'WorkflowNonDeterminismError',
        message: expect.stringContaining('"cool-off" was recorded as a sleep, but the code now calls it as a step.'),
      },
    });
    expect(world.ops()).toEqual([]);
  });
});

describe('durable timers', () => {
  it('parks each sleep until its deadline, for every duration unit and an absolute time', async () => {
    const t0 = clock.now();
    const until = t0 + 5 * 86_400_000;

    @Workflow('units')
    class Units {
      async run(ctx: WorkflowContext) {
        await ctx.sleep('ms', '250ms');
        await ctx.sleep('fraction', '1.5s');
        await ctx.sleep('minutes', '2m');
        await ctx.sleep('week', '1w');
        await ctx.sleep('until', { until: new Date(until + 8 * 86_400_000) });
        return ctx.now();
      }
    }

    const node = await start([Units]);
    await node.client.start(Units, undefined, { id: 'u-1' });

    const wakeAts: number[] = [];
    for (let i = 0; i < 5; i++) {
      await node.worker.drain();
      const status = await node.client.getStatus('u-1');
      wakeAts.push(status!.wakeAt! - clock.now());
      clock.set(status!.wakeAt!);
    }
    await node.worker.drain();

    expect(wakeAts).toEqual([250, 1_500, 120_000, 604_800_000, until + 8 * 86_400_000 - (t0 + 250 + 1_500 + 120_000 + 604_800_000)]);
    expect(await node.client.getStatus('u-1')).toMatchObject({ status: 'completed', output: until + 8 * 86_400_000 });
  });

  it('passes a zero sleep, or a deadline in the past, without suspending', async () => {
    @Workflow('no-wait')
    class NoWait {
      async run(ctx: WorkflowContext) {
        await ctx.sleep('zero', 0);
        await ctx.sleep('past', { until: 0 });
        return 'through';
      }
    }

    const node = await start([NoWait]);
    await node.client.start(NoWait, undefined, { id: 'n-1' });
    expect(await node.worker.drain()).toBe(1);

    expect(await node.client.getStatus('n-1', { journal: true })).toMatchObject({
      status: 'completed',
      runs: 1,
      output: 'through',
      journal: [
        { name: 'zero', kind: 'sleep', status: 'completed', wakeAt: clock.now() },
        { name: 'past', kind: 'sleep', status: 'completed', wakeAt: 0 },
      ],
    });
  });

  it.each([
    ['a malformed duration', '5 minutes', 'Invalid duration "5 minutes". Use milliseconds or a string such as "15m" or "3d".'],
    ['a negative duration', -1, 'Invalid duration -1. Use a non-negative number of milliseconds.'],
    ['an infinite duration', Number.POSITIVE_INFINITY, 'Invalid duration Infinity. Use a non-negative number of milliseconds.'],
  ])('fails the instance for %s', async (_label, duration, message) => {
    @Workflow(`bad-sleep-${Math.random().toString(36).slice(2)}`)
    class BadSleep {
      async run(ctx: WorkflowContext) {
        await ctx.sleep('pause', duration as '1s');
      }
    }

    const node = await start([BadSleep]);
    await node.client.start(BadSleep, undefined, { id: 'b-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('b-1', { journal: true })).toMatchObject({ status: 'failed', error: { name: 'TypeError', message }, journal: [] });
  });
});

describe('durable waits', () => {
  it('keeps the deadline a wait recorded when it was first reached, however often the instance resumes', async () => {
    @Workflow('approve-or-expire')
    class ApproveOrExpire {
      async run(ctx: WorkflowContext) {
        const [, approval] = await Promise.all([ctx.sleep('reminder', '10m'), ctx.waitForSignal('approval', 'approve', { timeout: '1h' })]);
        return approval;
      }
    }

    const node = await start([ApproveOrExpire]);
    const t0 = clock.now();
    await node.client.start(ApproveOrExpire, undefined, { id: 'a-1' });
    await node.worker.drain();
    expect(await node.client.getStatus('a-1')).toMatchObject({ status: 'suspended', wakeAt: t0 + 600_000, waits: [{ signal: 'approve', key: null }] });

    clock.advance('10m');
    await node.worker.drain();
    expect(await node.client.getStatus('a-1')).toMatchObject({ status: 'suspended', wakeAt: t0 + 3_600_000, waits: [{ signal: 'approve', key: null }] });

    clock.set(t0 + 3_600_000 - 1);
    expect(await node.worker.drain()).toBe(0);
    clock.advance(1);
    await node.worker.drain();

    expect(await node.client.getStatus('a-1', { journal: true })).toMatchObject({
      status: 'completed',
      output: null,
      runs: 3,
      journal: [
        { name: 'reminder', status: 'completed' },
        { name: 'approval', kind: 'signal', status: 'completed', wakeAt: t0 + 3_600_000, result: { signalId: null, payload: null } },
      ],
    });
  });

  it('parks a wait without a timeout with no wake time: only a signal resumes it', async () => {
    @Workflow('forever-wait')
    class ForeverWait {
      async run(ctx: WorkflowContext) {
        return ctx.waitForSignal<string>('go', 'go', { key: 'k' });
      }
    }

    const node = await start([ForeverWait]);
    await node.client.start(ForeverWait, undefined, { id: 'f-1' });
    await node.worker.drain();
    expect(await node.client.getStatus('f-1')).toMatchObject({ status: 'suspended', wakeAt: null, leaseUntil: null });

    clock.advance('365d');
    expect(await node.worker.drain()).toBe(0);

    await node.client.signal('go', 'finally', { key: 'k' });
    await node.worker.drain();
    expect(await node.client.getStatus('f-1')).toMatchObject({ status: 'completed', output: 'finally' });
  });

  it('times a wait with a zero timeout out in the same execution, unless the signal is already there', async () => {
    @Workflow('peek')
    class Peek {
      async run(ctx: WorkflowContext) {
        const before = await ctx.waitForSignal('before', 'poke', { timeout: 0 });
        await ctx.step('gap', () => 'ok');
        await ctx.sleep('later', '1m');
        const after = await ctx.waitForSignal('after', 'poke', { timeout: 0 });
        return { before, after };
      }
    }

    const node = await start([Peek]);
    await node.client.start(Peek, undefined, { id: 'p-1' });
    await node.worker.drain();
    await node.client.signal('poke', 'here');
    clock.advance('1m');
    await node.worker.drain();

    expect(await node.client.getStatus('p-1')).toMatchObject({ status: 'completed', runs: 2, output: { before: null, after: 'here' } });
    expect(node.events.filter((e) => e.type === 'signal-timed-out' || e.type === 'signal-received').map((e) => e.type)).toEqual([
      'signal-timed-out',
      'signal-received',
    ]);
  });
});

describe('step timeouts', () => {
  it('fails an attempt that runs past its timeout, and aborts its signal with the timeout error', async () => {
    const reasons: string[] = [];

    @Workflow('slow')
    class Slow {
      async run(ctx: WorkflowContext) {
        await ctx.step(
          'call',
          (s) => {
            s.signal.addEventListener('abort', () => reasons.push(`${s.signal.reason.name} (attempt ${s.attempt})`));
            return forever();
          },
          { timeout: 30, retry: { attempts: 2, backoff: { delay: '1s' } } },
        );
      }
    }

    const node = await start([Slow]);
    const t0 = clock.now();
    await node.client.start(Slow, undefined, { id: 's-1' });
    await node.worker.drain();
    expect(await node.client.getStatus('s-1')).toMatchObject({ status: 'suspended', wakeAt: t0 + 1_000 });

    clock.advance('1s');
    await node.worker.drain();

    expect(await node.client.getStatus('s-1')).toMatchObject({
      status: 'failed',
      error: { name: 'StepFailedError', message: 'Step "call" failed after 2 attempt(s): StepTimeoutError: Step "call" timed out after 30ms.' },
    });
    expect(reasons).toEqual(['StepTimeoutError (attempt 1)', 'StepTimeoutError (attempt 2)']);
    expect(node.events.filter((e) => e.type === 'step-failed')).toMatchObject([
      { attempt: 1, retryAt: t0 + 1_000, error: { name: 'StepTimeoutError' } },
      { attempt: 2, retryAt: null, error: { name: 'StepTimeoutError' } },
    ]);
  });

  it('lets a step that finishes within its timeout complete', async () => {
    @Workflow('quick')
    class Quick {
      async run(ctx: WorkflowContext) {
        return ctx.step('call', async () => 'fast', { timeout: '1m', heartbeatTimeout: '1m' });
      }
    }

    const node = await start([Quick]);
    await node.client.start(Quick, undefined, { id: 'q-1' });
    await node.worker.drain();
    expect(await node.client.getStatus('q-1')).toMatchObject({ status: 'completed', output: 'fast' });
  });

  // Node runs a timer longer than 2^31 - 1 ms (about 24.8 days) after 1 ms instead.
  it.each<WorkflowStepOptions<unknown>>([{ timeout: '30d' }, { heartbeatTimeout: '30d' }])('lets a step complete under %o, longer than a timer can wait', async (options) => {
    @Workflow('month-long')
    class MonthLong {
      async run(ctx: WorkflowContext) {
        return ctx.step('call', () => new Promise((resolve) => setTimeout(() => resolve('done'), 20)), options);
      }
    }

    const node = await start([MonthLong]);
    await node.client.start(MonthLong, undefined, { id: 'm-1' });
    await node.worker.drain();
    expect(await node.client.getStatus('m-1')).toMatchObject({ status: 'completed', output: 'done' });
    expect(node.events.filter((e) => e.type === 'step-failed')).toEqual([]);
  });
});

describe('retry settings', () => {
  const attemptsAt: number[] = [];

  const failing = (name: string, retry: unknown) => {
    @Workflow(name)
    class Failing {
      async run(ctx: WorkflowContext) {
        await ctx.step(
          'call',
          () => {
            attemptsAt.push(clock.now());
            throw new Error('503');
          },
          { retry: retry as any },
        );
      }
    }

    return Failing;
  };

  const runToEnd = async (node: Node, id: string) => {
    for (let i = 0; i < 20; i++) {
      await node.worker.drain();
      const status = await node.client.getStatus(id);
      if (status!.status !== 'suspended') {
        return status!;
      }
      clock.set(status!.wakeAt!);
    }

    throw new Error('did not finish');
  };

  beforeEach(() => {
    attemptsAt.length = 0;
  });

  it('caps the exponential backoff at maxDelay', async () => {
    const Capped = failing('capped', { attempts: 5, backoff: { delay: '1s', factor: 10, maxDelay: '5s' } });
    const node = await start([Capped]);
    const t0 = clock.now();
    await node.client.start(Capped, undefined, { id: 'c-1' });

    expect(await runToEnd(node, 'c-1')).toMatchObject({ status: 'failed', error: { message: expect.stringContaining('after 5 attempt(s)') } });
    expect(attemptsAt.map((at) => at - t0)).toEqual([0, 1_000, 6_000, 11_000, 16_000]);
  });

  it('caps a delay longer than the default maxDelay at 5 minutes', async () => {
    const Long = failing('long-delay', { attempts: 2, backoff: { delay: '1h' } });
    const node = await start([Long]);
    const t0 = clock.now();
    await node.client.start(Long, undefined, { id: 'l-1' });

    await runToEnd(node, 'l-1');
    expect(attemptsAt.map((at) => at - t0)).toEqual([0, 300_000]);
  });

  it('makes one attempt for retry: 0, as for retry: false', async () => {
    const Zero = failing('zero-retries', 0);
    const node = await start([Zero]);
    await node.client.start(Zero, undefined, { id: 'z-1' });

    expect(await runToEnd(node, 'z-1')).toMatchObject({ status: 'failed', error: { message: 'Step "call" failed after 1 attempt(s): Error: 503' } });
    expect(attemptsAt).toHaveLength(1);
  });

  it('rejects a backoff factor that would not grow, at startup', async () => {
    await expect(start([], { retry: { backoff: { factor: 0 } } })).rejects.toThrow('Invalid backoff factor 0. Use a positive number (1 = constant).');
  });

  it("retries a compensation with the step's retry unless compensateRetry overrides it", async () => {
    @Workflow('undo-retries')
    class UndoRetries {
      constructor(@Inject(World) private readonly world: World) {}

      async run(ctx: WorkflowContext) {
        const undo = (step: string) => (_: unknown, s: { attempt: number }) => {
          this.world.record(`undo-${step}`, '', s.attempt);
          throw new Error('provider down');
        };
        await ctx.step('inherits', () => 1, { retry: { attempts: 2, backoff: { delay: '1s' } }, compensate: undo('inherits') });
        await ctx.step('overrides', () => 2, { retry: 5, compensateRetry: false, compensate: undo('overrides') });
        ctx.fail('out of stock');
      }
    }

    const node = await start([UndoRetries]);
    await node.client.start(UndoRetries, undefined, { id: 'u-1' });
    await node.worker.drain();

    // Reverse order: the override gives up at once, which stops the compensation there.
    expect(await node.client.getStatus('u-1')).toMatchObject({
      status: 'compensation_failed',
      error: {
        name: 'WorkflowFailedError',
        message: 'out of stock',
        compensation: { message: 'Step "$compensate:overrides" failed after 1 attempt(s): Error: provider down' },
      },
    });
    expect(world.ops()).toEqual(['undo-overrides']);
  });

  it("gives a compensation the step's attempts when it has no compensateRetry", async () => {
    @Workflow('undo-inherits')
    class UndoInherits {
      constructor(@Inject(World) private readonly world: World) {}

      async run(ctx: WorkflowContext) {
        await ctx.step('book', () => 1, {
          retry: { attempts: 2, backoff: { delay: '1s' } },
          compensate: (_, s) => {
            this.world.record('undo-book', s.idempotencyKey, s.attempt);
            throw new Error('provider down');
          },
        });
        ctx.fail('out of stock');
      }
    }

    const node = await start([UndoInherits]);
    const t0 = clock.now();
    await node.client.start(UndoInherits, undefined, { id: 'u-2' });
    await node.worker.drain();
    expect(await node.client.getStatus('u-2')).toMatchObject({ status: 'compensating', wakeAt: t0 + 1_000 });

    clock.advance('1s');
    await node.worker.drain();

    expect(await node.client.getStatus('u-2')).toMatchObject({
      status: 'compensation_failed',
      error: { compensation: { message: 'Step "$compensate:book" failed after 2 attempt(s): Error: provider down' } },
    });
    expect(world.calls).toEqual([
      { op: 'undo-book', key: 'u-2:$compensate:book', attempt: 1 },
      { op: 'undo-book', key: 'u-2:$compensate:book', attempt: 2 },
    ]);
  });
});

describe('run timeouts', () => {
  @Workflow('await-approval', { timeout: '1h' })
  class AwaitApproval {
    constructor(@Inject(World) private readonly world: World) {}

    async run(ctx: WorkflowContext) {
      await ctx.step('reserve', () => this.world.record('reserve', ''), { compensate: () => this.world.record('release', '') });
      return ctx.waitForSignal('approval', 'approve');
    }
  }

  it('stores the deadline, parks a wait without a timeout until it, and then compensates and fails, across a restart', async () => {
    let node = await start([AwaitApproval]);
    const t0 = clock.now();
    await node.client.start(AwaitApproval, undefined, { id: 'a-1' });
    await node.worker.drain();
    expect(await node.client.getStatus('a-1')).toMatchObject({ status: 'suspended', deadline: t0 + 3_600_000, wakeAt: t0 + 3_600_000 });

    node = await restart(node, [AwaitApproval]);
    clock.advance('59m');
    expect(await node.worker.drain()).toBe(0);
    clock.advance('1m');
    await node.worker.drain();

    expect(await node.client.getStatus('a-1', { journal: true })).toMatchObject({
      status: 'failed',
      error: {
        name: 'WorkflowTimeoutError',
        message: `Instance "a-1" of workflow "await-approval@1" did not finish within its timeout (its deadline was ${new Date(t0 + 3_600_000).toISOString()}).`,
      },
      journal: [
        { name: 'reserve', status: 'completed' },
        { name: 'approval', status: 'cancelled' },
        { name: '$compensate:reserve', status: 'completed' },
      ],
    });
    expect(world.ops()).toEqual(['reserve', 'release']);
    expect(node.events.map((e) => e.type)).toEqual(['workflow-resumed', 'workflow-compensating', 'step-compensated', 'workflow-failed']);
  });

  it('lets an instance that finishes in time complete, and a signal wake it before the deadline', async () => {
    const node = await start([AwaitApproval]);
    await node.client.start(AwaitApproval, undefined, { id: 'a-1' });
    await node.worker.drain();
    clock.advance('59m');
    await node.client.signal('approve', 'yes');
    await node.worker.drain();

    expect(await node.client.getStatus('a-1')).toMatchObject({ status: 'completed', output: 'yes' });
    expect(world.ops()).toEqual(['reserve']);
  });

  it("takes start()'s timeout over the decorator's, and caps a longer sleep at the deadline", async () => {
    @Workflow('nap', { timeout: '30d' })
    class Nap {
      async run(ctx: WorkflowContext) {
        await ctx.sleep('week', '7d');
        return 'rested';
      }
    }

    const node = await start([Nap]);
    const t0 = clock.now();
    await node.client.start(Nap, undefined, { id: 'short', timeout: '1d' });
    await node.client.start(Nap, undefined, { id: 'long' });
    await node.worker.drain();
    expect(await node.client.getStatus('short')).toMatchObject({ deadline: t0 + 86_400_000, wakeAt: t0 + 86_400_000 });
    expect(await node.client.getStatus('long')).toMatchObject({ deadline: t0 + 30 * 86_400_000, wakeAt: t0 + 7 * 86_400_000 });

    clock.advance('7d');
    await node.worker.drain();
    expect(await node.client.getStatus('short')).toMatchObject({ status: 'failed', error: { name: 'WorkflowTimeoutError' } });
    expect(await node.client.getStatus('long')).toMatchObject({ status: 'completed', output: 'rested' });
  });

  it('lets a step that runs past the deadline finish, and stops at the next ctx call', async () => {
    @Workflow('slow-export', { timeout: '10m' })
    class SlowExport {
      constructor(@Inject(World) private readonly world: World) {}

      async run(ctx: WorkflowContext) {
        await ctx.step('export', () => {
          clock.advance('15m');
          this.world.record('export', '');
        }, { compensate: () => this.world.record('delete-export', '') });
        await ctx.step('publish', () => this.world.record('publish', ''));
      }
    }

    const node = await start([SlowExport]);
    await node.client.start(SlowExport, undefined, { id: 's-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('s-1', { journal: true })).toMatchObject({
      status: 'failed',
      error: { name: 'WorkflowTimeoutError' },
      journal: [{ name: 'export', status: 'completed' }, { name: '$compensate:export', status: 'completed' }],
    });
    expect(world.ops()).toEqual(['export', 'delete-export']);
  });

  it('times out an instance no worker claimed before its deadline without running a step', async () => {
    const node = await start([AwaitApproval]);
    await node.client.start(AwaitApproval, undefined, { id: 'late', timeout: '1m' });
    clock.advance('2m');
    await node.worker.drain();

    expect(await node.client.getStatus('late', { journal: true })).toMatchObject({ status: 'failed', error: { name: 'WorkflowTimeoutError' }, journal: [] });
    expect(world.ops()).toEqual([]);
  });

  it('rejects a timeout that is not a positive duration, before creating anything', async () => {
    expect(() => Workflow('zero', { timeout: 0 })).toThrow(new TypeError('Invalid timeout 0 for workflow "zero". Use a positive duration, such as "30d".'));
    expect(() => Workflow('soon', { timeout: 'soon' as never })).toThrow('Invalid timeout "soon" for workflow "soon".');

    const node = await start([AwaitApproval]);
    await expect(node.client.start(AwaitApproval, undefined, { timeout: '-1m' as never })).rejects.toThrow(
      new TypeError('Invalid timeout "-1m" for start(). Use a positive duration, such as "30d".'),
    );
    expect(await node.client.list()).toEqual([]);
  });
});

describe('journal growth', () => {
  /** A reminder loop: a step and a sleep per round, so every round adds two entries. */
  @Workflow('reminders')
  class Reminders {
    constructor(@Inject(World) private readonly world: World) {}

    async run(ctx: WorkflowContext, input: { rounds: number; note?: string }) {
      await ctx.step('hold', () => this.world.record('hold', ''), { compensate: () => this.world.record('unhold', '') });
      for (let i = 1; i <= input.rounds; i++) {
        await ctx.step(`remind-${i}`, () => input.note ?? `round ${i}`);
        await ctx.sleep(`wait-${i}`, '1d');
      }
      return input.rounds;
    }
  }

  const runDays = async (node: Node, days: number) => {
    for (let day = 0; day <= days; day++) {
      await node.worker.drain();
      clock.advance('1d');
    }
  };

  it('warns once, when the journal crosses the line, however many executions load it after', async () => {
    const node = await start([Reminders], { journal: { warnEntries: 6 } });
    await node.client.start(Reminders, { rounds: 5 }, { id: 'r-1' });
    await runDays(node, 6);

    expect(await node.client.getStatus('r-1')).toMatchObject({ status: 'completed', output: 5 });
    const large = node.events.filter((e) => e.type === 'journal-large');
    expect(large).toEqual([expect.objectContaining({ id: 'r-1', entries: 6, bytes: expect.any(Number) })]);
  });

  it('fails the instance before it records an entry past maxEntries, and still compensates', async () => {
    const node = await start([Reminders], { journal: { maxEntries: 6 } });
    await node.client.start(Reminders, { rounds: 5 }, { id: 'r-1' });
    await runDays(node, 6);

    const status = await node.client.getStatus('r-1', { journal: true });
    expect(status).toMatchObject({ status: 'failed', error: { name: 'WorkflowJournalLimitError' } });
    expect(status!.error!.message).toMatch(/^Instance "r-1" of workflow "reminders@1" reached its journal limit before "wait-3": 6 entries, \d+ bytes \(journal.maxEntries 6, journal.maxBytes 10000000\)/);
    expect(status!.journal.map((e) => e.name)).toEqual(['hold', 'remind-1', 'wait-1', 'remind-2', 'wait-2', 'remind-3', '$compensate:hold']);
    expect(world.ops()).toEqual(['hold', 'unhold']);
  });

  it('fails on bytes too: a large result is kept, and the next new entry stops the run', async () => {
    const node = await start([Reminders], { journal: { maxBytes: 5_000 } });
    await node.client.start(Reminders, { rounds: 3, note: 'x'.repeat(6_000) }, { id: 'r-1' });
    await runDays(node, 4);

    expect(await node.client.getStatus('r-1', { journal: true })).toMatchObject({
      status: 'failed',
      error: { name: 'WorkflowJournalLimitError' },
      journal: [{ name: 'hold' }, { name: 'remind-1', status: 'completed' }, { name: '$compensate:hold', status: 'completed' }],
    });
  });

  it('continues a long loop in a new instance, started from the last step of each generation', async () => {
    @Workflow('daily-digest')
    class DailyDigest {
      constructor(
        @Inject(World) private readonly world: World,
        @Inject(WorkflowClient) private readonly workflowClient: WorkflowClient,
      ) {}

      async run(ctx: WorkflowContext, input: { subscriber: string; generation: number }) {
        for (let day = 1; day <= 3; day++) {
          await ctx.step(`send-${day}`, ({ idempotencyKey }) => this.world.record('digest', idempotencyKey));
          await ctx.sleep(`wait-${day}`, '1d');
        }
        // The next generation starts with a fresh journal; a retried step gets it back instead of a second one.
        const next = { subscriber: input.subscriber, generation: input.generation + 1 };
        return ctx.step('continue', () =>
          this.workflowClient.start(DailyDigest, next, { id: `digest-${input.subscriber}-${next.generation}` }).then(({ id }) => id),
        );
      }
    }

    const node = await start([DailyDigest], { journal: { maxEntries: 8 } });
    await node.client.start(DailyDigest, { subscriber: 'u42', generation: 1 }, { id: 'digest-u42-1' });
    await runDays(node, 7);

    expect(await node.client.getStatus('digest-u42-1')).toMatchObject({ status: 'completed', output: 'digest-u42-2' });
    expect(await node.client.getStatus('digest-u42-2')).toMatchObject({ status: 'completed', output: 'digest-u42-3' });
    expect(await node.client.getStatus('digest-u42-3')).toMatchObject({ status: 'suspended' });
    expect(world.count('digest')).toBe(8); // 3 + 3 + 2 so far
  });

  it('rejects a limit that is not a positive number at startup', async () => {
    await expect(start([Reminders], { journal: { maxEntries: 0 } })).rejects.toThrow(
      new TypeError('journal.maxEntries (0) must be a positive number, or Infinity to turn the check off.'),
    );
  });
});
