/**
 * `@nestjs/workflows/core`'s `LeasedWorker`: claims as many items as it has free slots, renews their leases on a
 * heartbeat, aborts an item's signal when its lease is lost or the worker shuts down, drains on demand, and shuts down
 * gracefully. worker.spec.ts, leases.spec.ts and recovery.spec.ts cover it as the workflow engine's worker.
 */
import { hostname } from 'node:os';
import { LeasedWorker, ManualClock, type LeasedRun, type LeasedWorkerOptions, type LeaseRequest } from '../../lib/core/index.js';

interface Job {
  id: string;
  token: string;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** A queue of due jobs, leased under the tokens its claims hand out; what the worker did to it is recorded. */
class JobQueue {
  readonly due: string[] = [];
  readonly leases = new Map<string, { token: string; until: number }>();
  readonly claims: LeaseRequest[] = [];
  readonly renewals: Array<{ id: string; until: number }> = [];

  add(...ids: string[]): void {
    this.due.push(...ids);
  }

  async claim(lease: LeaseRequest, limit: number): Promise<Job[]> {
    this.claims.push(lease);
    return this.due.splice(0, limit).map((id) => {
      this.leases.set(id, { token: lease.token, until: lease.until });
      return { id, token: lease.token };
    });
  }

  async renew(job: Job, until: number): Promise<boolean> {
    this.renewals.push({ id: job.id, until });
    const lease = this.leases.get(job.id);
    if (lease?.token !== job.token) {
      return false;
    }
    lease.until = until;
    return true;
  }
}

/** A renewal `heldUp()` holds: its index in `calls`, and how to let it answer. */
interface HeldRenewal {
  index: number;
  /** Answers as the queue would then (no argument), with `held`, or by throwing `error`. */
  answer(outcome?: boolean | Error): void;
}

/**
 * A `renew` over `queue` whose next call `holdNext()` holds up until it's answered, as a lock its row waits on would
 * hold it; `calls` records each renewal's `until` and how many were in flight with it.
 */
function heldUp(queue: JobQueue) {
  const calls: Array<{ until: number; inFlight: number }> = [];
  let inFlight = 0;
  let next: ((held: HeldRenewal) => void) | null = null;
  const renew = async (job: Job, until: number): Promise<boolean> => {
    inFlight += 1;
    calls.push({ until, inFlight });
    try {
      if (next) {
        const asked = next;
        next = null;
        const outcome = await new Promise<boolean | Error | undefined>((answer) => asked({ index: calls.length - 1, answer }));
        if (outcome instanceof Error) {
          throw outcome;
        }
        return outcome ?? (await queue.renew(job, until));
      }
      return await queue.renew(job, until);
    } finally {
      inFlight -= 1;
    }
  };
  return {
    renew,
    calls,
    /** Holds up the next renewal: resolves once it was asked for. */
    holdNext: () => new Promise<HeldRenewal>((resolve) => (next = resolve)),
  };
}

function workerOn(queue: JobQueue, execute: LeasedWorkerOptions<Job>['execute'], options: Partial<LeasedWorkerOptions<Job>> = {}) {
  return new LeasedWorker<Job>({
    concurrency: 2,
    pollInterval: '20ms',
    leaseDuration: '90ms',
    heartbeatInterval: '30ms',
    shutdownTimeout: '200ms',
    claim: (lease, limit) => queue.claim(lease, limit),
    renew: (job, until) => queue.renew(job, until),
    execute,
    ...options,
  });
}

describe('LeasedWorker', () => {
  it('drains due items, as many at once as its concurrency, round after round, with what produce() makes due', async () => {
    const queue = new JobQueue();
    queue.add('a', 'b', 'c');
    const ran: string[] = [];
    let running = 0;
    let most = 0;
    let produced = 0;
    const worker = workerOn(
      queue,
      async (job) => {
        most = Math.max(most, ++running);
        await sleep(5);
        running--;
        ran.push(job.id);
      },
      {
        produce: async () => {
          if (produced++ === 1) {
            queue.add('d');
            return 1;
          }
          return 0;
        },
      },
    );

    expect(await worker.drain()).toBe(4);
    expect(ran.sort()).toEqual(['a', 'b', 'c', 'd']);
    expect(most).toBe(2);
    expect(queue.claims.map((claim) => claim.owner)).toEqual(Array(queue.claims.length).fill(worker.owner));
    expect(await worker.drain({ maxRounds: 1 })).toBe(0);
  });

  it('leases each claim under a new token until now plus the lease, from its clock', async () => {
    const clock = new ManualClock(1_000_000);
    const queue = new JobQueue();
    queue.add('a');
    await workerOn(queue, async () => undefined, { clock, owner: 'pod-1' }).drain();
    queue.add('b');
    await workerOn(queue, async () => undefined, { clock, owner: 'pod-1' }).drain();

    expect(queue.claims[0]).toMatchObject({ owner: 'pod-1', now: 1_000_000, until: 1_000_090 });
    expect(queue.claims[0]!.token).not.toBe(queue.claims[2]!.token);
  });

  it('renews a running item on its heartbeat, and aborts its signal when a renewal finds the lease gone', async () => {
    const queue = new JobQueue();
    queue.add('slow');
    let run!: LeasedRun;
    const worker = workerOn(queue, async (job, leased) => {
      run = leased;
      await sleep(80);
      queue.leases.set(job.id, { token: 'someone-else', until: Infinity });
      await new Promise((resolve) => leased.signal.addEventListener('abort', resolve, { once: true }));
    });

    await worker.drain();
    expect(queue.renewals.filter((renewal) => renewal.id === 'slow').length).toBeGreaterThanOrEqual(2);
    expect(run.leaseLost).toBe(true);
    expect(run.signal.reason).toEqual(new Error('The lease was lost: another worker may run this item now.'));

    // No more renewals once it's lost.
    const renewals = queue.renewals.length;
    await sleep(70);
    expect(queue.renewals.length).toBe(renewals);
  });

  it("keeps a lease a renewal failed to reach, marks it lost at once when told, and renews on the item's own call", async () => {
    const queue = new JobQueue();
    queue.add('a');
    let failures = 0;
    const renew = async (job: Job, until: number) => {
      if (failures++ < 2) {
        throw new Error('connection reset');
      }
      return queue.renew(job, until);
    };
    const seen: boolean[] = [];
    const worker = workerOn(
      queue,
      async (_job, run) => {
        seen.push(await run.renew(), await run.renew(), run.leaseLost);
        seen.push(await run.renew());
        run.loseLease();
        seen.push(run.leaseLost, run.signal.aborted, await run.renew());
      },
      { renew },
    );

    await worker.drain();
    expect(seen).toEqual([true, true, false, true, true, true, false]);
  });

  it('renews an item one renewal at a time: what asks while one is held up waits, and one more goes out once it answered', async () => {
    const clock = new ManualClock(1_000_000);
    const queue = new JobQueue();
    queue.add('slow');
    const renewals = heldUp(queue);
    let run!: LeasedRun;
    let finish!: () => void;
    const worker = workerOn(
      queue,
      async (_job, leased) => {
        run = leased;
        await new Promise<void>((resolve) => (finish = resolve));
      },
      { clock, leaseDuration: '1h', heartbeatInterval: '10ms', renew: renewals.renew },
    );

    const draining = worker.drain();
    await vi.waitFor(() => expect(run).toBeDefined());
    // The next renewal is held up, as a lock would hold it: the clock moves meanwhile.
    const held = await renewals.holdNext();
    clock.advance(5_000);
    // The item's own call and ten heartbeats meanwhile send nothing.
    let renewed: boolean | undefined;
    const renewing = run.renew().then((answer) => (renewed = answer));
    await sleep(100);
    expect([renewals.calls.length, renewed]).toEqual([held.index + 1, undefined]);

    // Once it answered, one more goes out at once for all of them, its until read then; the call waits for it.
    const following = renewals.holdNext();
    held.answer();
    const follow = await following;
    await sleep(30);
    expect([follow.index, renewals.calls.length, renewed]).toEqual([held.index + 1, held.index + 2, undefined]);
    expect(renewals.calls[follow.index]!.until - renewals.calls[held.index]!.until).toBe(5_000);
    follow.answer();
    await renewing;
    expect(renewed).toBe(true);
    expect(Math.max(...renewals.calls.map((call) => call.inFlight))).toBe(1);

    finish();
    await draining;
  });

  it('sends no renewal after one held up once the lease was lost, marked lost, the run ended or was detached', async () => {
    for (const ending of ['lost', 'marked lost', 'ended', 'detached'] as const) {
      const queue = new JobQueue();
      queue.add('slow');
      const renewals = heldUp(queue);
      let run!: LeasedRun;
      let finish!: () => void;
      const worker = workerOn(
        queue,
        async (_job, leased) => {
          run = leased;
          // Its signal ignored.
          await new Promise<void>((resolve) => (finish = resolve));
        },
        { heartbeatInterval: '10ms', shutdownTimeout: '20ms', renew: renewals.renew },
      );

      const draining = worker.drain();
      await vi.waitFor(() => expect(run).toBeDefined());
      const held = await renewals.holdNext();
      // The item's own call, and heartbeats, ask for another meanwhile.
      const renewing = run.renew();
      await sleep(30);
      if (ending === 'lost') {
        held.answer(false);
      } else {
        if (ending === 'marked lost') {
          run.loseLease();
        } else if (ending === 'ended') {
          finish();
          await draining;
        } else {
          await worker.shutdown();
        }
        held.answer(true);
      }
      await sleep(30);
      expect([ending, renewals.calls.length, await renewing]).toEqual([ending, held.index + 1, ending === 'ended' || ending === 'detached']);
      finish();
      await draining;
    }
  });

  it('renews again after a renewal held up fails: the one asked for meanwhile goes out, and the lease is kept', async () => {
    const queue = new JobQueue();
    queue.add('slow');
    const renewals = heldUp(queue);
    let run!: LeasedRun;
    let finish!: () => void;
    const worker = workerOn(
      queue,
      async (_job, leased) => {
        run = leased;
        await new Promise<void>((resolve) => (finish = resolve));
      },
      { heartbeatInterval: '10ms', renew: renewals.renew },
    );

    const draining = worker.drain();
    await vi.waitFor(() => expect(run).toBeDefined());
    const held = await renewals.holdNext();
    const renewing = run.renew();
    const following = renewals.holdNext();
    // The store dropped the connection: a failure, which says nothing of the lease.
    held.answer(new Error('connection reset'));
    const follow = await following;
    expect(follow.index).toBe(held.index + 1);
    follow.answer();
    expect(await renewing).toBe(true);
    expect(run.leaseLost).toBe(false);
    finish();
    await draining;
  });

  it('polls, claims what a kick() or a finished item made due at once, and never runs more than its concurrency', async () => {
    const queue = new JobQueue();
    let running = 0;
    let most = 0;
    const done: string[] = [];
    const worker = workerOn(
      queue,
      async (job) => {
        most = Math.max(most, ++running);
        await sleep(15);
        running--;
        done.push(job.id);
      },
      { pollInterval: '10s' },
    );
    worker.start();
    await sleep(10);

    queue.add('a', 'b', 'c', 'd', 'e');
    worker.kick();
    await vi.waitFor(() => expect(done).toHaveLength(5), { timeout: 2_000 });
    expect(most).toBe(2);
    await worker.shutdown();
  });

  it("aborts running items' signals at shutdown, waits for them, and starts what a claim in flight claimed told to stop", async () => {
    const queue = new JobQueue();
    queue.add('running');
    const signals: Array<{ id: string; aborted: boolean; lost: boolean }> = [];
    let releaseClaim!: () => void;
    let claimStarted!: () => void;
    const started = new Promise<void>((resolve) => (claimStarted = resolve));
    let claims = 0;
    const worker = workerOn(
      queue,
      async (job, run) => {
        if (job.id === 'running') {
          await new Promise((resolve) => run.signal.addEventListener('abort', resolve, { once: true }));
        }
        signals.push({ id: job.id, aborted: run.signal.aborted, lost: run.leaseLost });
      },
      {
        concurrency: 1,
        pollInterval: '10ms',
        claim: async (lease, limit) => {
          if (++claims === 2) {
            // Held until the shutdown began: what it claims starts after it, told to stop.
            claimStarted();
            await new Promise<void>((resolve) => (releaseClaim = resolve));
            queue.add('late');
          }
          return queue.claim(lease, limit);
        },
      },
    );

    const first = worker.drain({ maxRounds: 1 });
    await sleep(5);
    const second = worker.drain({ maxRounds: 1 });
    await started;
    const stopping = worker.shutdown();
    releaseClaim();
    await stopping;
    await Promise.all([first, second]);

    expect(signals).toEqual(
      expect.arrayContaining([
        { id: 'running', aborted: true, lost: false },
        { id: 'late', aborted: true, lost: false },
      ]),
    );
    expect(await worker.drain()).toBe(0);
  });

  it('detaches what outlives the shutdown timeout: no more renewals', async () => {
    const queue = new JobQueue();
    queue.add('stuck');
    let run!: LeasedRun;
    const worker = workerOn(
      queue,
      async (_job, leased) => {
        run = leased;
        await new Promise(() => undefined);
      },
      { shutdownTimeout: '40ms' },
    );

    void worker.drain();
    await sleep(5);
    const started = performance.now();
    await worker.shutdown();
    expect(performance.now() - started).toBeGreaterThanOrEqual(35);
    expect(run.detached).toBe(true);
    expect(run.signal.aborted).toBe(true);
    expect(await run.renew()).toBe(true);

    const renewals = queue.renewals.length;
    await sleep(70);
    expect(queue.renewals.length).toBe(renewals);
    expect(worker.running.map((job) => job.id)).toEqual(['stuck']);
  });

  // Node runs a timer longer than 2^31 - 1 ms (about 24.8 days) after 1 ms instead.
  it('waits at most as long as a timer can for a heartbeat or a shutdown longer than that, instead of not at all', async () => {
    const queue = new JobQueue();
    queue.add('slow');
    let finished = false;
    const worker = workerOn(
      queue,
      async () => {
        await sleep(60);
        finished = true;
      },
      { leaseDuration: '90d', heartbeatInterval: '30d', shutdownTimeout: '30d' },
    );

    void worker.drain();
    await sleep(5);
    await worker.shutdown();
    expect(finished).toBe(true);
    expect(queue.renewals).toEqual([]);
  });

  it('reports what fails in the loop and carries on, while drain() throws a failed claim', async () => {
    const queue = new JobQueue();
    const errors: Array<[string, string]> = [];
    let claims = 0;
    const worker = workerOn(
      queue,
      async (job) => {
        throw new Error(`${job.id} broke`);
      },
      {
        pollInterval: '10ms',
        produce: async () => {
          throw new Error('schedules unreachable');
        },
        claim: async (lease, limit) => {
          if (++claims === 1) {
            throw new Error('store unreachable');
          }
          return queue.claim(lease, limit);
        },
        onError: (error, stage) => void errors.push([stage, (error as Error).message]),
      },
    );
    await expect(worker.drain()).rejects.toThrow('schedules unreachable');

    worker.start();
    queue.add('a');
    await vi.waitFor(() => expect(errors).toContainEqual(['execute', 'a broke']), { timeout: 2_000 });
    await worker.shutdown();
    expect(errors).toEqual(expect.arrayContaining([['produce', 'schedules unreachable'], ['claim', 'store unreachable']]));
  });

  it('refuses settings it can not use, naming them, and names its owner after the host and process by default', () => {
    const claim = async () => [];
    const renew = async () => true;
    const execute = async () => undefined;
    const create = (options: Partial<LeasedWorkerOptions<Job>>) => new LeasedWorker<Job>({ claim, renew, execute, ...options });

    expect(() => create({ concurrency: 0 })).toThrow(new TypeError('worker.concurrency (0) must be a positive integer.'));
    expect(() => create({ name: 'processor', pollInterval: 0 })).toThrow(new TypeError('processor.pollInterval and processor.heartbeatInterval must be positive, such as "1s".'));
    expect(() => create({ leaseDuration: '30s', heartbeatInterval: '30s' })).toThrow(
      new TypeError('worker.heartbeatInterval (30000ms) must be shorter than worker.leaseDuration (30000ms).'),
    );
    expect(() => create({ shutdownTimeout: 'soon' as '1s' })).toThrow('Invalid duration "soon"');

    const worker = create({});
    expect(worker.owner).toMatch(new RegExp(`^${hostname().replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}:${process.pid}:[0-9a-f]{8}$`));
    expect(worker.leaseMs).toBe(30_000);
  });
});
