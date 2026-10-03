/**
 * `@nestjs/workflows/core`'s `ResultWaiter`: an outcome settled in this process ends a wait at once, one another
 * process stored is read with a backoff, and a wait gives up past its timeout, when its signal aborts, and when the
 * waiter closes (the application shuts down). results.spec.ts and shutdown.integration.spec.ts cover it through
 * `WorkflowClient.result()`.
 */
import { ResultWaiter, type ResultOutcome } from '../../lib/core/index.js';
import { heapUsed } from '../support/heap.js';

class Timeout extends Error {
  constructor(
    readonly id: string,
    readonly timeoutMs: number,
  ) {
    super(`${id} took over ${timeoutMs}ms`);
  }
}

/** A store's outcomes, read as the waiter reads them, counting the reads. */
function storeOf(outcomes: Record<string, ResultOutcome<string> | null>) {
  const reads: string[] = [];
  const waiter = new ResultWaiter<string>({
    read: async (id) => {
      reads.push(id);
      return outcomes[id] ?? null;
    },
    timeoutError: (id, timeoutMs) => new Timeout(id, timeoutMs),
    closedError: (id) => new Error(`closed while waiting for ${id}`),
  });
  return { waiter, reads, outcomes };
}

describe('ResultWaiter', () => {
  it('resolves with a value it reads, and rejects with an error it reads', async () => {
    const { waiter } = storeOf({ done: { value: 'PDF-1' }, failed: { error: new Error('render failed') }, gone: { error: new Error('not found') } });
    await expect(waiter.wait('done')).resolves.toBe('PDF-1');
    await expect(waiter.wait('failed')).rejects.toThrow('render failed');
    await expect(waiter.wait('gone')).rejects.toThrow('not found');
  });

  it('reads again with a backoff until the outcome is stored', async () => {
    const { waiter, reads, outcomes } = storeOf({ report: null });
    const result = waiter.wait('report');
    await new Promise((resolve) => setTimeout(resolve, 100));
    outcomes.report = { value: 'ready' };

    await expect(result).resolves.toBe('ready');
    // 0, 25, 75 (+50) ms, then the read after the 100 ms sleep that follows: a handful, not one per millisecond.
    expect(reads.length).toBeGreaterThanOrEqual(3);
    expect(reads.length).toBeLessThanOrEqual(5);
  });

  it('settles at once with an outcome this process saw, without another read, and a read in flight gives way', async () => {
    const reads: string[] = [];
    const waiter = new ResultWaiter<string>({
      read: (id) => {
        reads.push(id);
        return new Promise(() => undefined); // a store that never answers
      },
    });
    const first = waiter.wait('report');
    const second = waiter.wait('report');
    await Promise.resolve();

    let computed = 0;
    waiter.settle('other', () => {
      computed++;
      return { value: 'unwaited' };
    });
    waiter.settle('report', () => {
      computed++;
      return { value: 'ready' };
    });
    waiter.settle('report', { error: new Error('a later outcome') });

    await expect(first).resolves.toBe('ready');
    await expect(second).resolves.toBe('ready');
    expect(computed).toBe(1);
    expect(reads).toEqual(['report', 'report']);
  });

  it('gives up past its timeout, in wall-clock time, with the timeout error', async () => {
    const { waiter } = storeOf({ slow: null });
    const started = performance.now();
    const error = await waiter.wait('slow', { timeout: '60ms' }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(Timeout);
    expect(error).toMatchObject({ id: 'slow', timeoutMs: 60 });
    expect(performance.now() - started).toBeGreaterThanOrEqual(55);

    await expect(new ResultWaiter<string>({ read: async () => null }).wait('slow', { timeout: 10 })).rejects.toThrow('"slow" didn\'t end within 10ms.');
  });

  it("stops waiting when its signal aborts, with the signal's reason, and doesn't read at all for one already aborted", async () => {
    const { waiter, reads } = storeOf({ slow: null });
    const controller = new AbortController();
    const result = waiter.wait('slow', { signal: controller.signal });
    setTimeout(() => controller.abort(new Error('the client went away')), 30);
    await expect(result).rejects.toThrow('the client went away');

    reads.length = 0;
    await expect(waiter.wait('slow', { signal: AbortSignal.abort(new Error('already')) })).rejects.toThrow('already');
    expect(reads).toEqual([]);
  });

  it('rejects when the read does', async () => {
    const waiter = new ResultWaiter<string>({ read: async () => Promise.reject(new Error('connection terminated')) });
    await expect(waiter.wait('report')).rejects.toThrow('connection terminated');
  });

  it('ends every wait when it closes, without reading again, and answers them before close() resolves', async () => {
    const { waiter, reads } = storeOf({ slow: null });
    const answered: string[] = [];
    const waits = ['a', 'b'].map((id) => waiter.wait(id).catch((error: Error) => void answered.push(error.message)));
    await new Promise((resolve) => setTimeout(resolve, 10));
    const readsBefore = reads.length;

    expect(waiter.closed).toBe(false);
    await waiter.close();
    expect(waiter.closed).toBe(true);
    expect(answered).toEqual(['closed while waiting for a', 'closed while waiting for b']);
    expect(reads.length).toBe(readsBefore);

    await expect(waiter.wait('later')).rejects.toThrow('closed while waiting for later');
    expect(reads).not.toContain('later');
    await Promise.all(waits);
  });

  it('ends a wait that closes while its read is in flight, without waiting for the store', async () => {
    let reads = 0;
    const waiter = new ResultWaiter<string>({
      read: () => (reads++, new Promise(() => undefined)), // a store that never answers
      closedError: (id) => new Error(`closed while waiting for ${id}`),
    });
    const result = waiter.wait('report').catch((error: Error) => error.message);
    await Promise.resolve();
    expect(reads).toBe(1);

    await waiter.close();
    expect(await result).toBe('closed while waiting for report');
    expect(reads).toBe(1);
  });

  it('ends a wait settled during its backoff, without reading again', async () => {
    let reads = 0;
    const waiter = new ResultWaiter<string>({ read: async () => (reads++, null) });
    const result = waiter.wait('report');
    await new Promise((resolve) => setTimeout(resolve, 10)); // mid-backoff: the first read answered null
    expect(reads).toBe(1);

    waiter.settle('report', { value: 'ready' });
    await expect(result).resolves.toBe('ready');
    expect(reads).toBe(1);
  });

  it('answers with the outcome it was settled with, not the closed error, when both land together', async () => {
    const waiter = new ResultWaiter<string>({
      read: () => new Promise(() => undefined),
      closedError: (id) => new Error(`closed while waiting for ${id}`),
    });
    const result = waiter.wait('report');
    await Promise.resolve();

    waiter.settle('report', { value: 'ready' });
    await waiter.close();
    await expect(result).resolves.toBe('ready');
  });

  it('keeps nothing for an id once its wait ended, however it ended', async () => {
    const outcomes: Record<string, ResultOutcome<string> | null> = { done: { value: 'PDF-1' } };
    const waiter = new ResultWaiter<string>({
      read: (id) => {
        if (id === 'broken') return Promise.reject(new Error('connection terminated'));
        if (id === 'exploding') throw new Error('no client in the pool');
        return Promise.resolve(outcomes[id] ?? null);
      },
    });

    await expect(waiter.wait('done')).resolves.toBe('PDF-1');
    await expect(waiter.wait('broken')).rejects.toThrow('connection terminated');
    await expect(waiter.wait('exploding')).rejects.toThrow('no client in the pool');
    await expect(waiter.wait('slow', { timeout: '20ms' })).rejects.toThrow('"slow" didn\'t end within 20ms.');

    // settle() only calls its function when something still waits for the id: nothing does, for any of these.
    let computed = 0;
    for (const id of ['done', 'broken', 'exploding', 'slow']) {
      waiter.settle(id, () => (computed++, { value: 'late' }));
    }
    expect(computed).toBe(0);
  });

  it('gives up once the read in flight answers, when its deadline passed while it was out', async () => {
    let reads = 0;
    const waiter = new ResultWaiter<string>({
      read: () => (reads++, new Promise((resolve) => setTimeout(() => resolve(null), 50))),
    });
    const started = performance.now();

    await expect(waiter.wait('slow', { timeout: '20ms' })).rejects.toThrow('"slow" didn\'t end within 20ms.');
    // The read isn't cancelled: the wait gives up on the loop's next turn, late rather than never.
    expect(reads).toBe(1);
    expect(performance.now() - started).toBeGreaterThanOrEqual(45);
  });

  it('keeps nothing per read while a wait goes on, however long it waits', async () => {
    let reads = 0;
    const waiter = new ResultWaiter<string>({ read: async () => (reads++, null) });
    // Only the backoff's sleeps are faked: the wait reads 20,000 times in a moment, each read answering in turn.
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    const result = waiter.wait('report').catch((error: Error) => error.message);
    /** Lets the wait read `n` more times, or says where it stopped reading. */
    const polls = async (n: number) => {
      const until = reads + n;
      for (let turns = 0; reads < until; turns++) {
        if (turns > n * 2 + 1_000) {
          throw new Error(`the wait stopped reading at ${reads} of ${until}`);
        }
        await new Promise((resolve) => setImmediate(resolve));
        vi.advanceTimersByTime(1_000);
      }
    };

    let kept: number;
    try {
      await polls(100);
      const before = heapUsed();
      await polls(20_000);
      kept = heapUsed() - before;
    } finally {
      vi.useRealTimers();
    }

    await waiter.close();
    expect(await result).toBe('The waiter closed while waiting for the result of "report".');
    // Each read used to race a promise that lived as long as the wait: about 300 bytes a read, 6 MB here.
    expect(kept).toBeLessThan(1_000_000);
  }, 30_000);
});
