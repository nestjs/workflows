/**
 * `@nestjs/workflows/core`'s `ResultWaiter`: an outcome settled in this process ends a wait at once, one another
 * process stored is read with a backoff, and a wait gives up past its timeout, when its signal aborts, and when the
 * waiter closes (the application shuts down). results.spec.ts and shutdown.integration.spec.ts cover it through
 * `WorkflowClient.result()`.
 */
import { ResultWaiter, type ResultOutcome } from '../../lib/core/index.js';

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

  describe('with a read in flight', () => {
    /** A store whose reads answer `null` after `ms`, or never, counting them. */
    function slowStore(ms: number | 'never', answer: () => Promise<ResultOutcome<string> | null> = async () => null) {
      const reads: string[] = [];
      const waiter = new ResultWaiter<string>({
        read: (id) => {
          reads.push(id);
          return ms === 'never' ? new Promise(() => undefined) : new Promise((resolve) => setTimeout(resolve, ms)).then(answer);
        },
        timeoutError: (id, timeoutMs) => new Timeout(id, timeoutMs),
      });
      return { waiter, reads };
    }

    async function timed(wait: Promise<unknown>) {
      const started = performance.now();
      const error = await wait.catch((e: unknown) => e);
      return { error, ms: performance.now() - started };
    }

    it('gives up the read past its timeout, with the timeout error', async () => {
      const { waiter } = slowStore('never');
      const { error, ms } = await timed(waiter.wait('report', { timeout: '50ms' }));
      expect(error).toBeInstanceOf(Timeout);
      expect(error).toMatchObject({ id: 'report', timeoutMs: 50 });
      expect(ms).toBeGreaterThanOrEqual(45);
      expect(ms).toBeLessThan(400);
    });

    it("gives up the read when its signal aborts, with the signal's reason", async () => {
      const { waiter } = slowStore('never');
      const controller = new AbortController();
      setTimeout(() => controller.abort(new Error('the client went away')), 30);
      const { error, ms } = await timed(waiter.wait('report', { signal: controller.signal, timeout: '1h' }));
      expect(error).toEqual(new Error('the client went away'));
      expect(ms).toBeLessThan(400);
    });

    it("doesn't read again once its signal aborted during the backoff's sleep", async () => {
      const { waiter, reads } = slowStore(0);
      const controller = new AbortController();
      const result = timed(waiter.wait('report', { signal: controller.signal }));
      await new Promise((resolve) => setTimeout(resolve, 10)); // the first read answered null: the wait sleeps 25 ms
      const readsBefore = reads.length;
      controller.abort(new Error('the client went away'));

      const { error } = await result;
      expect(error).toEqual(new Error('the client went away'));
      expect(reads.length).toBe(readsBefore);
    });

    it('holds to its timeout, also one shorter than a read: 0 rejects on a slow store, even for an outcome stored', async () => {
      const { waiter, reads } = slowStore(40, async () => ({ value: 'ready' }));
      for (const timeout of [0, '10ms'] as const) {
        const { error, ms } = await timed(waiter.wait('report', { timeout }));
        expect(error).toBeInstanceOf(Timeout);
        expect(ms).toBeLessThan(35);
      }
      expect(reads).toEqual(['report', 'report']);

      // A read that answers before the deadline still counts, 0 included: here, one that answers within the same tick.
      await expect(storeOf({ report: { value: 'ready' } }).waiter.wait('report', { timeout: 0 })).resolves.toBe('ready');
    });

    it('still waits for a read with no timeout, and for one with a timeout beyond what timers support', async () => {
      // A timer longer than 2^31 - 1 ms would run after 1 ms: each read would be given up at once.
      for (const timeout of [undefined, '30d'] as const) {
        const { waiter } = slowStore(40, async () => ({ value: 'ready' }));
        await expect(waiter.wait('report', timeout === undefined ? {} : { timeout })).resolves.toBe('ready');
      }
    });

    it('leaves the other waits for the same id reading when one gives up', async () => {
      const { waiter } = slowStore(80, async () => ({ value: 'ready' }));
      const short = timed(waiter.wait('report', { timeout: '20ms' }));
      const long = waiter.wait('report', { timeout: '5s' });

      expect((await short).error).toBeInstanceOf(Timeout);
      await expect(long).resolves.toBe('ready');
    });

    it('ignores a read that fails after the wait gave it up', async () => {
      const unhandled: unknown[] = [];
      const onUnhandled = (reason: unknown) => unhandled.push(reason);
      process.on('unhandledRejection', onUnhandled);
      try {
        const { waiter } = slowStore(40, () => Promise.reject(new Error('connection terminated')));
        await expect(waiter.wait('report', { timeout: '10ms' })).rejects.toBeInstanceOf(Timeout);
        await new Promise((resolve) => setTimeout(resolve, 80));
        expect(unhandled).toEqual([]);
      } finally {
        process.off('unhandledRejection', onUnhandled);
      }
    });
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
});
