/**
 * Waiting for an instance's result: `WorkflowClient.result()` and `startAndWait()`. In this
 * process the worker's events end the wait at once; across processes the store is read again
 * with a backoff.
 */
import {
  ManualWorkflowClock,
  Workflow,
  WorkflowFailedError,
  WorkflowNotFoundError,
  WorkflowResultTimeoutError,
  WorkflowSignal,
  type WorkflowContext,
} from '../lib/index.js';
import { boot, tempDb, waitFor, type Node, type TestDb } from './support.js';

const approval = new WorkflowSignal<{ approved: boolean }>('refund.approval');

@Workflow('refund')
class Refund {
  async run(ctx: WorkflowContext, input: { orderId: string; amount: number }) {
    const decision = await ctx.waitForSignal('approval', approval, { key: input.orderId });
    if (!decision?.approved) {
      ctx.fail(`Refund of ${input.orderId} was declined.`);
    }

    return ctx.step('refund', () => ({ orderId: input.orderId, refunded: input.amount, at: new Date(Date.UTC(2026, 0, 2)) }));
  }
}

@Workflow('quote')
class Quote {
  async run(ctx: WorkflowContext, input: { items: number }) {
    return ctx.step('price', () => input.items * 1_299);
  }
}

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

async function start(options: { worker?: boolean } = {}) {
  const node = await boot({ db, clock, workflows: [Refund, Quote], worker: { enabled: options.worker ?? false, pollInterval: '20ms' } });
  nodes.push(node);
  return node;
}

describe('result()', () => {
  it('resolves with the output, as journaled, once the instance completes in this process', async () => {
    const node = await start();
    await node.client.start(Refund, { orderId: 'o-1', amount: 2_499 }, { id: 'refund-o-1' });
    const result = node.client.result('refund-o-1');
    let settled = false;
    void result.then(() => (settled = true));

    await node.worker.drain();
    await node.client.signal(approval, { approved: true }, { key: 'o-1' });
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(settled).toBe(false);

    await node.worker.drain();
    await expect(result).resolves.toEqual({ orderId: 'o-1', refunded: 2_499, at: '2026-01-02T00:00:00.000Z' });
    // An instance that ended already resolves at once.
    await expect(node.client.result('refund-o-1')).resolves.toMatchObject({ refunded: 2_499 });
  });

  it("rejects with a WorkflowFailedError that carries the instance's status and error", async () => {
    const node = await start();
    await node.client.start(Refund, { orderId: 'o-2', amount: 100 }, { id: 'refund-o-2' });
    await node.client.start(Refund, { orderId: 'o-3', amount: 100 }, { id: 'refund-o-3' });
    const failed = node.client.result('refund-o-2').catch((error: unknown) => error);
    const cancelled = node.client.result('refund-o-3').catch((error: unknown) => error);

    await node.worker.drain();
    await node.client.signal(approval, { approved: false }, { key: 'o-2' });
    await node.client.cancel('refund-o-3', 'The customer kept the cat tree.');
    await node.worker.drain();

    const failure = (await failed) as WorkflowFailedError;
    expect(failure).toBeInstanceOf(WorkflowFailedError);
    expect(failure).toMatchObject({ instanceId: 'refund-o-2', status: 'failed', cause: { name: 'WorkflowFailedError', message: 'Refund of o-2 was declined.' } });
    expect(failure.message).toBe('Instance "refund-o-2" of workflow "refund" failed: WorkflowFailedError: Refund of o-2 was declined.');
    expect(await cancelled).toMatchObject({
      instanceId: 'refund-o-3',
      status: 'cancelled',
      cause: { name: 'WorkflowCancelledError', message: 'The customer kept the cat tree.' },
    });
  });

  it('times out, and leaves the instance running', async () => {
    const node = await start();
    await node.client.start(Refund, { orderId: 'o-4', amount: 100 }, { id: 'refund-o-4' });
    await node.worker.drain();

    const started = performance.now();
    const error = await node.client.result('refund-o-4', { timeout: '80ms' }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(WorkflowResultTimeoutError);
    expect(error).toMatchObject({ instanceId: 'refund-o-4', timeoutMs: 80 });
    expect(performance.now() - started).toBeGreaterThanOrEqual(75);
    expect(await node.client.getStatus('refund-o-4')).toMatchObject({ status: 'suspended' });
  });

  it("doesn't wait for a slow store past its timeout or its signal, a timeout of 0 included", async () => {
    const node = await start();
    await node.client.start(Refund, { orderId: 'o-10', amount: 100 }, { id: 'refund-o-10' });
    const get = node.store.get.bind(node.store);
    // Each read answers after 600 ms; the ones the waits give up are awaited before the node closes.
    const reads: Promise<unknown>[] = [];
    vi.spyOn(node.store, 'get').mockImplementation((...args) => {
      const read = new Promise((resolve) => setTimeout(resolve, 600)).then(() => get(...args));
      reads.push(read);
      return read;
    });

    const started = performance.now();
    const [timedOut, peeked, aborted, startedAndTimedOut] = await Promise.all([
      node.client.result('refund-o-10', { timeout: '80ms' }).catch((e: unknown) => e),
      node.client.result('refund-o-10', { timeout: 0 }).catch((e: unknown) => e),
      node.client.result('refund-o-10', { signal: AbortSignal.timeout(80) }).catch((e: unknown) => e),
      node.client.startAndWait(Refund, { orderId: 'o-11', amount: 1 }, { id: 'refund-o-11' }, { timeout: '80ms' }).catch((e: unknown) => e),
    ]);
    expect(timedOut).toBeInstanceOf(WorkflowResultTimeoutError);
    expect(peeked).toMatchObject({ name: 'WorkflowResultTimeoutError', timeoutMs: 0 });
    expect(aborted).toMatchObject({ name: 'TimeoutError' });
    expect(startedAndTimedOut).toBeInstanceOf(WorkflowResultTimeoutError);
    expect(performance.now() - started).toBeLessThan(400);
    await Promise.allSettled(reads);
  });

  it('stops waiting when its signal aborts, and rejects for an unknown or deleted instance', async () => {
    const node = await start();
    await expect(node.client.result('missing')).rejects.toThrow(WorkflowNotFoundError);

    await node.client.start(Refund, { orderId: 'o-5', amount: 100 }, { id: 'refund-o-5' });
    const controller = new AbortController();
    const aborted = node.client.result('refund-o-5', { signal: controller.signal });
    controller.abort(new Error('The client went away.'));
    await expect(aborted).rejects.toThrow('The client went away.');
    await expect(node.client.result('refund-o-5', { signal: AbortSignal.abort(new Error('Already gone.')) })).rejects.toThrow('Already gone.');

    // Caught at once: a read of the wait may find the instance gone before delete() returns.
    const deleted = node.client.result('refund-o-5').catch((error: unknown) => error);
    await node.client.delete('refund-o-5', { force: true });
    expect(await deleted).toMatchObject({ name: 'WorkflowNotFoundError', message: 'No workflow instance with id "refund-o-5".' });
  });

  it('rejects at once when the application shuts down while it waits, without waiting for its store', async () => {
    const node = await start();
    await node.client.start(Refund, { orderId: 'o-8', amount: 100 }, { id: 'refund-o-8' });
    // The database closes with the application: from now on a read never settles.
    const get = node.store.get.bind(node.store);
    let closing = false;
    let reads = 0;
    vi.spyOn(node.store, 'get').mockImplementation((...args) => {
      reads++;
      return closing ? new Promise<never>(() => undefined) : get(...args);
    });

    const waiting = node.client.result('refund-o-8').catch((error: unknown) => error);
    await waitFor(() => reads === 1);
    closing = true;
    await waitFor(() => reads === 2);
    nodes.splice(nodes.indexOf(node), 1);
    await node.close();
    vi.restoreAllMocks();
    expect(await waiting).toEqual(new Error('The application shut down while waiting for the result of instance "refund-o-8".'));
  });

  it('stops waiting as the shutdown begins, and has its callers answer before the hook returns, as Nest then closes the HTTP server', async () => {
    const node = await start();
    await node.client.start(Refund, { orderId: 'o-9', amount: 100 }, { id: 'refund-o-9' });
    const answers: string[] = [];
    // A route answering some awaits after its wait ended, as through the app's interceptors and filters.
    const answering = (async () => {
      try {
        await node.client.result('refund-o-9', { timeout: '1h' });
      } catch (error) {
        for (let i = 0; i < 50; i++) {
          await null;
        }
        answers.push((error as Error).message);
      }
    })();

    await node.client.beforeApplicationShutdown();
    expect(answers).toEqual(['The application shut down while waiting for the result of instance "refund-o-9".']);
    await answering;

    // A wait that starts from now on rejects at once, without reading the store.
    const get = vi.spyOn(node.store, 'get');
    await expect(node.client.startAndWait(Quote, { items: 1 }, { id: 'quote-late' })).rejects.toThrow(
      'The application shut down while waiting for the result of instance "quote-late".',
    );
    expect(get).not.toHaveBeenCalled();
    vi.restoreAllMocks();
    expect(await node.client.getStatus('quote-late')).toMatchObject({ status: 'pending' });
  });

  it("sees an instance another process runs by reading the store, without that process's events", async () => {
    const api = await start();
    const worker = await start({ worker: true });
    await api.client.start(Refund, { orderId: 'o-6', amount: 700 }, { id: 'refund-o-6' });

    const result = api.client.result('refund-o-6', { timeout: '5s' });
    await api.client.signal(approval, { approved: true }, { key: 'o-6' });
    worker.worker.kick();
    await expect(result).resolves.toMatchObject({ refunded: 700 });
    expect(api.events.map((event) => event.type)).toEqual([]);
  });
});

describe('startAndWait()', () => {
  it('starts an instance and resolves with its output, and waits for an existing one with the same id', async () => {
    const node = await start({ worker: true });
    await expect(node.client.startAndWait(Quote, { items: 3 }, { id: 'quote-1' }, { timeout: '5s' })).resolves.toBe(3_897);
    await expect(node.client.startAndWait(Quote, { items: 3 }, { id: 'quote-1' })).resolves.toBe(3_897);
    expect(node.events.filter((event) => event.type === 'workflow-started')).toHaveLength(1);
  });

  it('takes the run timeout in its options and the wait timeout in its own', async () => {
    const node = await start();
    await expect(node.client.startAndWait(Refund, { orderId: 'o-7', amount: 1 }, { id: 'refund-o-7', timeout: '1h' }, { timeout: '30ms' })).rejects.toThrow(
      WorkflowResultTimeoutError,
    );
    expect(await node.client.getStatus('refund-o-7')).toMatchObject({ status: 'pending', deadline: clock.now() + 3_600_000 });
  });

  it("refuses { transaction }, which the instance wouldn't exist before", async () => {
    const node = await start();
    await expect(node.client.startAndWait(Quote, { items: 1 }, { transaction: {} })).rejects.toThrow(
      "startAndWait() can't take { transaction }: the instance only exists once your transaction commits.",
    );
    expect(await node.client.list()).toEqual([]);
  });
});
