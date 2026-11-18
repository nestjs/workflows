import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
  type OnModuleDestroy,
} from '@nestjs/common';
import { systemClock } from '../core/time/clock.js';
import type { LeasedRun, LeaseRequest } from '../core/interfaces/leased-worker-options.interface.js';
import { LeasedWorker } from '../core/workers/leased-worker.js';
import type { WorkflowClock } from '../interfaces/workflow-clock.interface.js';
import { WorkflowNonDeterminismError } from '../errors/workflow-non-determinism.error.js';
import { isWorkflowInterrupt } from '../errors/workflow-interrupt.error.js';
import { serializeError } from '../utils/serialize-error.util.js';
import { assertStorableJson } from '../utils/storable-text.util.js';
import type { SerializedWorkflowError } from '../interfaces/serialized-workflow-error.interface.js';
import { resolveRetry } from '../core/retries/retry.js';
import { resolveJournalLimits } from '../utils/journal-limits.util.js';
import { normalize } from '../utils/normalize.util.js';
import { assertSameInstance } from '../utils/new-instance.util.js';
import { CHILD_ENDED_SIGNAL } from '../workflows.constants.js';
import type { WorkflowStatus } from '../interfaces/workflow-instance.interface.js';
import {
  uniqueEntries,
  WorkflowExecution,
  type ChildEnded,
  type ClaimedWorkflowInstance,
  type ExecutionDeps,
  type RunOutcome,
} from './workflow-execution.service.js';
import { WorkflowEvents } from '../events/workflow-events.service.js';
import type { WorkflowEvent } from '../events/workflow-events.interface.js';
import type { EncodedWorkflowStore } from '../storage/encoded-workflow.store.js';
import { ENGINE_STORE, WorkflowStorage } from '../storage/workflow.storage.js';
import type { WorkflowInstanceDetails, WorkflowRelease, WorkflowWrite } from '../interfaces/workflow-store.interface.js';
import { WorkflowRegistry } from './workflow-registry.service.js';
import type { ScheduleProduction } from '../core/interfaces/scheduler-options.interface.js';
import { WorkflowScheduler } from './workflow-scheduler.service.js';
import { WORKFLOWS_MODULE_OPTIONS } from '../workflows.module-definition.js';
import type { WorkflowsModuleOptions } from '../interfaces/workflows-module-options.interface.js';

/** A claimed instance, with the execution that runs it once its journal is read. */
interface Claimed {
  instance: ClaimedWorkflowInstance;
  signalCursor: number;
  exec?: WorkflowExecution;
}

type EventBody = WorkflowEvent extends infer E
  ? E extends WorkflowEvent
    ? Omit<E, 'id' | 'workflow' | 'version' | 'at'>
    : never
  : never;

/**
 * Claims due instances under a lease, executes them and records the outcome.
 * Any number of processes can run a worker against the same store. Inject it
 * for `drain()`, which runs due instances on demand (tests, scripts).
 */
@Injectable()
export class WorkflowWorker implements OnApplicationBootstrap, OnModuleDestroy, OnApplicationShutdown {
  /** Shown as `leaseOwner` on the instances this worker runs. */
  readonly id: string;
  private readonly logger = new Logger('Workflows');
  private readonly clock: WorkflowClock;
  private readonly deps: ExecutionDeps;
  private readonly enabled: boolean;
  /** The claim-execute loop: polling, concurrency, lease renewals, the drain at shutdown. */
  private readonly leased: LeasedWorker<Claimed>;

  constructor(
    private readonly storage: WorkflowStorage,
    @Inject(WORKFLOWS_MODULE_OPTIONS) options: WorkflowsModuleOptions,
    private readonly registry: WorkflowRegistry,
    private readonly events: WorkflowEvents,
    private readonly scheduler: WorkflowScheduler,
  ) {
    const worker = options.worker === false ? { enabled: false } : (options.worker ?? {});

    this.clock = options.clock ?? systemClock;
    this.enabled = worker.enabled ?? true;
    this.leased = new LeasedWorker<Claimed>({
      name: 'worker',
      owner: worker.id,
      concurrency: worker.concurrency,
      pollInterval: worker.pollInterval,
      leaseDuration: worker.leaseDuration,
      heartbeatInterval: worker.heartbeatInterval,
      shutdownTimeout: worker.shutdownTimeout,
      clock: this.clock,
      // Schedules have second precision: their occurrences are started at most once a second, however often it polls.
      produceInterval: '1s',
      produce: async () => this.produced(await this.scheduler.produce(this.id, this.leased.leaseMs)),
      claim: (lease, limit, signal) => this.claim(lease, limit, signal),
      renew: (claimed, until) => this.renew(claimed, until),
      execute: (claimed, run) => this.execute(claimed, run),
      onError: (error, stage) => this.logger.error(FAILED[stage], error as Error),
    });
    this.id = this.leased.owner;

    this.deps = {
      get store() {
        return storage[ENGINE_STORE];
      },
      textCheck: (place) => storage[ENGINE_STORE].textCheck(place),
      resolve: (workflow, version) => registry.resolve(workflow, version),
      createChild: async (child) => {
        const { instance, created } = await storage[ENGINE_STORE].create(child);
        if (created) {
          this.kick();
        } else {
          assertSameInstance(instance, child, { input: true, parent: true });
        }
        return instance;
      },
      clock: this.clock,
      events,
      defaultRetry: resolveRetry(options.retry),
      journalLimits: resolveJournalLimits(options.journal),
    };
  }

  onApplicationBootstrap() {
    this.registry.keys(); // validate definitions at startup
    if (this.enabled) {
      this.leased.start();
    }
  }

  /** Read at each use, never in the constructor: sources register while providers are created. */
  private get store(): EncodedWorkflowStore {
    return this.storage[ENGINE_STORE];
  }

  async onModuleDestroy() {
    await this.shutdown();
  }

  /** Stops the worker if `onModuleDestroy` didn't run (an application closed before it initialized). */
  async onApplicationShutdown() {
    await this.shutdown();
  }

  /** @internal Look for work now instead of at the next poll (after a local start or signal). */
  kick(): void {
    this.leased.kick();
  }

  /**
   * @internal After an accepted `WorkflowClient.cancel()` or `terminate()` in this process: an
   * execution of the instance running here stops at its next `ctx` call now,
   * instead of when its next heartbeat reads the flag.
   */
  noticeCancel(id: string, terminate = false): void {
    for (const { exec } of this.leased.running) {
      if (exec?.instance.id === id) {
        exec.cancelRequested = true;
        exec.terminateRequested ||= terminate;
      }
    }
    this.kick();
  }

  /**
   * Executes every due instance, repeatedly, until none is left that its workflow's concurrency and rate
   * limits let start. Returns the number of executions. For tests (with a `ManualWorkflowClock` and
   * `worker: false`), scripts and cron-driven workers.
   */
  drain(options: { maxRounds?: number } = {}): Promise<number> {
    return this.leased.drain(options);
  }

  /** After a production of the schedules' occurrences: cancels its executions of the instances it cancelled. */
  private produced(production: ScheduleProduction): number {
    for (const id of production.cancelled) {
      this.noticeCancel(id);
    }
    return production.started;
  }

  /**
   * @internal Called on application shutdown. Stops claiming, asks running executions to stop (their steps see
   * `signal.aborted`), and gives claims and executions a shared `shutdownTimeout`. Late claims and detached
   * executions keep their leases until expiry.
   */
  shutdown(): Promise<void> {
    return this.leased.shutdown();
  }

  /** Leases due instances of the workflow versions this process runs, under their limits. */
  private async claim(lease: LeaseRequest, limit: number, signal?: AbortSignal): Promise<Claimed[]> {
    const workflows = this.registry.versions();
    if (workflows.length === 0) {
      return [];
    }

    const { instances, lastSignalId } = await this.store.claim(
      {
        owner: lease.owner,
        token: lease.token,
        now: lease.now,
        leaseUntil: lease.until,
        limit,
        workflows,
        limits: this.registry.limits(),
        rateLimits: this.registry.rateLimits(),
      },
      signal,
    );
    return instances.map((instance) => ({ instance: { ...instance, leaseToken: lease.token }, signalCursor: lastSignalId }));
  }

  /** Extends an instance's lease, and hands its execution the cancel and terminate requests the renewal read. */
  private async renew(claimed: Claimed, until: number): Promise<boolean> {
    const { instance, exec } = claimed;
    const flags = await this.store.renew(instance.id, instance.leaseToken, until);
    if (flags && exec) {
      exec.cancelRequested ||= flags.cancelRequested;
      exec.terminateRequested ||= flags.terminateRequested;
    }
    return flags !== null;
  }

  private async execute(claimed: Claimed, run: LeasedRun): Promise<void> {
    const { instance, signalCursor } = claimed;
    const definition = this.registry.get(instance.workflow, instance.version)!;

    try {
      const details = await this.claimed(instance, signalCursor, run);
      if (run.detached || !details) {
        return;
      }

      const replayOnly = instance.status === 'compensating' || instance.cancelRequested;
      const exec = new WorkflowExecution(instance, details.journal ?? [], signalCursor, this.deps, run, replayOnly);
      claimed.exec = exec;
      // Nothing of it runs any more, not even its compensations.
      if (instance.terminateRequested) {
        return await this.terminate(exec, instance);
      }

      this.emit(instance, instance.runs === 1 ? { type: 'workflow-started' } : { type: 'workflow-resumed', run: instance.runs });
      const outcome = await exec.run(async () => definition.instance.run(exec.context, instance.input));
      await this.conclude(exec, instance, outcome);
    } catch (error) {
      this.logger.error(
        `Executing "${instance.id}" (${instance.workflow}@${instance.version}) failed; it is retried when its lease expires.`,
        error as Error,
      );
    }
  }

  /**
   * The claimed instance with its journal, or `null` when that can't be read (a codec whose key is gone wrote part of
   * the journal, as after a key rotation that was rolled back): then it is handed back, as a claim hands back an
   * instance whose own payloads can't be read, instead of holding its lease and its concurrency slot until the lease
   * expires, and going first at every claim after that. A detached run leaves its lease to expire.
   */
  private async claimed(instance: ClaimedWorkflowInstance, signalCursor: number, run: LeasedRun): Promise<WorkflowInstanceDetails | null> {
    let details: WorkflowInstanceDetails | null;
    try {
      details = await this.store.get(instance.id, { journal: true });
    } catch (error) {
      if (run.detached) {
        return null;
      }
      this.logger.error(`Instance "${instance.id}" can't be read, so it isn't run; it is claimed again later.`, error as Error);
      const lease = { now: this.clock.now(), leaseUntil: instance.leaseUntil!, signalCursor };
      // At worst its lease expires, as if its worker had died.
      await this.store.handBack(instance.id, instance.leaseToken, lease, () => run.detached).catch(() => undefined);
      return null;
    }

    if (!details && !run.detached) {
      throw new Error(`The store has no instance "${instance.id}", which it just leased.`);
    }
    return details;
  }

  private async conclude(exec: WorkflowExecution, instance: ClaimedWorkflowInstance, outcome: RunOutcome): Promise<void> {
    if (await this.interrupted(exec, instance, outcome.ok)) {
      return;
    }

    if (exec.fatal) {
      return this.finish(exec, instance, 'failed', { error: serializeError(exec.fatal) });
    }

    if (instance.status === 'compensating') {
      return this.compensate(exec, instance, instance.error ?? { name: 'Error', message: 'Unknown failure.' }, true);
    }

    if (instance.cancelRequested) {
      return this.compensate(exec, instance, cancelled(instance.cancelReason), false);
    }

    // A cancel noticed during the run (a heartbeat, or cancel() in this process) stopped it.
    // Checked before a suspension: a parked sleep, wait or retry backoff left next to it
    // will never resume, so the instance compensates now instead of parking first and being
    // woken again for that. The same goes for a run timeout.
    if (!outcome.ok && isWorkflowInterrupt(outcome.error) && outcome.error.reason === 'cancel') {
      const current = await this.store.get(instance.id);
      if (current?.terminateRequested) {
        return this.finish(exec, instance, 'cancelled', { error: terminated(current.cancelReason) });
      }
      return this.compensate(exec, instance, cancelled(current?.cancelReason ?? null), false);
    }
    if (!outcome.ok && isWorkflowInterrupt(outcome.error) && outcome.error.reason === 'timeout') {
      return this.compensate(exec, instance, timedOut(instance), false);
    }
    if (exec.journalLimitError) {
      return this.compensate(exec, instance, exec.journalLimitError, false);
    }

    if (exec.suspension) {
      const { waits } = exec.suspension;
      // Parked no later than the run timeout, so a sleep or wait past it can't outlive it.
      const wakeAt = instance.deadline === null ? exec.suspension.wakeAt : Math.min(exec.suspension.wakeAt ?? instance.deadline, instance.deadline);
      const ok = await this.write(exec, instance, {
        entries: exec.drainBuffer(),
        status: 'suspended',
        release: this.release(exec, wakeAt, waits),
      });
      if (ok) {
        this.emit(instance, { type: 'workflow-suspended', wakeAt, waits });
      } else {
        this.leaseLostWarning(instance);
      }
      return;
    }

    if (outcome.ok) {
      const missing = exec.unvisited();
      if (missing.length) {
        const error = new WorkflowNonDeterminismError(
          `Instance "${instance.id}" of workflow "${instance.workflow}@${instance.version}" does not match its journal: ` +
            `${missing.map((n) => `"${n}"`).join(', ')} ${missing.length === 1 ? 'was' : 'were'} recorded by an earlier run but never reached. ` +
            'Ship step removals as a new workflow version.',
        );
        return this.finish(exec, instance, 'failed', { error: serializeError(error) });
      }

      let output: unknown;
      try {
        output = normalize(outcome.value);
        assertStorableJson('Workflow output', output, this.store.textCheck('payload'));
      } catch (error) {
        return this.compensate(exec, instance, serializeError(error), false);
      }

      return this.finish(exec, instance, 'completed', { output });
    }

    return this.compensate(exec, instance, serializeError(outcome.error), false);
  }

  private async compensate(
    exec: WorkflowExecution,
    instance: ClaimedWorkflowInstance,
    reason: SerializedWorkflowError,
    alreadyCompensating: boolean,
  ): Promise<void> {
    if (!alreadyCompensating) {
      const ok = await this.write(exec, instance, { entries: exec.drainBuffer(), status: 'compensating', error: reason });
      if (!ok) {
        return this.leaseLostWarning(instance);
      }
      this.emit(instance, { type: 'workflow-compensating', error: reason });
    }
    // Every execution that compensates closes them again: a crash can't leave one running.
    await this.closeChildren(exec, instance, `its parent "${instance.id}" is compensating (${reason.name}: ${reason.message})`);

    const result = await exec.compensate(reason);
    switch (result.state) {
      case 'done':
        return this.finish(exec, instance, reason.name === 'WorkflowCancelledError' ? 'cancelled' : 'failed', {
          error: reason,
        });
      case 'failed':
        return this.finish(exec, instance, 'compensation_failed', { error: { ...reason, compensation: result.error } });
      case 'suspended': {
        const ok = await this.write(exec, instance, {
          entries: exec.drainBuffer(),
          status: 'compensating',
          release: this.release(exec, result.wakeAt, []),
        });
        if (ok) {
          this.emit(instance, { type: 'workflow-suspended', wakeAt: result.wakeAt, waits: [] });
        }
        return;
      }
      case 'interrupted':
        await this.interrupted(exec, instance, false);
        return;
      case 'terminated':
        return this.terminate(exec, instance);
    }
  }

  /**
   * Applies each child's `parentClose` to the children of this instance that are still running:
   * cancels or terminates them. At least once: it runs again after a crash, and a request that
   * was already accepted is refused, changing nothing.
   */
  private async closeChildren(exec: WorkflowExecution, instance: ClaimedWorkflowInstance, reason: string): Promise<void> {
    if (!exec.hasChildren()) {
      return;
    }

    // Every child, oldest first: the list's order doesn't change while the statuses do.
    for (let offset = 0; ; offset += CHILDREN_PAGE) {
      // Read as stored: closing a child needs no payload, and one no codec can read any more is closed too.
      const children = await this.storage[ENGINE_STORE].inner.list({ parentId: instance.id, limit: CHILDREN_PAGE, offset });
      for (const child of children) {
        if (!RUNNABLE.includes(child.status) || child.parentClose === 'abandon' || child.parentClose === null) {
          continue;
        }

        const terminate = child.parentClose === 'terminate';
        const reasonText = `${terminate ? 'Terminated' : 'Cancelled'}: ${reason}.`;
        if (await this.store.requestCancel(child.id, { reason: reasonText, now: this.clock.now(), terminate })) {
          this.noticeCancel(child.id, terminate);
        }
      }
      if (children.length < CHILDREN_PAGE) {
        return;
      }
    }
  }

  /** Ends the instance as `cancelled` without running (more of) its compensations. */
  private async terminate(exec: WorkflowExecution, instance: ClaimedWorkflowInstance): Promise<void> {
    const current = await this.store.get(instance.id);
    return this.finish(exec, instance, 'cancelled', { error: terminated(current?.cancelReason ?? instance.cancelReason) });
  }

  /**
   * Handles executions that must not record an outcome. Returns true if
   * handled. A run that finished cleanly during shutdown is still recorded;
   * one that was cut short is handed back, and whatever it was doing is
   * re-derived by the next execution.
   */
  private async interrupted(exec: WorkflowExecution, instance: ClaimedWorkflowInstance, finished: boolean): Promise<boolean> {
    if (exec.leaseLost) {
      this.leaseLostWarning(instance);
      return true;
    }

    if (exec.storeError) {
      this.logger.error(
        `Store failed while executing "${instance.id}"; it is retried when its lease expires.`,
        exec.storeError as Error,
      );
      return true;
    }

    if (exec.detached) {
      return true;
    }

    if (exec.shuttingDown && !finished) {
      // Handed back: due at once, for another worker (or this one, after a restart).
      await this.write(exec, instance, {
        entries: [...exec.drainBuffer(), ...exec.rollbacks],
        release: this.release(exec, this.clock.now(), []),
      });
      return true;
    }

    return false;
  }

  private async finish(
    exec: WorkflowExecution,
    instance: ClaimedWorkflowInstance,
    status: 'completed' | 'failed' | 'cancelled' | 'compensation_failed',
    result: { output?: unknown; error?: SerializedWorkflowError },
  ): Promise<void> {
    // Before the instance ends, so a crash in between closes them again on the next execution.
    await this.closeChildren(exec, instance, `its parent "${instance.id}" ended as ${status}`);

    const entries = exec.drainBuffer();
    if (status !== 'completed') {
      entries.push(...exec.abandoned().map((entry) => ({ ...entry, updatedAt: this.clock.now() })));
    }

    // A child tells its parent, in the same transaction: a parent never misses its child's end.
    const ended: ChildEnded = { status, output: result.output, error: result.error ?? null };
    const ok = await this.write(exec, instance, {
      entries,
      status,
      output: result.output,
      error: result.error ?? null,
      release: this.release(exec, null, []),
      ...(instance.parentId === null
        ? {}
        : { signal: { name: CHILD_ENDED_SIGNAL, key: instance.id, dedupeId: instance.id, payload: ended, now: this.clock.now() } }),
    });
    if (!ok) {
      return this.leaseLostWarning(instance);
    }

    if (status === 'completed') {
      this.emit(instance, { type: 'workflow-completed', output: result.output });
    } else if (status === 'cancelled') {
      this.emit(instance, { type: 'workflow-cancelled', error: result.error! });
    } else if (status === 'failed') {
      this.emit(instance, { type: 'workflow-failed', error: result.error! });
    } else {
      this.emit(instance, { type: 'workflow-compensation-failed', error: result.error! });
    }
  }

  /** A fenced write under the execution's lease, stamped with the clock, with the custom status if it changed. */
  private async write(exec: WorkflowExecution, instance: ClaimedWorkflowInstance, write: Omit<WorkflowWrite, 'now'>): Promise<boolean> {
    const change = exec.statusChange();
    const ok = await this.store.write(instance.id, instance.leaseToken, { ...write, ...change, entries: uniqueEntries(write.entries), now: this.clock.now() });
    if (ok) {
      exec.statusWritten(change);
    }
    return ok;
  }

  private release(exec: WorkflowExecution, wakeAt: number | null, waits: WorkflowRelease['waits']): WorkflowRelease {
    return { wakeAt, waits, signalCursor: exec.signalCursor };
  }

  private leaseLostWarning(instance: ClaimedWorkflowInstance): void {
    this.logger.warn(
      `Lost the lease on "${instance.id}" (${instance.workflow}); another worker took over. Nothing from this execution after that point was recorded.`,
    );
  }

  private emit(instance: ClaimedWorkflowInstance, body: EventBody): void {
    this.events.emit({
      id: instance.id,
      workflow: instance.workflow,
      version: instance.version,
      at: this.clock.now(),
      ...body,
    } as WorkflowEvent);
  }
}

/** What the loop logs when a stage fails; it carries on. */
const FAILED: Record<'claim' | 'produce' | 'execute', string> = {
  claim: 'Claiming workflow instances failed.',
  produce: "Starting the schedules' occurrences failed.",
  execute: 'Executing a workflow instance failed.',
};

function cancelled(reason: string | null): SerializedWorkflowError {
  return { name: 'WorkflowCancelledError', message: reason ?? 'Cancelled.' };
}

const RUNNABLE: WorkflowStatus[] = ['pending', 'running', 'suspended', 'compensating'];
const CHILDREN_PAGE = 500;

function terminated(reason: string | null): SerializedWorkflowError {
  return { name: 'WorkflowTerminatedError', message: reason ?? 'Terminated.' };
}

function timedOut(instance: ClaimedWorkflowInstance): SerializedWorkflowError {
  return {
    name: 'WorkflowTimeoutError',
    message:
      `Instance "${instance.id}" of workflow "${instance.workflow}@${instance.version}" did not finish within its timeout ` +
      `(its deadline was ${new Date(instance.deadline!).toISOString()}).`,
  };
}
