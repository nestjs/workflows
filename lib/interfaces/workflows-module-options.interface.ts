import type { ConfigurableModuleAsyncOptions, Type } from '@nestjs/common';
import type { WorkflowPayloadCodec } from './workflow-payload-codec.interface.js';
import type { WorkflowClock } from './workflow-clock.interface.js';
import type { Duration } from '../core/time/duration.js';
import type { WorkflowRetryOptions } from './workflow-retry-options.interface.js';

export interface WorkflowWorkerOptions {
  /**
   * Run the background polling loop. Default true. With false the process can
   * still start, signal and inspect workflows, and `WorkflowWorker.drain()`
   * runs due instances on demand (tests, scripts, a separate worker process).
   */
  enabled?: boolean;
  /** Shown as `leaseOwner`. Default `hostname:pid:random`. */
  id?: string;
  /** Instances executed at once by this process. Default 10. */
  concurrency?: number;
  /** How often to look for due instances. Default 1s; local starts and signals wake the loop at once. */
  pollInterval?: Duration;
  /** How long a claim is valid without a heartbeat. Default 30s. */
  leaseDuration?: Duration;
  /** How often a running instance extends its lease. Default a third of `leaseDuration`. */
  heartbeatInterval?: Duration;
  /**
   * Shared time for in-flight claims and executions at shutdown. Late claims and detached executions keep their
   * leases until expiry. Default 10s.
   */
  shutdownTimeout?: Duration;
}

/**
 * How large an instance's journal may grow. Every execution loads the whole journal and replays
 * `run()` from the top, so a journal that keeps growing (a loop with a step or sleep per round)
 * makes every execution slower. Counts are journal entries (steps, sleeps, waits, commits,
 * `now()`/`random()`/`uuid()` values and compensations), bytes their size as JSON. `Infinity`
 * turns a check off.
 */
export interface WorkflowJournalLimits {
  /** Log a warning, and emit `journal-large`, once, when an instance's journal reaches this many entries. Default 1,000. */
  warnEntries?: number;
  /** The same, at this many bytes. Default 1,000,000 (1 MB). */
  warnBytes?: number;
  /**
   * Fail the instance before it records a new entry once its journal holds this many: it
   * compensates and ends as `failed` with a `WorkflowJournalLimitError`. Compensations may still
   * record theirs. Default 10,000.
   */
  maxEntries?: number;
  /** The same, at this many bytes. Default 10,000,000 (10 MB). */
  maxBytes?: number;
}

/** What `forRoot()` takes, and what `forRootAsync()`'s factory returns. */
export interface WorkflowsModuleOptions {
  /** Default: the system clock. Pass a `ManualWorkflowClock` in tests. */
  clock?: WorkflowClock;
  /** `false` is shorthand for `{ enabled: false }`. */
  worker?: WorkflowWorkerOptions | false;
  /** How large a journal may grow before a warning, and before the instance fails. */
  journal?: WorkflowJournalLimits;
  /** Default step retry: 3 attempts, 1s doubling up to 5m, no jitter. */
  retry?: number | false | WorkflowRetryOptions;
  /**
   * With `NODE_ENV=production`, startup fails when no `WorkflowStore` is registered, because
   * the in-memory default loses every running instance on restart. Set this to run on it
   * anyway (a demo, a single-process tool whose workflows may be lost).
   */
  allowInMemoryStorage?: boolean;
  /**
   * Encodes what workflows store for you before the store sees it (inputs, outputs, step results, checkpoints,
   * signal payloads, custom statuses, schedule inputs, errors' messages), to encrypt it: an `AesGcmPayloadCodec`,
   * your own `WorkflowPayloadCodec`, or its class (Nest creates it, with dependencies from this module's
   * `imports` and from global modules). Several: the first encodes, and each keeps decoding what it encoded, so
   * list the one you replace after the new one. Payloads stored before a codec was set are read as they are.
   */
  codec?: WorkflowPayloadCodec | Type<WorkflowPayloadCodec> | Array<WorkflowPayloadCodec | Type<WorkflowPayloadCodec>>;
}

/** Implemented by a `forRootAsync({ useClass })` class: Nest calls `createWorkflowsOptions()`. */
export interface WorkflowsOptionsFactory {
  createWorkflowsOptions(): WorkflowsModuleOptions | Promise<WorkflowsModuleOptions>;
}

/** What `forRootAsync()` takes: `imports`, `inject` and `useFactory`, `useClass` or `useExisting`, and `isGlobal`. */
export type WorkflowsModuleAsyncOptions = ConfigurableModuleAsyncOptions<WorkflowsModuleOptions, 'createWorkflowsOptions'> & {
  /** Default true. */
  isGlobal?: boolean;
};
