import { Logger } from '@nestjs/common';
import type { WorkflowPayloadContext } from '../interfaces/workflow-payload-codec.interface.js';
import { isEncodedPayload as encoded, type PayloadCodecs } from '../core/codecs/payload-codecs.js';
import { andThen, toPromise, type Maybe } from '../core/utils/maybe.util.js';
import type { WorkflowScheduleMethods } from '../utils/schedule-store.util.js';
import { storableText, type TextCheck, type TextPlace } from '../utils/storable-text.util.js';
import type { SerializedWorkflowError } from '../interfaces/serialized-workflow-error.interface.js';
import type { WorkflowInstance, WorkflowJournalEntry, WorkflowStatus } from '../interfaces/workflow-instance.interface.js';
import type {
  NewWorkflowInstance,
  NewWorkflowSignal,
  WorkflowCancelRequest,
  WorkflowClaim,
  WorkflowClaimRequest,
  WorkflowInstanceDetails,
  WorkflowListQuery,
  WorkflowPurgeQuery,
  WorkflowPurgeResult,
  WorkflowReopen,
  WorkflowSignalQuery,
  WorkflowSignalRecord,
  WorkflowSignalResult,
  WorkflowStore,
  WorkflowTextColumn,
  WorkflowWrite,
} from '../interfaces/workflow-store.interface.js';

/** @internal A `WorkflowStore` without its schedules, which the engine reaches through the core's `Scheduler`. */
export type WorkflowInstanceStore = Omit<WorkflowStore, WorkflowScheduleMethods>;

/**
 * @internal The store as the engine sees it with a codec: payloads encoded on the way in, decoded on the way out, so
 * the store sees only what the codec returns, and the engine only plain values. Ids, names, keys, statuses and
 * times pass as they are: the store matches on them. Schedules aren't here: the `Scheduler` encodes their inputs.
 */
export class EncodedWorkflowStore implements WorkflowInstanceStore {
  private static readonly logger = new Logger('Workflows');
  private readonly unreadable = new Set<string>();
  readonly createInTransaction?: WorkflowStore['createInTransaction'];
  readonly signalInTransaction?: WorkflowStore['signalInTransaction'];
  readonly checkKey?: WorkflowStore['checkKey'];

  constructor(
    /** The store itself: what it holds, undecoded. */
    readonly inner: WorkflowStore,
    readonly codecs: PayloadCodecs<WorkflowPayloadContext>,
  ) {
    // Only when the store has them: the client tells a store that can't join transactions by their absence.
    // Nothing is awaited before the store's call when the codec is synchronous, nor after it when the store returns at
    // once and there is nothing to decode (see WorkflowClient.start()).
    if (typeof inner.createInTransaction === 'function') {
      this.createInTransaction = (transaction, instance) =>
        this.instanceOf(andThen(this.encodeNew(instance), (encoded) => inner.createInTransaction!(transaction, encoded)));
    }
    if (typeof inner.signalInTransaction === 'function') {
      this.signalInTransaction = (transaction, signal) => andThen(this.encodeSignal(signal), (encoded) => inner.signalInTransaction!(transaction, encoded));
    }
    // Keys aren't encoded: the store checks them as they are.
    if (typeof inner.checkKey === 'function') {
      this.checkKey = (kind, value) => inner.checkKey!(kind, value);
    }
  }

  /** Whether a codec encodes what is written. */
  get encodes(): boolean {
    return this.codecs.encodes;
  }

  /**
   * What the store can't keep of a text at `place` (`WorkflowStore.unstorableText()`), or `undefined` where it keeps
   * any: a store without the method, and a payload or a cancel reason a codec encodes, which the store sees encoded.
   */
  readonly textCheck = (place: TextPlace): TextCheck | undefined => {
    const inner = this.inner;
    if (typeof inner.unstorableText !== 'function' || (this.encodes && (place === 'payload' || place === 'cancelReason'))) {
      return undefined;
    }
    const column: WorkflowTextColumn = place === 'payload' || place === 'name' ? 'json' : 'text';
    return (text) => inner.unstorableText!(text, column);
  };

  create(instance: NewWorkflowInstance) {
    return toPromise(this.instanceOf(andThen(this.encodeNew(instance), (encoded) => this.inner.create(encoded))));
  }

  async get(id: string, options?: { journal?: boolean }): Promise<WorkflowInstanceDetails | null> {
    const details = await this.inner.get(id, options);
    if (!details) {
      return null;
    }

    const decoded = await this.decodeInstance(details);
    if (details.journal?.some(entryEncoded)) {
      decoded.journal = await Promise.all(details.journal.map((entry) => this.decodeEntry(entry, id)));
    }
    return decoded;
  }

  async list(query: WorkflowListQuery): Promise<WorkflowInstance[]> {
    return Promise.all((await this.inner.list(query)).map((instance) => this.decodeInstance(instance)));
  }

  async requestCancel(id: string, request: WorkflowCancelRequest): Promise<boolean> {
    const reason = (await this.codecs.encode(request.reason, { field: 'cancelReason', instanceId: id })) as string | null;
    return this.inner.requestCancel(id, { ...request, reason });
  }

  async reopen(id: string, reopen: WorkflowReopen): Promise<boolean> {
    const [error, entries] = await Promise.all([
      this.codecs.encodeError(this.storableError(reopen.error), { field: 'error', instanceId: id }),
      this.encodeEntries(reopen.entries, id),
    ]);
    return this.inner.reopen(id, { ...reopen, error, entries });
  }

  delete(id: string, statuses: WorkflowStatus[]): Promise<boolean> {
    return this.inner.delete(id, statuses);
  }

  signal(signal: NewWorkflowSignal): Promise<WorkflowSignalResult> {
    return toPromise(andThen(this.encodeSignal(signal), (encoded) => this.inner.signal(encoded)));
  }

  async signals(query: WorkflowSignalQuery): Promise<WorkflowSignalRecord[]> {
    const records = await this.inner.signals(query);
    return Promise.all(
      records.map(async (record) => (encoded(record.payload) ? { ...record, payload: await this.codecs.decode(record.payload, { field: 'payload', signal: record.name }) } : record)),
    );
  }

  purge(query: WorkflowPurgeQuery): Promise<WorkflowPurgeResult> {
    return this.inner.purge(query);
  }

  async claim(request: WorkflowClaimRequest): Promise<WorkflowClaim> {
    const claim = await this.inner.claim(request);
    const instances = await this.readable(claim.instances, (instance) => this.decodeInstance(instance), 'Instance', (instance) =>
      this.handBack(instance.id, request.token, { now: request.now, leaseUntil: request.leaseUntil, signalCursor: claim.lastSignalId }),
    );
    return { ...claim, instances };
  }

  /**
   * Hands a leased instance back untouched, with its waits, due when its lease would have ended: its concurrency slot
   * is free at once, and the instances due before then go first, instead of this one taking the slot at every claim.
   */
  async handBack(id: string, token: string, lease: { now: number; leaseUntil: number; signalCursor: number }): Promise<void> {
    const waits = (await this.inner.get(id))?.waits ?? [];
    await this.inner.write(id, token, {
      now: lease.now,
      entries: [],
      release: { wakeAt: lease.leaseUntil, waits, signalCursor: lease.signalCursor },
    });
  }

  renew(id: string, token: string, leaseUntil: number) {
    return this.inner.renew(id, token, leaseUntil);
  }

  async write(id: string, token: string, write: WorkflowWrite): Promise<boolean> {
    const context = (field: WorkflowPayloadContext['field']): WorkflowPayloadContext => ({ field, instanceId: id });
    const [entries, output, error, customStatus, signal] = await Promise.all([
      this.encodeEntries(write.entries, id),
      this.codecs.encode(write.output, context('output')),
      this.codecs.encodeError(this.storableError(write.error), context('error')),
      this.codecs.encode(write.customStatus, context('customStatus')),
      write.signal ? this.encodeSignal(write.signal) : undefined,
    ]);
    return this.inner.write(id, token, {
      ...write,
      entries,
      ...(write.output === undefined ? {} : { output }),
      ...(write.error === undefined ? {} : { error }),
      ...(write.customStatus === undefined ? {} : { customStatus }),
      ...(signal ? { signal } : {}),
    });
  }

  // ---------------------------------------------------------------- payloads

  /**
   * `error` with what the store can't keep in its text replaced with U+FFFD (`storableText()`), as an error is recorded
   * whatever it holds: its name, and its message and stack unless a codec encodes them.
   */
  private storableError<E extends SerializedWorkflowError | null | undefined>(error: E): E {
    const check = this.textCheck('name');
    if (!check || error === null || error === undefined) {
      return error;
    }

    const text = <T>(value: T): T => (typeof value !== 'string' || this.encodes ? value : (storableText(value, check) as T));
    const { compensation } = error;
    return {
      ...error,
      name: typeof error.name === 'string' ? storableText(error.name, check) : error.name,
      message: text(error.message),
      ...(error.stack === undefined ? {} : { stack: text(error.stack) }),
      ...(compensation === undefined ? {} : { compensation: this.storableError(compensation) }),
    } as E;
  }

  private encodeNew(instance: NewWorkflowInstance): Maybe<NewWorkflowInstance> {
    return andThen(this.codecs.encode(instance.input, { field: 'input', instanceId: instance.id }), (input) => ({ ...instance, input }));
  }

  private encodeSignal(signal: NewWorkflowSignal): Maybe<NewWorkflowSignal> {
    return andThen(this.codecs.encode(signal.payload, { field: 'payload', signal: signal.name }), (payload) => ({ ...signal, payload }));
  }

  private encodeEntries(entries: WorkflowJournalEntry[], instanceId: string): Promise<WorkflowJournalEntry[]> {
    return Promise.all(
      entries.map(async (entry) => {
        const context = (field: WorkflowPayloadContext['field']): WorkflowPayloadContext => ({ field, instanceId, entry: entry.name });
        const [result, progress, data, error] = await Promise.all([
          this.codecs.encode(entry.result, context('result')),
          this.codecs.encode(entry.progress, context('progress')),
          this.codecs.encode(entry.data, context('data')),
          this.codecs.encodeError(this.storableError(entry.error), context('error')),
        ]);
        return { ...entry, result, progress, data, error };
      }),
    );
  }

  private async decodeEntry(entry: WorkflowJournalEntry, instanceId: string): Promise<WorkflowJournalEntry> {
    if (!entryEncoded(entry)) {
      return entry;
    }

    const context = (field: WorkflowPayloadContext['field']): WorkflowPayloadContext => ({ field, instanceId, entry: entry.name });
    const [result, progress, data, error] = await Promise.all([
      this.codecs.decode(entry.result, context('result')),
      this.codecs.decode(entry.progress, context('progress')),
      this.codecs.decode(entry.data, context('data')),
      this.codecs.decodeError(entry.error, context('error')),
    ]);
    // Only the fields the entry has: a store keeps `null` and `undefined` apart, and so does the engine.
    return {
      ...entry,
      ...('result' in entry ? { result } : {}),
      ...('progress' in entry ? { progress } : {}),
      ...('data' in entry ? { data } : {}),
      ...('error' in entry ? { error } : {}),
    };
  }

  /**
   * An instance as stored, decoded; one no listed codec can decode any more, without its payloads (its error's name
   * kept), logged once: a listing shows it instead of failing whole.
   */
  async readableInstance<T extends WorkflowInstance>(instance: T): Promise<T> {
    try {
      return await this.decodeInstance(instance);
    } catch (error) {
      if (!this.unreadable.has(instance.id)) {
        this.unreadable.add(instance.id);
        EncodedWorkflowStore.logger.warn(`Instance "${instance.id}" is listed without its payloads, which can't be read: ${(error as Error).message}`);
      }
      const { name } = instance.error ?? {};
      return { ...instance, input: undefined, output: undefined, customStatus: null, cancelReason: null, error: name === undefined ? null : { name, message: '(unreadable)' } };
    }
  }

  private async decodeInstance<T extends WorkflowInstance>(instance: T): Promise<T> {
    if (!instanceEncoded(instance)) {
      return instance;
    }

    const context = (field: WorkflowPayloadContext['field']): WorkflowPayloadContext => ({ field, instanceId: instance.id });
    const [input, output, error, customStatus, cancelReason] = await Promise.all([
      this.codecs.decode(instance.input, context('input')),
      this.codecs.decode(instance.output, context('output')),
      this.codecs.decodeError(instance.error, context('error')),
      this.codecs.decode(instance.customStatus, context('customStatus')),
      this.codecs.decode(instance.cancelReason, context('cancelReason')),
    ]);
    return { ...instance, input, ...('output' in instance ? { output } : {}), error, customStatus, cancelReason: cancelReason as string | null };
  }

  private instanceOf(stored: Maybe<{ instance: WorkflowInstance; created: boolean }>): Maybe<{ instance: WorkflowInstance; created: boolean }> {
    return andThen(stored, ({ instance, created }) => {
      // At once when there is nothing to decode: with a store that returns at once, start() checks the instance it
      // found under the id before the application's transaction callback returns.
      const decoded = instanceEncoded(instance) ? this.decodeInstance(instance) : instance;
      return andThen(decoded, (readable) => ({ instance: readable, created }));
    });
  }

  /**
   * What a claim leased, decoded. One that can't be (a codec that is gone) is left out, logged, and handed back to
   * be claimed again later, and read once the codec is back, while the others run.
   */
  private async readable<T extends { id: string }>(claimed: T[], decode: (item: T) => Promise<T>, kind: string, handBack: (item: T) => Promise<unknown>): Promise<T[]> {
    const decoded: T[] = [];
    for (const item of claimed) {
      try {
        decoded.push(await decode(item));
      } catch (error) {
        EncodedWorkflowStore.logger.error(`${kind} "${item.id}" can't be read, so it isn't run; it is claimed again later. ${(error as Error).message}`);
        // At worst its lease expires, as if its worker had died.
        await handBack(item).catch(() => undefined);
      }
    }
    return decoded;
  }
}

function instanceEncoded(instance: WorkflowInstance): boolean {
  return [instance.input, instance.output, instance.error?.message, instance.customStatus, instance.cancelReason].some(encoded);
}

function entryEncoded(entry: WorkflowJournalEntry): boolean {
  return encoded(entry.result) || encoded(entry.progress) || encoded(entry.data) || encoded(entry.error?.message);
}
