import { Injectable, Logger, type OnApplicationBootstrap, type OnModuleInit } from '@nestjs/common';
import { EventBus, UnhandledExceptionBus, type AsyncContext, type IEvent, type IEventPublisher } from '@nestjs/cqrs';
import { WorkflowClient } from '../services/workflow-client.service.js';
import { canonical } from '../core/utils/canonical.util.js';
import { isPriority, MAX_PRIORITY } from '../core/limits/limits.js';
import { normalize } from '../utils/normalize.util.js';
import { ROUTE_EVENTS, WorkflowRegistry } from '../services/workflow-registry.service.js';
import type { WorkflowDispatcherContext } from './interfaces/workflow-dispatcher-context.interface.js';
import { WorkflowEventsExplorer, type WorkflowEventTargets } from './workflow-events.explorer.js';

/** One `start()` or `signal()` an event asks for. */
interface WorkflowWrite {
  event: object;
  run(transaction: unknown): Promise<unknown>;
}

/**
 * The CQRS `EventBus`'s publisher while `WorkflowsCqrsModule` is imported. It wraps the
 * publisher that was set before it (the in-memory one that feeds `@EventsHandler()`s and
 * sagas, or the one from `CqrsModule.forRoot({ eventPublisher })`): for an event that a
 * workflow maps with `@StartOn()` or `@SignalOn()`, it first starts and signals the workflows,
 * through the transaction in the dispatcher context when there is one, then hands the event to
 * the wrapped publisher. Other events go straight through, synchronously, as before.
 */
@Injectable()
export class WorkflowEventPublisher implements IEventPublisher, OnModuleInit, OnApplicationBootstrap {
  private readonly logger = new Logger('WorkflowsCqrsModule');
  private readonly inner: IEventPublisher;
  private routes?: Map<Function, WorkflowEventTargets>;
  /** Event classes an aggregate's commit() published, warned about once each. */
  private readonly committedByAggregates = new Set<Function>();

  constructor(
    private readonly eventBus: EventBus,
    private readonly unhandledExceptionBus: UnhandledExceptionBus,
    private readonly workflowClient: WorkflowClient,
    private readonly explorer: WorkflowEventsExplorer,
    registry: WorkflowRegistry,
  ) {
    // In the constructor, so no lifecycle hook can publish an event before it's installed.
    this.inner = eventBus.publisher;
    eventBus.publisher = this;
    registry[ROUTE_EVENTS]();
  }

  onModuleInit(): void {
    const routes = this.table();
    // A process that publishes mapped events without registering their workflows starts and
    // signals nothing, and would otherwise do so silently.
    if (routes.size === 0) {
      this.logger.warn(
        'WorkflowsCqrsModule is imported, but no registered workflow maps an event with @StartOn() or @SignalOn(), so ' +
          'events start and signal no workflow in this process. Register the workflow classes in every process that ' +
          'publishes their events, with worker: false in WorkflowsModule.forRoot() if the process runs none.',
      );
      return;
    }

    const described = [...routes].map(([event, { starts, signals }]) => {
      const actions = [
        ...starts.map(({ name }) => `starts ${name}`),
        ...new Set(signals.map(({ route }) => `signals ${route.signal}`)),
      ];
      return `${event.name} ${actions.join(', ')}`;
    });
    this.logger.log(`Events routed to workflows: ${described.join('; ')}`);
  }

  onApplicationBootstrap(): void {
    if (this.eventBus.publisher !== this) {
      throw new Error(
        `WorkflowsCqrsModule: EventBus.publisher was replaced (by ${this.eventBus.publisher?.constructor?.name ?? 'nothing'}) ` +
          'after WorkflowsCqrsModule installed its publisher, so events would no longer start or signal workflows. ' +
          'Set your publisher with CqrsModule.forRoot({ eventPublisher }) instead: WorkflowsCqrsModule wraps it.',
      );
    }
  }

  publish<TEvent extends IEvent>(event: TEvent, dispatcherContext?: unknown, asyncContext?: AsyncContext): unknown {
    if (!this.isRouted(event)) {
      return this.inner.publish(event, dispatcherContext, asyncContext);
    }

    return this.deliver([event], dispatcherContext, () => this.inner.publish(event, dispatcherContext, asyncContext));
  }

  publishAll<TEvent extends IEvent>(events: TEvent[], dispatcherContext?: unknown, asyncContext?: AsyncContext): unknown {
    const forward = (all: TEvent[]) =>
      this.inner.publishAll
        ? this.inner.publishAll(all, dispatcherContext, asyncContext)
        : all.map((event) => this.inner.publish(event, dispatcherContext, asyncContext));
    if (!events?.some((event) => this.isRouted(event))) {
      return forward(events ?? []);
    }

    // Copied before anything is awaited: an aggregate's commit() hands over its own array and
    // empties it as soon as this returns, before the events are forwarded.
    const all = [...events];
    return this.deliver(all, dispatcherContext, async () => {
      const result = forward(all);
      return Array.isArray(result) ? Promise.all(result) : result;
    });
  }

  /**
   * Starts and signals the workflows the events map to, then forwards the events. Nothing is
   * forwarded when a write fails, so a retried publish reaches the handlers once.
   */
  private deliver(events: object[], context: unknown, forward: () => unknown): Promise<unknown> {
    const failure: { event?: object } = {};
    const delivered = this.write(events, transactionOf(context), failure).then(forward);

    // An aggregate's commit() without a dispatcher context: before @nestjs/cqrs 12.1 it drops the
    // promise this returns, and from 12.1 on nothing may await it either. Report its failures the
    // way cqrs reports a failing event handler, rather than as an unhandled rejection. (Given a
    // context, commit() passes it instead of the aggregate, and its caller awaits the result.)
    if (isAggregate(context)) {
      this.warnAboutCommit(events);
      delivered.catch((exception: unknown) => this.report(failure.event ?? events[0], exception));
    }

    return delivered;
  }

  private async write(events: object[], transaction: unknown, failure: { event?: object }): Promise<void> {
    const writes = this.plan(events);

    if (transaction === undefined) {
      for (const write of writes) {
        failure.event = write.event;
        await write.run(undefined);
      }
      failure.event = undefined;
      return;
    }

    // Issued back to back, not awaited in between: a driver whose transactions are synchronous
    // must see every statement before the application's callback returns, and on the
    // transaction's one connection they still run in this order. A write that throws (start()
    // and signal() do in a transaction) is settled with the others, as one that rejects.
    const settled = await Promise.allSettled(writes.map(async (write) => write.run(transaction)));
    const index = settled.findIndex((result) => result.status === 'rejected');
    if (index !== -1) {
      failure.event = writes[index].event;
      throw (settled[index] as PromiseRejectedResult).reason;
    }
  }

  /**
   * Every start and signal, computed before any is written: a mapping function that throws
   * leaves nothing half done. Per event, starts come first, then signals, each signal once.
   */
  private plan(events: object[]): WorkflowWrite[] {
    const writes: WorkflowWrite[] = [];

    for (const event of events) {
      const targets = this.targetsOf(event);
      if (!targets) {
        continue;
      }

      for (const { workflow, name, route } of targets.starts) {
        const id = route.id(event);
        const input = route.input ? route.input(event) : event;
        const priority = startOption(event, name, 'priority', route.priority);
        const concurrencyKey = startOption(event, name, 'concurrencyKey', route.concurrencyKey);
        const rateLimitKey = startOption(event, name, 'rateLimitKey', route.rateLimitKey);
        writes.push({
          event,
          run: (transaction) => this.workflowClient.start(workflow, input, { id, transaction, priority, concurrencyKey, rateLimitKey }),
        });
      }

      // A signal reaches every instance waiting with its name and key, whatever the workflow:
      // two declarations that agree are one signal, and two that disagree are a mistake.
      const sent = new Map<string, { payload: string; id: string | undefined; name: string }>();
      for (const { name, route } of targets.signals) {
        const key = route.key ? route.key(event) : undefined;
        assertString(event, name, 'key', key, 'Waits match keys exactly, so convert it (String(event.orderId)) on both sides.');
        const id = route.id ? route.id(event) : undefined;
        assertString(event, name, 'id', id, 'Derive it from the event, such as event.deliveryId.');

        const payload = route.payload ? route.payload(event) : event;
        const identity = JSON.stringify([route.signal, key ?? null]);
        const json = canonical(normalize(payload) ?? null);
        const earlier = sent.get(identity);
        if (earlier) {
          if (earlier.payload !== json || earlier.id !== id) {
            throw new Error(
              `${event.constructor.name} maps to two different ${earlier.payload !== json ? 'payloads' : 'ids'} for the signal "${route.signal}"` +
                `${key === undefined ? '' : ` (key "${key}")`}: @SignalOn() on ${earlier.name} and on ${name}. ` +
                'A signal reaches every instance waiting for it: make the declarations agree, or keep one.',
            );
          }
          continue;
        }

        sent.set(identity, { payload: json, id, name });
        writes.push({ event, run: (transaction) => this.workflowClient.signal(route.signal, payload, { key, id, transaction }) });
      }
    }

    return writes;
  }

  /**
   * The one lossy path this publisher can recognize: an aggregate merged with `EventPublisher`
   * passes itself as the dispatcher context when `commit()` is given none. (`@Publishable()`
   * aggregates pass nothing, like any other untransacted publish, so they can't be told apart.)
   * A warning, not an error: the start or signal still happens, only outside the transaction.
   */
  private warnAboutCommit(events: object[]): void {
    for (const event of events) {
      if (!this.isRouted(event) || this.committedByAggregates.has(event.constructor)) {
        continue;
      }

      this.committedByAggregates.add(event.constructor);
      this.logger.warn(
        `${event.constructor.name} starts or signals workflows, and an aggregate's commit() published it without ` +
          'a transaction: the start or signal is written outside your transaction, so it outlives a rollback of your ' +
          "writes, and a crash before it's written loses it. Pass your transaction and await the result: " +
          'await aggregate.commit({ transaction }) (@nestjs/cqrs 12.1 or later). With older versions, publish the ' +
          "aggregate's events with eventBus.publishAll(aggregate.getUncommittedEvents(), { transaction }), then call " +
          'aggregate.uncommit(). (Logged once per event class.)',
      );
    }
  }

  private report(event: object, exception: unknown): void {
    this.logger.error(
      `Publishing ${event.constructor.name} from an aggregate's commit() without a transaction failed. To handle ` +
        'the failure in your command handler, and roll its transaction back, await aggregate.commit({ transaction }) ' +
        "(@nestjs/cqrs 12.1 or later). With older versions, await eventBus.publishAll(aggregate.getUncommittedEvents(), " +
        '{ transaction }), then call aggregate.uncommit().',
      exception instanceof Error ? exception.stack : String(exception),
    );
    this.unhandledExceptionBus.publish({ cause: event, exception });
  }

  private isRouted(event: unknown): boolean {
    return this.targetsOf(event) !== undefined;
  }

  private targetsOf(event: unknown): WorkflowEventTargets | undefined {
    if (event === null || typeof event !== 'object') {
      return undefined;
    }
    return this.table().get(event.constructor);
  }

  /** Built at `onModuleInit`, or at the first publish if another module's hook publishes earlier. */
  private table(): Map<Function, WorkflowEventTargets> {
    return (this.routes ??= this.explorer.explore());
  }
}

/**
 * A `@StartOn()` option for the event: the value it was given, or what its function returns, checked here, before
 * anything is written, rather than by `start()` once earlier writes of the same publish have landed.
 */
function startOption<T extends number | string>(
  event: object,
  workflow: string,
  option: 'priority' | 'concurrencyKey' | 'rateLimitKey',
  given: T | ((event: object) => T | undefined) | undefined,
): T | undefined {
  const value: unknown = typeof given === 'function' ? given(event) : given;
  if (value === undefined) {
    return undefined;
  }

  const where = `@StartOn(${event.constructor.name}) on ${workflow}: \`${option}\` returned`;
  if (option === 'priority') {
    if (!isPriority(value)) {
      throw new TypeError(`${where} ${JSON.stringify(value)}. Return an integer from 1 (first) to ${MAX_PRIORITY}, or undefined for none.`);
    }
  } else if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError(
      `${where} ${value === '' ? 'an empty string' : `${typeof value}, not a string`}. Return a non-empty string, or undefined for the computed one.`,
    );
  }
  return value as T;
}

/** A key may be empty (it matches waits for ''), an id may not (WorkflowClient.signal() refuses it). */
function assertString(event: object, workflow: string, option: 'key' | 'id', value: unknown, advice: string): void {
  if (value !== undefined && (typeof value !== 'string' || (option === 'id' && value.length === 0))) {
    throw new TypeError(
      `@SignalOn(${event.constructor.name}) on ${workflow}: \`${option}\` returned ${value === '' ? 'an empty string' : `${typeof value}, not a string`}. ${advice}`,
    );
  }
}

/** The dispatcher context's `transaction`. An aggregate (what `mergeObjectContext()` passes) never has one. */
function transactionOf(context: unknown): unknown {
  if (context === null || typeof context !== 'object' || isAggregate(context)) {
    return undefined;
  }
  return (context as Partial<WorkflowDispatcherContext>).transaction;
}

function isAggregate(context: unknown): boolean {
  return typeof (context as { getUncommittedEvents?: unknown } | null | undefined)?.getUncommittedEvents === 'function';
}
