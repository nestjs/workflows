import type { ScheduleMissed, ScheduleOptions, ScheduleOverlap, ScheduleSpec } from '../interfaces/schedule.interface.js';
import { parseDuration } from '../time/duration.js';
import { canonical } from '../utils/canonical.util.js';
import { cronOccurrences, parseCron, type CronExpression } from './cron.util.js';
import { parseRRule, rruleCounts, rruleOccurrences, type RRule } from './rrule.util.js';
import { assertTimeZone } from './time-zone.util.js';

const MISSED: ScheduleMissed[] = ['skip', 'once', 'all'];
const OVERLAP: ScheduleOverlap[] = ['skip', 'allow', 'cancel-previous', 'buffer-one'];

const MAX_COUNT = 10_000;
const MAX_PREVIEW = 1_000;

/** Schedule ids, like workflow names, can't hold the `@` that separates them from the time in an occurrence's run id. */
const SCHEDULE_ID = /^[\w.:-]+$/;

/**
 * Checks a schedule's options and returns its spec: validated, times in milliseconds, defaults applied (`tz: 'UTC'`,
 * `missed: 'skip'`, `overlap: 'skip'`). Throws a `TypeError` that starts with `owner` for anything invalid, with the
 * reason (`0 0 30 2 *` never matches: February has no 30th). Fields it doesn't know are left out: a package adds its
 * own to the spec.
 *
 * ```ts
 * const spec = parseSchedule({ cron: '0 0 8 * * MON', tz: 'Europe/Warsaw', missed: 'once' }, 'Schedule "weekly-digest"');
 * ```
 */
export function parseSchedule(options: ScheduleOptions, owner = 'Schedule'): ScheduleSpec {
  if (options === null || typeof options !== 'object') {
    throw new TypeError(`${owner}: expected an object with cron, every or rrule, got ${JSON.stringify(options)}.`);
  }

  const kinds = (['cron', 'every', 'rrule'] as const).filter((kind) => options[kind] !== undefined);
  if (kinds.length !== 1) {
    throw new TypeError(`${owner}: give exactly one of cron, every and rrule${kinds.length ? `, not ${kinds.join(' and ')}` : ''}.`);
  }

  const tz = options.tz ?? 'UTC';
  assertTimeZone(tz, owner);
  const startAt = instant(owner, 'startAt', options.startAt);
  const endAt = instant(owner, 'endAt', options.endAt);
  if (startAt !== null && endAt !== null && endAt <= startAt) {
    throw new TypeError(`${owner}: endAt (${new Date(endAt).toISOString()}) must be after startAt (${new Date(startAt).toISOString()}).`);
  }

  const timing: Pick<ScheduleSpec, 'cron' | 'every' | 'rrule'> = {};
  if (options.cron !== undefined) {
    if (typeof options.cron !== 'string') {
      throw new TypeError(`${owner}: cron must be a string, such as '0 8 * * MON'.`);
    }
    compiledCron(options.cron);
    timing.cron = options.cron;
  } else if (options.every !== undefined) {
    let every: number;
    try {
      every = parseDuration(options.every);
    } catch (error) {
      throw new TypeError(`${owner}: ${(error as Error).message}`);
    }
    if (every < 1_000) {
      throw new TypeError(`${owner}: every (${JSON.stringify(options.every)}) must be at least a second.`);
    }
    if (options.tz !== undefined) {
      throw new TypeError(
        `${owner}: every repeats a fixed interval from startAt (default: the Unix epoch), whatever the time zone; tz doesn't apply. ` +
          "For a time of day in a time zone, use cron: '0 0 * * *' with tz.",
      );
    }
    timing.every = every;
  } else {
    if (typeof options.rrule !== 'string') {
      throw new TypeError(`${owner}: rrule must be a string, such as 'FREQ=WEEKLY;BYDAY=MO;BYHOUR=8'.`);
    }
    const rule = compiledRRule(options.rrule);
    if (rruleCounts(rule) && startAt === null) {
      throw new TypeError(`${owner}: an rrule with COUNT counts from startAt: give one (or use limit).`);
    }
    // COUNT is counted from DTSTART at every computation of the next occurrence: `limit` counts as it goes.
    if ((rule.count ?? 0) > MAX_COUNT) {
      throw new TypeError(`${owner}: an rrule's COUNT (${rule.count}) can be at most ${MAX_COUNT}. For more, use limit.`);
    }
    timing.rrule = options.rrule;
  }

  const limit = options.limit ?? null;
  if (limit !== null && (!Number.isSafeInteger(limit) || limit < 1)) {
    throw new TypeError(`${owner}: invalid limit ${JSON.stringify(limit)}. Use a positive integer.`);
  }
  const missed = options.missed ?? 'skip';
  if (!MISSED.includes(missed)) {
    throw new TypeError(`${owner}: invalid missed ${JSON.stringify(missed)}. Use 'skip', 'once' or 'all'.`);
  }
  const overlap = options.overlap ?? 'skip';
  if (!OVERLAP.includes(overlap)) {
    throw new TypeError(`${owner}: invalid overlap ${JSON.stringify(overlap)}. Use 'skip', 'allow', 'cancel-previous' or 'buffer-one'.`);
  }
  if (missed === 'all' && overlap !== 'allow') {
    throw new TypeError(
      `${owner}: missed: 'all' starts every missed occurrence at once, so it needs overlap: 'allow'. ` + "For one catch-up run, use missed: 'once'.",
    );
  }

  return { ...timing, tz, startAt, endAt, limit, missed, overlap };
}

/**
 * The times (epoch milliseconds, ascending) of a schedule's next occurrences strictly after `from` (default: now),
 * at most `count` (default 10, at most 1,000) and at most what its `limit` leaves after `runs` occurrences: a
 * schedule's preview. No I/O. Throws a `TypeError` that starts with `owner` for an invalid `count` or `from`.
 *
 * ```ts
 * nextOccurrences(parseSchedule({ cron: '0 9 * * MON-FRI', tz: 'America/New_York' }), { from: Date.UTC(2026, 0, 1), count: 2 });
 * // [Date.UTC(2026, 0, 1, 14), Date.UTC(2026, 0, 2, 14)]: 09:00 EST on Thursday and Friday
 * ```
 */
export function nextOccurrences(spec: ScheduleSpec, options: { from?: Date | number; count?: number; runs?: number } = {}, owner = 'nextOccurrences()'): number[] {
  const count = options.count ?? 10;
  if (!Number.isSafeInteger(count) || count < 1 || count > MAX_PREVIEW) {
    throw new TypeError(`${owner}: count (${count}) must be an integer from 1 to ${MAX_PREVIEW}.`);
  }
  const from = options.from === undefined ? Date.now() : options.from instanceof Date ? options.from.getTime() : options.from;
  if (typeof from !== 'number' || !Number.isFinite(from)) {
    throw new TypeError(`${owner}: invalid from ${String(options.from)}. Pass a valid Date or a timestamp in milliseconds.`);
  }

  const left = spec.limit === null ? Infinity : spec.limit - (options.runs ?? 0);
  const times: number[] = [];
  for (const at of occurrences(spec, from)) {
    if (times.length >= Math.min(count, left)) {
      break;
    }
    times.push(at);
  }
  return times;
}

/**
 * The id of the run an occurrence starts: `<schedule id>@<ISO time of the occurrence>`, such as
 * `weekly-digest@2026-01-05T07:00:00.000Z`, the same wherever and however often it is started, so a second start of
 * an occurrence (by a worker that took over) finds the first.
 *
 * ```ts
 * occurrenceId('weekly-digest', Date.UTC(2026, 0, 5, 7)); // 'weekly-digest@2026-01-05T07:00:00.000Z'
 * ```
 */
export function occurrenceId(schedule: string, at: number): string {
  return `${schedule}@${new Date(at).toISOString()}`;
}

/**
 * Throws a `TypeError` unless `id` can be a schedule's id: letters, digits, `.`, `:`, `_` and `-` (never the `@` of
 * an occurrence's run id). `owner`, when given, is named in the message.
 *
 * ```ts
 * assertScheduleId(id, 'queue "emails"'); // Invalid schedule id "weekly digest" of queue "emails". ...
 * ```
 */
export function assertScheduleId(id: unknown, owner?: string): asserts id is string {
  if (typeof id !== 'string' || !SCHEDULE_ID.test(id)) {
    throw new TypeError(`Invalid schedule id ${JSON.stringify(id)}${owner === undefined ? '' : ` of ${owner}`}. Use letters, digits, ".", ":", "_" or "-".`);
  }
}

/** Whether two specs have the same occurrences (their policies aside). */
export function sameTiming(a: ScheduleSpec, b: ScheduleSpec): boolean {
  const timing = ({ cron, every, rrule, tz, startAt, endAt }: ScheduleSpec) => canonical({ cron, every, rrule, tz, startAt, endAt });
  return timing(a) === timing(b);
}

/** The spec's occurrences strictly after `after`, ascending, within `startAt` and `endAt` (not counting `limit`). */
export function* occurrences(spec: ScheduleSpec, after: number): Generator<number> {
  const from = spec.startAt === null ? after : Math.max(after, spec.startAt - 1);
  let times: Iterable<number>;
  if (spec.cron !== undefined) {
    times = cronOccurrences(compiledCron(spec.cron), from, spec.tz);
  } else if (spec.rrule !== undefined) {
    times = rruleOccurrences(compiledRRule(spec.rrule), spec.startAt, spec.tz, from);
  } else {
    times = everyOccurrences(spec.every!, spec.startAt ?? 0, from);
  }

  for (const at of times) {
    if (spec.endAt !== null && at > spec.endAt) {
      return;
    }
    yield at;
  }
}

/** The first occurrence strictly after `after`, or `null`. */
export function nextOccurrence(spec: ScheduleSpec, after: number): number | null {
  for (const at of occurrences(spec, after)) {
    return at;
  }
  return null;
}

/**
 * The latest `count` occurrences in `[from, to]`, ascending. Looks back from `to` in growing windows, so a
 * schedule that fell far behind costs as much as the occurrences it returns, not all it missed.
 */
export function latestOccurrences(spec: ScheduleSpec, from: number, to: number, count: number): number[] {
  for (let window = 60_000; ; window *= 16) {
    const start = Math.max(from, to - window);
    const found: number[] = [];
    for (const at of occurrences(spec, start - 1)) {
      if (at > to) {
        break;
      }
      found.push(at);
      if (found.length > count) {
        found.shift();
      }
    }
    if (found.length === count || start === from) {
      return found;
    }
  }
}

function* everyOccurrences(every: number, anchor: number, after: number): Generator<number> {
  let at = anchor + (Math.floor((after - anchor) / every) + 1) * every;
  for (;;) {
    yield at;
    at += every;
  }
}

function instant(owner: string, name: string, value: Date | number | undefined): number | null {
  if (value === undefined) {
    return null;
  }

  const ms = value instanceof Date ? value.getTime() : value;
  if (typeof ms !== 'number' || !Number.isFinite(ms)) {
    throw new TypeError(`${owner}: invalid ${name} ${String(value)}. Pass a valid Date or a timestamp in milliseconds.`);
  }
  return ms;
}

const crons = new Map<string, CronExpression>();
const rrules = new Map<string, RRule>();

function compiledCron(expression: string): CronExpression {
  let cron = crons.get(expression);
  if (!cron) {
    crons.set(expression, (cron = parseCron(expression)));
  }
  return cron;
}

function compiledRRule(rule: string): RRule {
  let compiled = rrules.get(rule);
  if (!compiled) {
    rrules.set(rule, (compiled = parseRRule(rule)));
  }
  return compiled;
}
