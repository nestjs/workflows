/**
 * `@nestjs/workflows/core`'s schedule specs: `parseSchedule()` checks a schedule's options (with the reason, naming
 * its owner), `nextOccurrences()` previews it without I/O, and `occurrenceId()` names the run an occurrence starts.
 * cron.spec.ts, rrule.spec.ts and time-zone.spec.ts cover the arithmetic; schedules.spec.ts the engine.
 */
import { assertScheduleId, nextOccurrences, occurrenceId, parseSchedule, type ScheduleOptions } from '../../lib/core/index.js';

const T0 = Date.UTC(2026, 0, 1); // a Thursday
const hours = (n: number) => T0 + n * 3_600_000;

describe('parseSchedule()', () => {
  it('checks the options and applies the defaults, times in milliseconds, leaving out what it does not know', () => {
    expect(parseSchedule({ every: '15m' })).toEqual({ every: 900_000, tz: 'UTC', startAt: null, endAt: null, limit: null, missed: 'skip', overlap: 'skip' });
    expect(
      parseSchedule({
        cron: '0 0 8 * * MON',
        tz: 'Europe/Warsaw',
        startAt: new Date(T0),
        endAt: hours(24 * 365),
        limit: 52,
        missed: 'once',
        overlap: 'buffer-one',
        priority: 3,
      } as ScheduleOptions),
    ).toEqual({ cron: '0 0 8 * * MON', tz: 'Europe/Warsaw', startAt: T0, endAt: hours(24 * 365), limit: 52, missed: 'once', overlap: 'buffer-one' });
    expect(parseSchedule({ rrule: 'FREQ=MONTHLY;BYDAY=-1FR;BYHOUR=17;BYMINUTE=0', tz: 'America/New_York' })).toMatchObject({ rrule: 'FREQ=MONTHLY;BYDAY=-1FR;BYHOUR=17;BYMINUTE=0' });
  });

  it('refuses invalid options with the reason, starting with its owner', () => {
    const owner = 'Schedule "digest" of queue "emails"';
    const cases: Array<[ScheduleOptions, string]> = [
      [{}, `${owner}: give exactly one of cron, every and rrule.`],
      [{ cron: '0 8 * * *', every: '1h' }, `${owner}: give exactly one of cron, every and rrule, not cron and every.`],
      [{ cron: '0 0 30 2 *' }, 'February has no 30th'],
      [{ every: '500ms' }, `${owner}: every ("500ms") must be at least a second.`],
      [{ every: '1h', tz: 'Europe/Warsaw' }, `${owner}: every repeats a fixed interval from startAt`],
      [{ rrule: 'FREQ=DAILY;COUNT=3' }, `${owner}: an rrule with COUNT counts from startAt: give one (or use limit).`],
      [{ rrule: 'FREQ=DAILY;BYSETPOS=1' }, 'BYSETPOS'],
      [{ every: '1h', tz: 'Mars/Olympus' as never }, 'Mars/Olympus'],
      [{ every: '1h', startAt: T0, endAt: T0 }, `${owner}: endAt (2026-01-01T00:00:00.000Z) must be after startAt (2026-01-01T00:00:00.000Z).`],
      [{ every: '1h', limit: 0 }, `${owner}: invalid limit 0. Use a positive integer.`],
      [{ every: '1h', missed: 'sometimes' as never }, `${owner}: invalid missed "sometimes".`],
      [{ every: '1h', overlap: 'queue' as never }, `${owner}: invalid overlap "queue".`],
      [{ every: '1h', missed: 'all' }, `${owner}: missed: 'all' starts every missed occurrence at once, so it needs overlap: 'allow'.`],
    ];
    for (const [options, message] of cases) {
      expect(() => parseSchedule(options, owner)).toThrow(message);
    }
    expect(() => parseSchedule(null as never)).toThrow(new TypeError('Schedule: expected an object with cron, every or rrule, got null.'));
  });
});

describe('nextOccurrences()', () => {
  it('lists the occurrences after from, at most count, in a time zone', () => {
    const weekdays = parseSchedule({ cron: '0 9 * * MON-FRI', tz: 'America/New_York' });
    expect(nextOccurrences(weekdays, { from: T0, count: 3 })).toEqual([Date.UTC(2026, 0, 1, 14), Date.UTC(2026, 0, 2, 14), Date.UTC(2026, 0, 5, 14)]);
    expect(nextOccurrences(parseSchedule({ every: '1h' }), { from: new Date(T0) })).toHaveLength(10);

    // 02:30 doesn't exist in Warsaw on 29 March 2026: it runs at 03:30 CEST (01:30 UTC).
    expect(nextOccurrences(parseSchedule({ cron: '0 30 2 * * *', tz: 'Europe/Warsaw' }), { from: Date.UTC(2026, 2, 28, 12), count: 2 })).toEqual([
      Date.UTC(2026, 2, 29, 1, 30),
      Date.UTC(2026, 2, 30, 0, 30),
    ]);
  });

  it('stops at the limit, less the runs a stored schedule has had, and at endAt', () => {
    const limited = parseSchedule({ every: '1h', limit: 3 });
    expect(nextOccurrences(limited, { from: T0 })).toEqual([hours(1), hours(2), hours(3)]);
    expect(nextOccurrences(limited, { from: T0, runs: 2 })).toEqual([hours(1)]);
    expect(nextOccurrences(limited, { from: T0, runs: 3 })).toEqual([]);
    expect(nextOccurrences(parseSchedule({ every: '1h', endAt: hours(2) }), { from: T0 })).toEqual([hours(1), hours(2)]);
    expect(nextOccurrences(parseSchedule({ every: '1h' }), { from: -7_200_000, count: 3 })).toEqual([
      -3_600_000,
      0,
      3_600_000,
    ]);
  });

  it('refuses a count or a from it can not use, naming its owner', () => {
    const spec = parseSchedule({ every: '1h' });
    expect(() => nextOccurrences(spec, { count: 0 })).toThrow(new TypeError('nextOccurrences(): count (0) must be an integer from 1 to 1000.'));
    expect(() => nextOccurrences(spec, { count: 1_001 }, 'schedules.preview()')).toThrow('schedules.preview(): count (1001) must be an integer from 1 to 1000.');
    expect(() => nextOccurrences(spec, { from: new Date('soon') })).toThrow('nextOccurrences(): invalid from Invalid Date.');
  });
});

describe('occurrence ids', () => {
  it('name the run of an occurrence after the schedule and the time it was due', () => {
    expect(occurrenceId('weekly-digest', Date.UTC(2026, 0, 5, 7))).toBe('weekly-digest@2026-01-05T07:00:00.000Z');
  });

  it("take schedule ids of letters, digits, '.', ':', '_' and '-' only", () => {
    expect(() => assertScheduleId('tenant-7:report_v2.monthly')).not.toThrow();
    expect(() => assertScheduleId('weekly digest')).toThrow(new TypeError('Invalid schedule id "weekly digest". Use letters, digits, ".", ":", "_" or "-".'));
    expect(() => assertScheduleId('a@b', 'queue "emails"')).toThrow('Invalid schedule id "a@b" of queue "emails".');
    expect(() => assertScheduleId(7)).toThrow('Invalid schedule id 7.');
  });
});
