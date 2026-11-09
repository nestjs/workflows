/**
 * Cron expressions: parsing, and the occurrences a scheduler asks for (the next instant strictly after `after`), in
 * UTC and across daylight saving changes. Expected instants are ISO strings with explicit offsets. The 2026 changes:
 * Europe/Warsaw skips 02:00-03:00 on 29 March and repeats 02:00-03:00 on 25 October (both at 01:00 UTC);
 * America/New_York skips 02:00-03:00 on 8 March and repeats 01:00-02:00 on 1 November; Australia/Lord_Howe repeats
 * 01:30-02:00 on 5 April and skips 02:00-02:30 on 4 October; Pacific/Auckland repeats 02:00-03:00 on 5 April and
 * skips 02:00-03:00 on 27 September.
 */
import { cronOccurrences, nextCron, parseCron, type CronExpression } from '../../lib/core/scheduling/cron.util.js';

const MINUTE = 60_000;
const WARSAW = 'Europe/Warsaw';
const NEW_YORK = 'America/New_York';
const LORD_HOWE = 'Australia/Lord_Howe';
const AUCKLAND = 'Pacific/Auckland';

const at = (iso: string) => Date.parse(iso);
const iso = (instant: number) => new Date(instant).toISOString();
/** ISO strings with any offset, as UTC ones. */
const utc = (...list: string[]) => list.map((item) => iso(at(item)));

/** The occurrences strictly after `after` and before `until`, checking they strictly increase. */
function between(expression: string, after: string, until: string, tz = 'UTC'): number[] {
  const instants: number[] = [];
  for (const instant of cronOccurrences(parseCron(expression), at(after), tz)) {
    if (instant >= at(until)) {
      break;
    }
    expect(instant).toBeGreaterThan(instants[instants.length - 1] ?? at(after));
    instants.push(instant);
  }
  return instants;
}

/** The first `count` occurrences strictly after `after`, as UTC ISO strings. */
function occurrences(expression: string, after: string, count: number, tz = 'UTC'): string[] {
  const instants: string[] = [];
  for (const instant of cronOccurrences(parseCron(expression), at(after), tz)) {
    instants.push(iso(instant));
    if (instants.length === count) {
      break;
    }
  }
  return instants;
}

/** What a cron allows, without its source. */
function fields(expression: string) {
  return { ...parseCron(expression), source: undefined };
}

function thrown(fn: () => unknown): Error {
  try {
    fn();
  } catch (error) {
    return error as Error;
  }
  throw new Error('Expected a throw.');
}

describe('parseCron()', () => {
  it('parses values, ranges, steps, lists and names, in any case', () => {
    expect(parseCron('*/15 0-6/2 1,15 JAN-MAR,dec mon-FRI')).toMatchObject({
      seconds: [0],
      minutes: [0, 15, 30, 45],
      hours: [0, 2, 4, 6],
      daysOfMonth: [1, 15],
      lastDayOfMonth: false,
      months: [1, 2, 3, 12],
      daysOfWeek: [1, 2, 3, 4, 5],
    });
  });

  it('reads a sixth field as the seconds, first', () => {
    expect(parseCron('*/20 30 9 * * *')).toMatchObject({ seconds: [0, 20, 40], minutes: [30], hours: [9] });
  });

  it('reads `a/n` as from a to the maximum, and `?` as `*`', () => {
    expect(parseCron('5/20 20/2 * * *')).toMatchObject({ minutes: [5, 25, 45], hours: [20, 22] });
    expect(fields('? ? ? ? ?')).toEqual(fields('* * * * *'));
  });

  it('reads 0 and 7 as Sunday, in ranges and steps too', () => {
    expect(parseCron('0 0 * * 7').daysOfWeek).toEqual([0]);
    expect(parseCron('0 0 * * 5-7').daysOfWeek).toEqual([0, 5, 6]);
    expect(parseCron('0 0 * * 1/2').daysOfWeek).toEqual([0, 1, 3, 5]);
    expect(parseCron('0 0 * * SAT,SUN').daysOfWeek).toEqual([0, 6]);
    expect(parseCron('0 0 * * *').daysOfWeek).toEqual([0, 1, 2, 3, 4, 5, 6]);
  });

  it('takes L in the day of the month, and nL and n#k in the day of the week', () => {
    expect(parseCron('0 0 L * *')).toMatchObject({ daysOfMonth: [], lastDayOfMonth: true });
    expect(parseCron('0 0 1,l * *')).toMatchObject({ daysOfMonth: [1], lastDayOfMonth: true });
    expect(parseCron('0 0 * * 5L,SUNL')).toMatchObject({ daysOfWeek: [], lastWeekdays: [0, 5] });
    expect(parseCron('0 0 * * 1#2,MON#4,7#1')).toMatchObject({
      nthWeekdays: [
        { weekday: 1, nth: 2 },
        { weekday: 1, nth: 4 },
        { weekday: 0, nth: 1 },
      ],
    });
  });

  it('knows the nicknames, in any case', () => {
    expect(fields('@yearly')).toEqual(fields('0 0 1 1 *'));
    expect(fields('@annually')).toEqual(fields('0 0 1 1 *'));
    expect(fields('@monthly')).toEqual(fields('0 0 1 * *'));
    expect(fields('@weekly')).toEqual(fields('0 0 * * 0'));
    expect(fields('@daily')).toEqual(fields('0 0 * * *'));
    expect(fields('@midnight')).toEqual(fields('0 0 * * *'));
    expect(fields('@hourly')).toEqual(fields('0 * * * *'));
    expect(fields(' @DAILY ')).toEqual(fields('0 0 * * *'));
  });

  it('trims, and takes any whitespace between the fields', () => {
    expect(parseCron('  0\t9 *  *\n MON ')).toMatchObject({ source: '  0\t9 *  *\n MON ', minutes: [0], hours: [9], daysOfWeek: [1] });
  });

  it('marks a day field restricted unless its text starts with * or ? (Vixie cron: DOM_STAR, DOW_STAR)', () => {
    expect(parseCron('0 0 */2 * 1')).toMatchObject({ dayOfMonthRestricted: false, dayOfWeekRestricted: true });
    expect(parseCron('0 0 1-31 * ?')).toMatchObject({ dayOfMonthRestricted: true, dayOfWeekRestricted: false });
    expect(parseCron('0 0 L * 5L')).toMatchObject({ dayOfMonthRestricted: true, dayOfWeekRestricted: true });
  });

  it('accepts February 29, which only leap years have', () => {
    expect(() => parseCron('0 0 29 2 *')).not.toThrow();
    expect(() => parseCron('0 0 * 2 1#5')).not.toThrow();
  });

  it.each([
    ['70 * * * *', 'Invalid cron field "70" in "70 * * * *": minutes are 0-59.'],
    ['60 * * * * *', 'Invalid cron field "60" in "60 * * * * *": seconds are 0-59.'],
    ['0 24 * * *', 'Invalid cron field "24" in "0 24 * * *": hours are 0-23.'],
    ['0 0 0 * *', 'Invalid cron field "0" in "0 0 0 * *": days of the month are 1-31 or L (the last day).'],
    ['0 0 L-2 * *', 'Invalid cron field "L-2" in "0 0 L-2 * *": days of the month are 1-31 or L (the last day).'],
    ['0 0 * 13 *', 'Invalid cron field "13" in "0 0 * 13 *": months are 1-12 or JAN-DEC.'],
    ['0 0 * FOO *', 'Invalid cron field "FOO" in "0 0 * FOO *": months are 1-12 or JAN-DEC.'],
    [
      '0 0 * * 8',
      'Invalid cron field "8" in "0 0 * * 8": days of the week are 0-7 (0 and 7 are Sunday) or SUN-SAT, nL (5L is the last Friday) or n#k (1#1 is the first Monday).',
    ],
    [
      '0 0 * * L',
      'Invalid cron field "L" in "0 0 * * L": days of the week are 0-7 (0 and 7 are Sunday) or SUN-SAT, nL (5L is the last Friday) or n#k (1#1 is the first Monday).',
    ],
    ['0 0 * * 1#6', 'Invalid cron field "1#6" in "0 0 * * 1#6": k in n#k is 1-5 (1#1 is the first Monday of the month).'],
    ['0 0 * * 1#0', 'Invalid cron field "1#0" in "0 0 * * 1#0": k in n#k is 1-5 (1#1 is the first Monday of the month).'],
    ['5-2 * * * *', 'Invalid cron field "5-2" in "5-2 * * * *": the range 5-2 is reversed; put the lower bound first.'],
    ['0 0 * * FRI-MON', 'Invalid cron field "FRI-MON" in "0 0 * * FRI-MON": the range FRI-MON is reversed; put the lower bound first.'],
    ['*/0 * * * *', 'Invalid cron field "*/0" in "*/0 * * * *": a step of 0 never moves; use a positive one, such as */15.'],
    ['*/x * * * *', 'Invalid cron field "*/x" in "*/x * * * *": a step is a whole number, such as */15.'],
    ['1,,2 * * * *', 'Invalid cron field "1,,2" in "1,,2 * * * *": a list item is empty.'],
    ['1- * * * *', 'Invalid cron field "1-" in "1- * * * *": minutes are 0-59.'],
    ['* * * *', 'Invalid cron expression "* * * *": use 5 fields (minute hour day-of-month month day-of-week) or 6 (second first), not 4.'],
    ['* * * * * * *', 'Invalid cron expression "* * * * * * *": use 5 fields (minute hour day-of-month month day-of-week) or 6 (second first), not 7.'],
    ['  ', 'Invalid cron expression "  ": use 5 fields (minute hour day-of-month month day-of-week) or 6 (second first), not 0.'],
    ['@reboot', 'Invalid cron expression "@reboot": use @yearly, @annually, @monthly, @weekly, @daily, @midnight or @hourly, or 5 or 6 fields.'],
    ['0 0 30 2 *', 'Invalid cron expression "0 0 30 2 *": it never matches (February has no 30th).'],
    ['0 0 31 4 *', 'Invalid cron expression "0 0 31 4 *": it never matches (April has no 31st).'],
    ['0 0 30,31 FEB *', 'Invalid cron expression "0 0 30,31 FEB *": it never matches (February has no 30th or 31st).'],
    ['0 0 31 4,6,9,11 *', 'Invalid cron expression "0 0 31 4,6,9,11 *": it never matches (April, June, September and November have no 31st).'],
    // The day of the week starts with *, so both day fields must match.
    ['0 0 30 2 */2', 'Invalid cron expression "0 0 30 2 */2": it never matches (February has no 30th).'],
    ['0 0 */10 2 1#5', 'Invalid cron expression "0 0 */10 2 1#5": it never matches.'],
  ])('throws a TypeError for %j', (expression, message) => {
    const error = thrown(() => parseCron(expression));
    expect(error).toBeInstanceOf(TypeError);
    expect(error.message).toBe(message);
  });

  it('throws a TypeError for a value that is not a string', () => {
    expect(() => parseCron(42 as unknown as string)).toThrow(new TypeError('Invalid cron expression 42. Use a string, such as "0 9 * * MON-FRI".'));
  });
});

describe('nextCron() and cronOccurrences() in UTC', () => {
  it.each([
    ['* * * * * *', '2026-09-29T10:07:13.500Z', ['2026-09-29T10:07:14Z', '2026-09-29T10:07:15Z', '2026-09-29T10:07:16Z']],
    ['* * * * *', '2026-09-29T10:07:13Z', ['2026-09-29T10:08:00Z', '2026-09-29T10:09:00Z']],
    ['* * * * *', '2026-09-29T10:08:00Z', ['2026-09-29T10:09:00Z']],
    ['0 * * * *', '2026-09-29T10:07:13Z', ['2026-09-29T11:00:00Z', '2026-09-29T12:00:00Z']],
    ['@hourly', '2026-09-29T23:00:00Z', ['2026-09-30T00:00:00Z']],
    ['*/15 * * * *', '2026-09-29T10:07:13Z', ['2026-09-29T10:15:00Z', '2026-09-29T10:30:00Z', '2026-09-29T10:45:00Z', '2026-09-29T11:00:00Z']],
    ['30 */10 * * * *', '2026-09-29T10:07:13Z', ['2026-09-29T10:10:30Z', '2026-09-29T10:20:30Z', '2026-09-29T10:30:30Z']],
    // Friday afternoon, then Monday morning.
    ['0 9-17 * * MON-FRI', '2026-10-02T16:30:00Z', ['2026-10-02T17:00:00Z', '2026-10-05T09:00:00Z', '2026-10-05T10:00:00Z']],
    ['0 0 L * *', '2026-01-15T00:00:00Z', ['2026-01-31T00:00:00Z', '2026-02-28T00:00:00Z', '2026-03-31T00:00:00Z', '2026-04-30T00:00:00Z']],
    ['0 0 L 2 *', '2027-03-01T00:00:00Z', ['2028-02-29T00:00:00Z', '2029-02-28T00:00:00Z']],
    ['0 0 * * 5L', '2026-10-01T00:00:00Z', ['2026-10-30T00:00:00Z', '2026-11-27T00:00:00Z', '2026-12-25T00:00:00Z', '2027-01-29T00:00:00Z']],
    ['0 10 * * 1#2', '2026-10-01T00:00:00Z', ['2026-10-12T10:00:00Z', '2026-11-09T10:00:00Z', '2026-12-14T10:00:00Z']],
    ['@weekly', '2026-09-29T10:00:00Z', ['2026-10-04T00:00:00Z', '2026-10-11T00:00:00Z']],
    ['@monthly', '2026-09-29T10:00:00Z', ['2026-10-01T00:00:00Z', '2026-11-01T00:00:00Z']],
    ['@yearly', '2026-09-29T10:00:00Z', ['2027-01-01T00:00:00Z', '2028-01-01T00:00:00Z']],
    // Month ends: only months with a 31st.
    ['0 12 31 * *', '2026-04-01T00:00:00Z', ['2026-05-31T12:00:00Z', '2026-07-31T12:00:00Z', '2026-08-31T12:00:00Z', '2026-10-31T12:00:00Z']],
    // Leap years: February 29, and the fifth Monday of February (28, then 40 years apart).
    ['0 0 29 2 *', '2026-01-01T00:00:00Z', ['2028-02-29T00:00:00Z', '2032-02-29T00:00:00Z']],
    ['0 0 * 2 1#5', '2026-01-01T00:00:00Z', ['2044-02-29T00:00:00Z', '2072-02-29T00:00:00Z', '2112-02-29T00:00:00Z']],
    // Year rollover.
    ['59 23 31 12 *', '2026-12-31T23:59:00Z', ['2027-12-31T23:59:00Z']],
    ['0 0 1 1 *', '2026-12-31T23:59:59.999Z', ['2027-01-01T00:00:00Z']],
    ['*/20 * * * * *', '2026-12-31T23:59:50Z', ['2027-01-01T00:00:00Z', '2027-01-01T00:00:20Z']],
  ])('%s after %s', (expression, after, expected) => {
    expect(occurrences(expression, after, expected.length)).toEqual(utc(...expected));
    expect(iso(nextCron(parseCron(expression), at(after), 'UTC')!)).toBe(iso(at(expected[0])));
  });

  it('returns null when nothing matches within 50 years', () => {
    const never: CronExpression = { ...parseCron('0 0 1 1 *'), daysOfMonth: [30], months: [2] };
    expect(nextCron(never, at('2026-01-01T00:00:00Z'), 'UTC')).toBeNull();
    expect([...cronOccurrences(never, at('2026-01-01T00:00:00Z'), 'UTC')]).toEqual([]);
  });
});

describe('day of the month and day of the week (Vixie cron)', () => {
  it('runs on a day that matches either one when both are restricted', () => {
    expect(occurrences('0 0 1,15 * 1', '2026-09-01T00:00:00Z', 9)).toEqual(
      utc(
        '2026-09-07T00:00:00Z',
        '2026-09-14T00:00:00Z',
        '2026-09-15T00:00:00Z',
        '2026-09-21T00:00:00Z',
        '2026-09-28T00:00:00Z',
        '2026-10-01T00:00:00Z',
        '2026-10-05T00:00:00Z',
        '2026-10-12T00:00:00Z',
        '2026-10-15T00:00:00Z',
      ),
    );
    // L, nL and n#k restrict too: the last day of the month, or a Monday.
    expect(occurrences('0 0 L * MON', '2026-09-01T00:00:00Z', 6)).toEqual(
      utc('2026-09-07T00:00:00Z', '2026-09-14T00:00:00Z', '2026-09-21T00:00:00Z', '2026-09-28T00:00:00Z', '2026-09-30T00:00:00Z', '2026-10-05T00:00:00Z'),
    );
    expect(occurrences('0 0 15 * 5L', '2026-10-01T00:00:00Z', 4)).toEqual(
      utc('2026-10-15T00:00:00Z', '2026-10-30T00:00:00Z', '2026-11-15T00:00:00Z', '2026-11-27T00:00:00Z'),
    );
  });

  it('runs on a day that matches both when either field starts with * or ?, even with a step', () => {
    // Odd days of the month that are Mondays, not odd days or Mondays.
    expect(occurrences('0 0 */2 * 1', '2026-09-01T00:00:00Z', 4)).toEqual(
      utc('2026-09-07T00:00:00Z', '2026-09-21T00:00:00Z', '2026-10-05T00:00:00Z', '2026-10-19T00:00:00Z'),
    );
    // The 13th when it's a Friday or a Sunday (*/5: 0 and 5).
    expect(occurrences('0 0 13 * */5', '2026-01-01T00:00:00Z', 5)).toEqual(
      utc('2026-02-13T00:00:00Z', '2026-03-13T00:00:00Z', '2026-09-13T00:00:00Z', '2026-11-13T00:00:00Z', '2026-12-13T00:00:00Z'),
    );
  });

  it('runs on the days of the one restricted field when the other is * or ?', () => {
    expect(occurrences('0 0 * * MON', '2026-09-01T00:00:00Z', 2)).toEqual(utc('2026-09-07T00:00:00Z', '2026-09-14T00:00:00Z'));
    expect(occurrences('0 0 ? * MON', '2026-09-01T00:00:00Z', 2)).toEqual(utc('2026-09-07T00:00:00Z', '2026-09-14T00:00:00Z'));
    expect(occurrences('0 0 1 * *', '2026-09-01T00:00:00Z', 2)).toEqual(utc('2026-10-01T00:00:00Z', '2026-11-01T00:00:00Z'));
    expect(occurrences('0 0 1 * ?', '2026-09-01T00:00:00Z', 2)).toEqual(utc('2026-10-01T00:00:00Z', '2026-11-01T00:00:00Z'));
  });
});

describe('daylight saving time', () => {
  describe('in Europe/Warsaw', () => {
    it('runs a daily 02:30 that clocks skip at 03:30 CEST, and a repeated one once, the first time', () => {
      expect(occurrences('30 2 * * *', '2026-03-27T12:00:00Z', 3, WARSAW)).toEqual(
        utc('2026-03-28T02:30:00+01:00', '2026-03-29T03:30:00+02:00', '2026-03-30T02:30:00+02:00'),
      );
      expect(occurrences('30 2 * * *', '2026-10-23T12:00:00Z', 3, WARSAW)).toEqual(
        utc('2026-10-24T02:30:00+02:00', '2026-10-25T02:30:00+02:00', '2026-10-26T02:30:00+01:00'),
      );
    });

    it('runs an hourly cron in every hour that exists, and once in the hour clocks repeat', () => {
      expect(occurrences('30 * * * *', '2026-03-28T23:00:00Z', 4, WARSAW)).toEqual(
        utc('2026-03-29T00:30:00+01:00', '2026-03-29T01:30:00+01:00', '2026-03-29T03:30:00+02:00', '2026-03-29T04:30:00+02:00'),
      );
      expect(occurrences('30 * * * *', '2026-10-24T22:00:00Z', 4, WARSAW)).toEqual(
        utc('2026-10-25T00:30:00+02:00', '2026-10-25T01:30:00+02:00', '2026-10-25T02:30:00+02:00', '2026-10-25T03:30:00+01:00'),
      );
    });

    it("runs an every-minute cron once in each minute, skipping the repeated hour's second pass", () => {
      const minutes = (from: string, count: number) => Array.from({ length: count }, (_, i) => at(from) + i * MINUTE);
      expect(between('* * * * *', '2026-03-28T23:59:00Z', '2026-03-29T02:00:00Z', WARSAW)).toEqual(minutes('2026-03-29T00:00:00Z', 120));
      expect(between('* * * * *', '2026-10-24T23:59:00Z', '2026-10-25T03:00:00Z', WARSAW)).toEqual([
        ...minutes('2026-10-25T00:00:00Z', 60),
        ...minutes('2026-10-25T02:00:00Z', 60),
      ]);
    });

    it('finds a skipped wall time that lands after an `after` just past the change', () => {
      const daily = parseCron('30 2 * * *');
      expect(nextCron(daily, at('2026-03-29T00:59:59Z'), WARSAW)).toBe(at('2026-03-29T03:30:00+02:00'));
      expect(nextCron(daily, at('2026-03-29T01:00:00Z'), WARSAW)).toBe(at('2026-03-29T03:30:00+02:00'));
      expect(nextCron(daily, at('2026-03-29T03:15:00+02:00'), WARSAW)).toBe(at('2026-03-29T03:30:00+02:00'));
      expect(nextCron(daily, at('2026-03-29T03:30:00+02:00'), WARSAW)).toBe(at('2026-03-30T02:30:00+02:00'));
      expect(nextCron(daily, at('2026-03-29T03:45:00+02:00'), WARSAW)).toBe(at('2026-03-30T02:30:00+02:00'));
      // 02:15 and 03:15 are one instant, as are 02:45 and 03:45.
      expect(occurrences('15,45 2,3 * * *', '2026-03-29T00:59:00Z', 3, WARSAW)).toEqual(
        utc('2026-03-29T03:15:00+02:00', '2026-03-29T03:45:00+02:00', '2026-03-30T02:15:00+02:00'),
      );
    });

    it('moves on from an `after` in the repeated hour without running its second pass', () => {
      // 02:15 CEST, the first pass, then 02:15 CET, the second.
      expect(nextCron(parseCron('30 2 * * *'), at('2026-10-25T02:15:00+02:00'), WARSAW)).toBe(at('2026-10-25T02:30:00+02:00'));
      expect(nextCron(parseCron('30 2 * * *'), at('2026-10-25T02:15:00+01:00'), WARSAW)).toBe(at('2026-10-26T02:30:00+01:00'));
      expect(nextCron(parseCron('30 * * * *'), at('2026-10-25T02:15:00+01:00'), WARSAW)).toBe(at('2026-10-25T03:30:00+01:00'));
      expect(nextCron(parseCron('* * * * *'), at('2026-10-25T02:15:00+01:00'), WARSAW)).toBe(at('2026-10-25T03:00:00+01:00'));
    });

    it('runs a daily cron once a day all year, and every half hour once in each real one, strictly increasing', () => {
      expect(between('30 2 * * *', '2025-12-31T23:00:00Z', '2026-12-31T23:00:00Z', WARSAW)).toHaveLength(365);
      expect(between('30 2 * * *', '2027-12-31T23:00:00Z', '2028-12-31T23:00:00Z', WARSAW)).toHaveLength(366);
      // 363 days of 48, the 23-hour day's 46 and the 25-hour day's 48 (its second 02:00 and 02:30 don't run).
      expect(between('*/30 * * * *', '2025-12-31T22:59:59Z', '2026-12-31T23:00:00Z', WARSAW)).toHaveLength(363 * 48 + 46 + 48);
    });
  });

  describe('in America/New_York', () => {
    it('runs a skipped 02:30 at 03:30 EDT, and a repeated 01:30 once', () => {
      expect(occurrences('30 2 * * *', '2026-03-06T12:00:00Z', 3, NEW_YORK)).toEqual(
        utc('2026-03-07T02:30:00-05:00', '2026-03-08T03:30:00-04:00', '2026-03-09T02:30:00-04:00'),
      );
      expect(occurrences('30 1 * * *', '2026-10-30T12:00:00Z', 3, NEW_YORK)).toEqual(
        utc('2026-10-31T01:30:00-04:00', '2026-11-01T01:30:00-04:00', '2026-11-02T01:30:00-05:00'),
      );
      expect(occurrences('30 2 * * *', '2026-10-30T12:00:00Z', 3, NEW_YORK)).toEqual(
        utc('2026-10-31T02:30:00-04:00', '2026-11-01T02:30:00-05:00', '2026-11-02T02:30:00-05:00'),
      );
    });

    it('runs an hourly cron in every hour that exists, and once in the hour clocks repeat', () => {
      expect(occurrences('30 * * * *', '2026-03-08T06:00:00Z', 3, NEW_YORK)).toEqual(
        utc('2026-03-08T01:30:00-05:00', '2026-03-08T03:30:00-04:00', '2026-03-08T04:30:00-04:00'),
      );
      expect(occurrences('30 * * * *', '2026-11-01T04:00:00Z', 3, NEW_YORK)).toEqual(
        utc('2026-11-01T00:30:00-04:00', '2026-11-01T01:30:00-04:00', '2026-11-01T02:30:00-05:00'),
      );
    });

    it('runs an every-minute cron once in each minute across both changes', () => {
      const minutes = (from: string, count: number) => Array.from({ length: count }, (_, i) => at(from) + i * MINUTE);
      expect(between('* * * * *', '2026-03-08T05:59:00Z', '2026-03-08T08:00:00Z', NEW_YORK)).toEqual(minutes('2026-03-08T06:00:00Z', 120));
      expect(between('* * * * *', '2026-11-01T04:59:00Z', '2026-11-01T08:00:00Z', NEW_YORK)).toEqual([
        ...minutes('2026-11-01T05:00:00Z', 60),
        ...minutes('2026-11-01T07:00:00Z', 60),
      ]);
    });

    it('handles an `after` just past the change and one in the repeated hour', () => {
      expect(nextCron(parseCron('30 2 * * *'), at('2026-03-08T03:10:00-04:00'), NEW_YORK)).toBe(at('2026-03-08T03:30:00-04:00'));
      expect(nextCron(parseCron('30 1 * * *'), at('2026-11-01T01:15:00-05:00'), NEW_YORK)).toBe(at('2026-11-02T01:30:00-05:00'));
      expect(between('30 2 * * *', '2026-01-01T05:00:00Z', '2027-01-01T05:00:00Z', NEW_YORK)).toHaveLength(365);
    });
  });

  describe('in Pacific/Auckland, more than 12 hours ahead of UTC', () => {
    it('runs a daily 02:30 that clocks skip at 03:30 NZDT, and a repeated one once, the first time', () => {
      expect(occurrences('30 2 * * *', '2026-09-25T00:00:00Z', 3, AUCKLAND)).toEqual(
        utc('2026-09-26T02:30:00+12:00', '2026-09-27T03:30:00+13:00', '2026-09-28T02:30:00+13:00'),
      );
      expect(occurrences('30 2 * * *', '2026-04-03T00:00:00Z', 3, AUCKLAND)).toEqual(
        utc('2026-04-04T02:30:00+13:00', '2026-04-05T02:30:00+13:00', '2026-04-06T02:30:00+12:00'),
      );
    });
  });

  describe('in Australia/Lord_Howe, whose clocks move by 30 minutes', () => {
    it('runs a skipped 02:10 at 02:40, after an existing 02:35', () => {
      const cron = parseCron('10,35 2 * * *');
      expect(occurrences('10,35 2 * * *', '2026-10-03T12:00:00Z', 3, LORD_HOWE)).toEqual(
        utc('2026-10-04T02:35:00+11:00', '2026-10-04T02:40:00+11:00', '2026-10-05T02:10:00+11:00'),
      );
      expect(nextCron(cron, at('2026-10-04T02:30:00+11:00'), LORD_HOWE)).toBe(at('2026-10-04T02:35:00+11:00'));
      expect(nextCron(cron, at('2026-10-04T02:35:00+11:00'), LORD_HOWE)).toBe(at('2026-10-04T02:40:00+11:00'));
    });

    it('runs every half hour and quarter once in each real one', () => {
      expect(occurrences('0,30 * * * *', '2026-10-04T01:00:00+10:30', 4, LORD_HOWE)).toEqual(
        utc('2026-10-04T01:30:00+10:30', '2026-10-04T02:30:00+11:00', '2026-10-04T03:00:00+11:00', '2026-10-04T03:30:00+11:00'),
      );
      expect(occurrences('*/15 * * * *', '2026-04-05T01:00:00+11:00', 5, LORD_HOWE)).toEqual(
        utc('2026-04-05T01:15:00+11:00', '2026-04-05T01:30:00+11:00', '2026-04-05T01:45:00+11:00', '2026-04-05T02:00:00+10:30', '2026-04-05T02:15:00+10:30'),
      );
    });
  });
});
