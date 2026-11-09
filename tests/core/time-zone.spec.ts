/**
 * Offsets and wall times in IANA zones, read through Intl. Expectations are written as ISO strings with explicit
 * offsets, so they don't lean on the code under test. The 2026 changes used below: Europe/Warsaw skips 02:00-03:00 on
 * 29 March and repeats 02:00-03:00 on 25 October (both at 01:00 UTC); America/New_York skips 02:00-03:00 on 8 March
 * and repeats 01:00-02:00 on 1 November; Australia/Lord_Howe moves by 30 minutes, repeating 01:30-02:00 on 5 April
 * and skipping 02:00-02:30 on 4 October. Pacific/Auckland (+12/+13) repeats 02:00-03:00 on 5 April and skips 02:00-03:00
 * on 27 September, and Pacific/Chatham (+12:45/+13:45) repeats and skips 02:45-03:45 on the same days (all at 14:00 UTC
 * the day before).
 */
import { assertTimeZone, fromWallTime, offsetAt, recentClockChange, toWallTime } from '../../lib/core/scheduling/time-zone.util.js';

const MINUTE = 60_000;
const HOUR = 3_600_000;
const WARSAW = 'Europe/Warsaw';
const NEW_YORK = 'America/New_York';
const LORD_HOWE = 'Australia/Lord_Howe';
const AUCKLAND = 'Pacific/Auckland';
const CHATHAM = 'Pacific/Chatham';

/** An instant, from an ISO string with an offset. */
const at = (iso: string) => Date.parse(iso);
/** A wall time: the local date-time's fields read as if they were UTC. */
const wall = (local: string) => Date.parse(`${local}Z`);

describe('offsetAt()', () => {
  it('is 0 in UTC, all year', () => {
    expect(offsetAt(at('2026-01-15T12:00:00Z'), 'UTC')).toBe(0);
    expect(offsetAt(at('2026-07-15T12:00:00Z'), 'UTC')).toBe(0);
  });

  it('is CET (+1h) in winter and CEST (+2h) in summer in Europe/Warsaw, changing at 01:00 UTC', () => {
    expect(offsetAt(at('2026-01-15T12:00:00Z'), WARSAW)).toBe(HOUR);
    expect(offsetAt(at('2026-07-15T12:00:00Z'), WARSAW)).toBe(2 * HOUR);
    expect(offsetAt(at('2026-03-29T00:59:59.999Z'), WARSAW)).toBe(HOUR);
    expect(offsetAt(at('2026-03-29T01:00:00Z'), WARSAW)).toBe(2 * HOUR);
    expect(offsetAt(at('2026-10-25T00:59:59.999Z'), WARSAW)).toBe(2 * HOUR);
    expect(offsetAt(at('2026-10-25T01:00:00Z'), WARSAW)).toBe(HOUR);
  });

  it('is EST (-5h) and EDT (-4h) in America/New_York', () => {
    expect(offsetAt(at('2026-01-15T12:00:00Z'), NEW_YORK)).toBe(-5 * HOUR);
    expect(offsetAt(at('2026-07-15T12:00:00Z'), NEW_YORK)).toBe(-4 * HOUR);
    expect(offsetAt(at('2026-03-08T06:59:59Z'), NEW_YORK)).toBe(-5 * HOUR);
    expect(offsetAt(at('2026-03-08T07:00:00Z'), NEW_YORK)).toBe(-4 * HOUR);
    expect(offsetAt(at('2026-11-01T05:59:59Z'), NEW_YORK)).toBe(-4 * HOUR);
    expect(offsetAt(at('2026-11-01T06:00:00Z'), NEW_YORK)).toBe(-5 * HOUR);
  });

  it("follows Australia/Lord_Howe's 30-minute daylight saving: +10:30 and +11:00", () => {
    expect(offsetAt(at('2026-07-15T12:00:00Z'), LORD_HOWE)).toBe(10.5 * HOUR);
    expect(offsetAt(at('2026-01-15T12:00:00Z'), LORD_HOWE)).toBe(11 * HOUR);
    expect(offsetAt(at('2026-04-04T14:59:59Z'), LORD_HOWE)).toBe(11 * HOUR);
    expect(offsetAt(at('2026-04-04T15:00:00Z'), LORD_HOWE)).toBe(10.5 * HOUR);
    expect(offsetAt(at('2026-10-03T15:29:59Z'), LORD_HOWE)).toBe(10.5 * HOUR);
    expect(offsetAt(at('2026-10-03T15:30:00Z'), LORD_HOWE)).toBe(11 * HOUR);
  });

  it('is fixed in Asia/Kolkata (+5:30) and Etc/GMT-3 (+3h: POSIX signs are inverted)', () => {
    for (const instant of [at('2026-01-15T12:00:00Z'), at('2026-07-15T12:00:00Z')]) {
      expect(offsetAt(instant, 'Asia/Kolkata')).toBe(5.5 * HOUR);
      expect(offsetAt(instant, 'Etc/GMT-3')).toBe(3 * HOUR);
    }
  });

  it('reads instants with milliseconds, and years before 1000 and 100 (local mean time in Warsaw was +1:24)', () => {
    expect(offsetAt(at('2026-07-15T12:00:00.750Z'), WARSAW)).toBe(2 * HOUR);
    expect(offsetAt(at('0999-06-01T00:00:00Z'), WARSAW)).toBe(84 * MINUTE);
    expect(offsetAt(at('0050-06-01T00:00:00Z'), 'UTC')).toBe(0);
    expect(offsetAt(at('0050-06-01T00:00:00Z'), WARSAW)).toBe(84 * MINUTE);
  });
});

describe('toWallTime()', () => {
  it('reads the local date-time, keeping milliseconds', () => {
    expect(toWallTime(at('2026-07-15T12:34:56.789Z'), WARSAW)).toBe(wall('2026-07-15T14:34:56.789'));
    expect(toWallTime(at('2026-01-15T03:00:00Z'), NEW_YORK)).toBe(wall('2026-01-14T22:00:00'));
    expect(toWallTime(at('2026-01-15T00:00:00Z'), 'Asia/Kolkata')).toBe(wall('2026-01-15T05:30:00'));
    expect(toWallTime(at('2026-01-15T00:00:00Z'), 'UTC')).toBe(wall('2026-01-15T00:00:00'));
  });

  it('reads years before 1000 and before 100, which Date.UTC would read as 1900-1999', () => {
    expect(toWallTime(at('0999-06-01T00:00:00Z'), WARSAW)).toBe(wall('0999-06-01T01:24:00'));
    expect(toWallTime(at('0050-06-15T12:00:00Z'), 'UTC')).toBe(wall('0050-06-15T12:00:00'));
    expect(toWallTime(at('0004-02-29T23:00:00Z'), 'Etc/GMT-3')).toBe(wall('0004-03-01T02:00:00'));
  });

  it('jumps over the wall times clocks skip, and repeats the ones they repeat', () => {
    expect(toWallTime(at('2026-03-29T00:59:59Z'), WARSAW)).toBe(wall('2026-03-29T01:59:59'));
    expect(toWallTime(at('2026-03-29T01:00:00Z'), WARSAW)).toBe(wall('2026-03-29T03:00:00'));
    expect(toWallTime(at('2026-10-25T00:30:00Z'), WARSAW)).toBe(wall('2026-10-25T02:30:00'));
    expect(toWallTime(at('2026-10-25T01:30:00Z'), WARSAW)).toBe(wall('2026-10-25T02:30:00'));
  });
});

describe('fromWallTime()', () => {
  it('maps ordinary wall times to their instant', () => {
    expect(fromWallTime(wall('2026-07-15T14:34:56.789'), WARSAW)).toBe(at('2026-07-15T14:34:56.789+02:00'));
    expect(fromWallTime(wall('2026-01-15T09:00:00'), WARSAW)).toBe(at('2026-01-15T09:00:00+01:00'));
    expect(fromWallTime(wall('2026-01-14T22:00:00'), NEW_YORK)).toBe(at('2026-01-14T22:00:00-05:00'));
    expect(fromWallTime(wall('2026-03-29T01:59:59'), WARSAW)).toBe(at('2026-03-29T01:59:59+01:00'));
    expect(fromWallTime(wall('2026-03-29T03:00:00'), WARSAW)).toBe(at('2026-03-29T03:00:00+02:00'));
  });

  it('reads a wall time clocks skip with the offset from before the change, so it lands that much later', () => {
    expect(fromWallTime(wall('2026-03-29T02:00:00'), WARSAW)).toBe(at('2026-03-29T03:00:00+02:00'));
    expect(fromWallTime(wall('2026-03-29T02:30:00'), WARSAW)).toBe(at('2026-03-29T03:30:00+02:00'));
    expect(fromWallTime(wall('2026-03-29T02:59:59.999'), WARSAW)).toBe(at('2026-03-29T03:59:59.999+02:00'));
    expect(fromWallTime(wall('2026-03-08T02:30:00'), NEW_YORK)).toBe(at('2026-03-08T03:30:00-04:00'));
    expect(fromWallTime(wall('2026-10-04T02:10:00'), LORD_HOWE)).toBe(at('2026-10-04T02:40:00+11:00'));
  });

  it('takes the first instant of a wall time clocks repeat', () => {
    expect(fromWallTime(wall('2026-10-25T02:00:00'), WARSAW)).toBe(at('2026-10-25T02:00:00+02:00'));
    expect(fromWallTime(wall('2026-10-25T02:30:00'), WARSAW)).toBe(at('2026-10-25T02:30:00+02:00'));
    expect(fromWallTime(wall('2026-10-25T03:00:00'), WARSAW)).toBe(at('2026-10-25T03:00:00+01:00'));
    expect(fromWallTime(wall('2026-11-01T01:30:00'), NEW_YORK)).toBe(at('2026-11-01T01:30:00-04:00'));
    expect(fromWallTime(wall('2026-11-01T02:30:00'), NEW_YORK)).toBe(at('2026-11-01T02:30:00-05:00'));
    expect(fromWallTime(wall('2026-04-05T01:45:00'), LORD_HOWE)).toBe(at('2026-04-05T01:45:00+11:00'));
  });

  it('maps wall times near a change in zones more than 12 hours ahead of UTC', () => {
    expect(fromWallTime(wall('2026-09-27T02:30:00'), AUCKLAND)).toBe(at('2026-09-27T03:30:00+13:00'));
    expect(fromWallTime(wall('2026-04-05T02:30:00'), AUCKLAND)).toBe(at('2026-04-05T02:30:00+13:00'));
    expect(fromWallTime(wall('2026-09-27T03:00:00'), CHATHAM)).toBe(at('2026-09-27T04:00:00+13:45'));
    expect(fromWallTime(wall('2026-04-05T02:30:00'), CHATHAM)).toBe(at('2026-04-05T02:30:00+13:45'));
    expect(fromWallTime(wall('2026-04-05T03:00:00'), CHATHAM)).toBe(at('2026-04-05T03:00:00+13:45'));
  });

  it('works in UTC and fixed zones, and before the year 100', () => {
    expect(fromWallTime(wall('2026-03-29T02:30:00'), 'UTC')).toBe(at('2026-03-29T02:30:00Z'));
    expect(fromWallTime(wall('2026-03-29T02:30:00'), 'Asia/Kolkata')).toBe(at('2026-03-29T02:30:00+05:30'));
    expect(fromWallTime(wall('2026-03-29T02:30:00'), 'Etc/GMT-3')).toBe(at('2026-03-29T02:30:00+03:00'));
    expect(fromWallTime(wall('0050-06-15T12:00:00'), 'UTC')).toBe(at('0050-06-15T12:00:00Z'));
    expect(fromWallTime(wall('0050-06-15T13:24:00'), WARSAW)).toBe(at('0050-06-15T12:00:00Z'));
  });

  it.each([
    // zone, the wall times clocks skip (start, end, by how much), the instants of the repeated wall times' second pass
    [WARSAW, ['2026-03-29T02:00:00', '2026-03-29T03:00:00', HOUR], ['2026-10-25T01:00:00Z', '2026-10-25T02:00:00Z', HOUR]],
    [NEW_YORK, ['2026-03-08T02:00:00', '2026-03-08T03:00:00', HOUR], ['2026-11-01T06:00:00Z', '2026-11-01T07:00:00Z', HOUR]],
    [LORD_HOWE, ['2026-10-04T02:00:00', '2026-10-04T02:30:00', 30 * MINUTE], ['2026-04-04T15:00:00Z', '2026-04-04T15:30:00Z', 30 * MINUTE]],
    [AUCKLAND, ['2026-09-27T02:00:00', '2026-09-27T03:00:00', HOUR], ['2026-04-04T14:00:00Z', '2026-04-04T15:00:00Z', HOUR]],
    [CHATHAM, ['2026-09-27T02:45:00', '2026-09-27T03:45:00', HOUR], ['2026-04-04T14:00:00Z', '2026-04-04T15:00:00Z', HOUR]],
  ] as const)('round-trips wall times and instants through 2026 in %s', (tz, [gapStart, gapEnd, gap], [repeatStart, repeatEnd, repeat]) => {
    for (let instant = at('2026-01-01T00:00:00Z'); instant < at('2027-01-01T00:00:00Z'); instant += 53 * MINUTE) {
      const inSecondPass = instant >= at(repeatStart) && instant < at(repeatEnd);
      expect(fromWallTime(toWallTime(instant, tz), tz)).toBe(inSecondPass ? instant - repeat : instant);
    }
    for (let local = wall('2026-01-01T00:00:00'); local < wall('2027-01-01T00:00:00'); local += 53 * MINUTE) {
      const skipped = local >= wall(gapStart) && local < wall(gapEnd);
      expect(toWallTime(fromWallTime(local, tz), tz)).toBe(skipped ? local + gap : local);
    }
    // Every minute of the change days too.
    for (let local = wall(gapStart) - HOUR; local < wall(gapEnd) + HOUR; local += MINUTE) {
      const skipped = local >= wall(gapStart) && local < wall(gapEnd);
      expect(toWallTime(fromWallTime(local, tz), tz)).toBe(skipped ? local + gap : local);
    }
  });
});

describe('recentClockChange()', () => {
  it('is how far clocks moved while the change is less than that long ago, else 0', () => {
    expect(recentClockChange(at('2026-03-29T00:59:59Z'), WARSAW)).toBe(0);
    expect(recentClockChange(at('2026-03-29T01:00:00Z'), WARSAW)).toBe(HOUR);
    expect(recentClockChange(at('2026-03-29T01:59:59Z'), WARSAW)).toBe(HOUR);
    expect(recentClockChange(at('2026-03-29T02:00:00Z'), WARSAW)).toBe(0);
    expect(recentClockChange(at('2026-10-25T01:00:00Z'), WARSAW)).toBe(-HOUR);
    expect(recentClockChange(at('2026-10-25T01:59:59Z'), WARSAW)).toBe(-HOUR);
    expect(recentClockChange(at('2026-10-25T02:00:00Z'), WARSAW)).toBe(0);
    expect(recentClockChange(at('2026-10-03T15:45:00Z'), LORD_HOWE)).toBe(30 * MINUTE);
    expect(recentClockChange(at('2026-10-03T16:00:00Z'), LORD_HOWE)).toBe(0);
    expect(recentClockChange(at('2026-03-29T01:30:00Z'), 'UTC')).toBe(0);
  });
});

describe('assertTimeZone()', () => {
  it('accepts IANA names, in any case, and UTC', () => {
    for (const tz of ['Europe/Warsaw', 'europe/warsaw', 'UTC', 'Asia/Kolkata', 'Etc/GMT-3', 'Australia/Lord_Howe']) {
      expect(() => assertTimeZone(tz, 'schedule "nightly"')).not.toThrow();
    }
  });

  it('throws a TypeError naming the owner for anything else', () => {
    expect(() => assertTimeZone('Mars/Olympus_Mons', 'schedule "nightly"')).toThrow(
      new TypeError('Invalid time zone "Mars/Olympus_Mons" for schedule "nightly". Use an IANA name, such as "Europe/Warsaw" or "UTC".'),
    );
    for (const tz of ['', 'CEST+2', 42, null, undefined]) {
      expect(() => assertTimeZone(tz, 'schedule "nightly"')).toThrow(TypeError);
    }
  });
});
