const DAY = 86_400_000;

const formats = new Map<string, Intl.DateTimeFormat>();

/** Throws a TypeError naming `owner` for a zone Intl doesn't know (e.g. 'Europe/Warsaw', 'UTC', 'Asia/Kolkata' are fine). */
export function assertTimeZone(tz: unknown, owner: string): asserts tz is string {
  if (typeof tz !== 'string' || !isTimeZone(tz)) {
    throw new TypeError(`Invalid time zone ${JSON.stringify(tz)} for ${owner}. Use an IANA name, such as "Europe/Warsaw" or "UTC".`);
  }
}

/** The zone's UTC offset (ms, e.g. +3_600_000 for CET) at instant `utc`. */
export function offsetAt(utc: number, tz: string): number {
  const second = Math.floor(utc / 1_000) * 1_000;
  return wallSecond(second, tz) - second;
}

/** The wall time in `tz` at instant `utc` (ms kept). */
export function toWallTime(utc: number, tz: string): number {
  return utc + offsetAt(utc, tz);
}

/**
 * The instant of wall time `wall` in `tz`. A wall time that happens twice (clocks going back) is its first
 * instant; one that doesn't exist (clocks going forward) is read with the offset from before the change, so it
 * lands that much later (02:30 on the day Europe/Warsaw skips 02:00-03:00 is 03:30 CEST) — RFC 5545's rule.
 */
export function fromWallTime(wall: number, tz: string): number {
  // A zone's changes are months apart, so the offsets a day either side of `wall` are the ones in force before and
  // after any change near it. Half a day isn't enough: read as an instant, `wall` is the zone's offset (up to 14
  // hours) away from its own, so 12 hours before it can already be past a change in Pacific/Auckland (+13).
  const before = wall - offsetAt(wall - DAY, tz);
  const after = wall - offsetAt(wall + DAY, tz);
  const first = Math.min(before, after);
  if (toWallTime(first, tz) === wall) {
    return first;
  }

  const second = Math.max(before, after);
  if (second !== first && toWallTime(second, tz) === wall) {
    return second;
  }
  return before;
}

/**
 * How far clocks moved (ms, positive when they went forward) at a change less than that long before instant `utc`,
 * else 0. Until then wall times and instants run out of step: when clocks skip 02:00-03:00, the skipped 02:30 lands
 * at 03:30, still ahead at 03:15; when they repeat that hour, the first 02:45 comes before the second 02:15.
 */
export function recentClockChange(utc: number, tz: string): number {
  const now = offsetAt(utc, tz);
  const change = now - offsetAt(utc - DAY, tz);
  return change !== 0 && offsetAt(utc - Math.abs(change), tz) !== now ? change : 0;
}

function isTimeZone(tz: string): boolean {
  try {
    formatter(tz);
    return true;
  } catch {
    return false;
  }
}

function formatter(tz: string): Intl.DateTimeFormat {
  let format = formats.get(tz);
  if (!format) {
    // `era` tells year 1 from 1 BC, which both print as "1".
    format = new Intl.DateTimeFormat('en-US', {
      timeZone: tz,
      hourCycle: 'h23',
      era: 'short',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
    formats.set(tz, format);
  }
  return format;
}

/** The wall time in `tz` at `utc`, a whole second. */
function wallSecond(utc: number, tz: string): number {
  const parts: Partial<Record<Intl.DateTimeFormatPartTypes, string>> = {};
  for (const { type, value } of formatter(tz).formatToParts(utc)) {
    parts[type] = value;
  }

  const year = parts.era === 'BC' ? 1 - Number(parts.year) : Number(parts.year);
  // Not Date.UTC, which reads years 0-99 as 1900-1999.
  const wall = new Date(Date.UTC(2000, 0, 1, Number(parts.hour), Number(parts.minute), Number(parts.second)));
  wall.setUTCFullYear(year, Number(parts.month) - 1, Number(parts.day));
  return wall.getTime();
}
