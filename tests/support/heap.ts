import { setFlagsFromString } from 'node:v8';
import { runInNewContext } from 'node:vm';

// `gc()` without starting the test process with --expose-gc: a context made after the flag is set has it. Importing
// this module sets that flag for the whole process it runs in, whether or not anything goes on to measure.
setFlagsFromString('--expose-gc');
const gc = runInNewContext('gc') as () => void;

/**
 * The bytes the heap holds once garbage is collected: what something keeps, measured before and after it runs.
 *
 * A reading only means anything because each spec file gets a process of its own (vitest's defaults, `pool: 'forks'`
 * and `isolate: true`), so what another file allocates never lands in it. Under `pool: 'threads'` or `isolate: false`
 * the files sharing the process would be measured too, and a threshold here would stop measuring what it names
 * without failing to say so.
 */
export function heapUsed(): number {
  gc();
  gc();
  return process.memoryUsage().heapUsed;
}
