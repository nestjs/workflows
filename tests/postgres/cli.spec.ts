/**
 * The `nest-workflows` command (lib/cli.ts: the kit's runStoreCli() on both stores' schemas) on PostgreSQL, its default
 * dialect: `sql` prints what migrationSql() does, `migrate` applies the migrations, `status` exits with 1 while the
 * schema is behind, and every misuse says what to do instead, in workflows' words. tests/mysql/cli.spec.ts runs it on
 * MySQL.
 */
import { runStoreCli, type StoreCliIo } from '@nestjs/store-kit';
import { mysqlWorkflowStoreSchema } from '../../lib/mysql/migrations/index.js';
import { PostgresWorkflowStore } from '../../lib/postgres/index.js';
import { workflowStoreSchema } from '../../lib/postgres/migrations/index.js';
import { testDatabase } from './support.js';

const { database, reason } = await testDatabase('pgstore_cli');

/** The command as the bin runs it: both stores' schemas. */
async function run(argv: string[], env: StoreCliIo['env'] = {}) {
  const output = { out: '', err: '' };
  const code = await runStoreCli([workflowStoreSchema, mysqlWorkflowStoreSchema], argv, { out: (text) => (output.out += text), err: (text) => (output.err += text), env });
  return { code, ...output };
}

describe('nest-workflows', () => {
  it('sql prints the migrations as migrationSql() does, without a database, with statement breakpoints on request', async () => {
    expect(await run(['sql'])).toEqual({ code: 0, out: PostgresWorkflowStore.migrationSql(), err: '' });
    expect(await run(['sql', '--schema', 'shop_workflows', '--from', '0', '--to', '1'])).toEqual({
      code: 0,
      out: PostgresWorkflowStore.migrationSql({ schema: 'shop_workflows', from: 0, to: 1 }),
      err: '',
    });
    expect(await run(['sql', '--dialect', 'postgres', '--statement-breakpoints'])).toEqual({
      code: 0,
      out: PostgresWorkflowStore.migrationSql({ statementBreakpoints: true }),
      err: '',
    });
    expect(await run(['sql', '--from', 'one'])).toEqual({ code: 1, out: '', err: '--from takes a version number, not "one".\n' });
    expect((await run(['sql', '--to', '9'])).err).toContain('no migrations lead from version 0 to 9');
    expect((await run(['sql', '--schema', 'bad-name'])).err).toContain('PostgresWorkflowStore: invalid schema "bad-name".');
  });

  it('prints its usage for --help, and on stderr, exiting with 1, for no command, an unknown one or an unknown option', async () => {
    const help = await run(['--help']);
    expect(help).toMatchObject({ code: 0, err: '' });
    expect(help.out).toMatch(/^Usage: npx @nestjs\/workflows <command> \[options\]/);
    expect(await run([])).toEqual({ code: 1, out: '', err: help.out });
    expect(await run(['upgrade'])).toEqual({ code: 1, out: '', err: `Unknown command "upgrade".\n\n${help.out}` });
    expect((await run(['sql', '--verbose'])).err).toMatch(/^Unknown option '--verbose'/);
  });

  it('needs the database for migrate and status: --url, else DATABASE_URL', async () => {
    expect(await run(['migrate'])).toEqual({ code: 1, out: '', err: 'npx @nestjs/workflows migrate needs the database: pass --url, or set DATABASE_URL.\n' });
    expect((await run(['status'])).err).toBe('npx @nestjs/workflows status needs the database: pass --url, or set DATABASE_URL.\n');
  });

  describe('on PostgreSQL', () => {
    beforeEach((context) => {
      if (reason) {
        context.skip(reason);
      }
    });

    it('migrate applies the pending migrations once, and status exits with 1 until they are', async () => {
      const url = database!.url;
      expect(await run(['status', '--url', url, '--schema', 'cli_store'])).toEqual({
        code: 1,
        out: 'Schema "cli_store" is at version 0; this version of @nestjs/workflows needs version 1.\n',
        err: '',
      });
      expect(await run(['migrate', '--schema', 'cli_store'], { DATABASE_URL: url })).toEqual({
        code: 0,
        out: 'Migrated schema "cli_store" to version 1 (applied 1).\n',
        err: '',
      });
      expect(await run(['migrate', '--url', url, '--schema', 'cli_store'])).toEqual({ code: 0, out: 'Schema "cli_store" is up to date (version 1).\n', err: '' });
      expect(await run(['status', '--url', url, '--schema', 'cli_store'])).toMatchObject({ code: 0, out: 'Schema "cli_store" is at version 1; this version of @nestjs/workflows needs version 1.\n' });
      expect(await database!.admin.query('SELECT version FROM cli_store.migrations')).toMatchObject({ rows: [{ version: 1 }] });
    });

    it('reports a database it cannot migrate, and exits with 1', async () => {
      await database!.admin.query('CREATE SCHEMA cli_taken');
      await database!.admin.query('CREATE TABLE cli_taken.signals (id int)');
      expect(await run(['migrate', '--url', database!.url, '--schema', 'cli_taken'])).toEqual({
        code: 1,
        out: '',
        err: 'PostgresWorkflowStore: migrating schema "cli_taken" from version 0 to 1 failed, and nothing was applied: relation "signals" already exists\n',
      });
    });
  });
});
