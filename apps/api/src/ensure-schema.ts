/**
 * Boot-time schema apply, delegating to packages/db/scripts/migrate.ts
 * (node-pg-migrate; tracking in `kortix_migrations.pgmigrations`).
 *
 * Apply at boot ONLY when the DB is private to a single instance — local dev,
 * or a self-hoster who sets KORTIX_AUTO_MIGRATE=1. Multi-replica / shared-DB
 * deployments (Kortix cloud; preview branches share the dev DB) leave it off and
 * are warn-only here; they apply migrations once in the deploy pipeline's
 * migrate-db job (or `bun packages/db/scripts/migrate.ts up`) before serving.
 */

import { join } from 'node:path';
import postgres from 'postgres';
import { config } from './config';

export async function ensureSchema(): Promise<void> {
  if (!config.DATABASE_URL) {
    console.log('[schema] No DATABASE_URL configured — skipping');
    return;
  }

  const isLocalDev = process.env.KORTIX_LOCAL_DEV === '1' || process.env.ENV_MODE === 'local';
  const autoMigrate = process.env.KORTIX_AUTO_MIGRATE === '1';

  // Boot-time apply is OPT-IN, for cases where the app's database is private to
  // a single instance: local dev, or a self-hoster who sets KORTIX_AUTO_MIGRATE=1.
  // Multi-replica / shared-DB deployments (Kortix cloud; preview branches share
  // the dev DB) must NOT migrate from boot — concurrent pods would race a
  // half-applied state — so they leave this off and apply migrations once in the
  // deploy pipeline (or `docker run <image> bun packages/db/scripts/migrate.ts up`)
  // before the new code serves. At boot we only surface drift loudly.
  if ((!isLocalDev && !autoMigrate) || process.env.KORTIX_SKIP_ENSURE_SCHEMA === '1') {
    const reason =
      process.env.KORTIX_SKIP_ENSURE_SCHEMA === '1'
        ? 'KORTIX_SKIP_ENSURE_SCHEMA=1'
        : `deployed env (INTERNAL_KORTIX_ENV=${config.INTERNAL_KORTIX_ENV}); set KORTIX_AUTO_MIGRATE=1 to apply at boot`;
    console.log(
      `[schema] ${reason} — not auto-applying (migrations are managed by the deploy pipeline). Checking for drift...`,
    );
    await warnIfCriticalTablesMissing();
    return;
  }

  const dbPkgRoot = join(import.meta.dir, '../../../packages/db');
  const migratorPath = join(dbPkgRoot, 'scripts', 'migrate.ts');

  console.log(
    `[schema] ${isLocalDev ? 'Local dev' : 'KORTIX_AUTO_MIGRATE=1'} — applying pending migrations via migrate.ts...`,
  );
  const bunBin = process.execPath;
  const proc = Bun.spawn([bunBin, migratorPath, 'up'], {
    cwd: dbPkgRoot,
    env: {
      ...process.env,
      DATABASE_URL: config.DATABASE_URL,
    },
    stdout: 'inherit',
    stderr: 'inherit',
  });
  const exitCode = await proc.exited;
  if (exitCode !== 0) {
    console.error(
      `[schema] migrate up failed (exit ${exitCode}) — the application may misbehave until the operator fixes it.`,
    );
    return;
  }
  console.log('[schema] Migrations complete.');
}

/**
 * When KORTIX_SKIP_ENSURE_SCHEMA=1 is set, probe a small set of
 * IAM-critical tables and log a single grouped warning if any are
 * missing. Operators usually set the flag to manage migrations
 * out-of-band; this helps them spot "I forgot to apply migration N"
 * before the first 500 hits a route.
 */
async function warnIfCriticalTablesMissing(): Promise<void> {
  if (!config.DATABASE_URL) return;
  // Critical tables for IAM + auth + vault paths. Keep this list
  // small and stable — extending it for every new migration would be
  // noise. We check only tables in the `kortix` schema (no tuple
  // joins, no driver-specific helpers) so the query stays portable.
  const required = [
    'account_groups',
    'account_group_members',
    'account_members',
    'accounts',
    'audit_events',
    'project_group_grants',
    'project_members',
    'project_secrets',
    'projects',
  ];
  const db = postgres(config.DATABASE_URL, { max: 1 });
  try {
    const rows = (await db`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'kortix' AND table_name IN ${db(required)}
    `) as Array<{ table_name: string }>;
    const present = new Set(rows.map((r) => r.table_name));
    const missing = required.filter((n) => !present.has(n));
    if (missing.length > 0) {
      console.warn('[schema] ⚠ KORTIX_SKIP_ENSURE_SCHEMA=1 but critical tables are missing:');
      for (const m of missing) console.warn(`[schema]   • kortix.${m}`);
      console.warn('[schema] Run `pnpm migrate` or remove the env flag to auto-apply.');
    }
  } catch (err) {
    console.warn('[schema] could not verify table presence:', (err as Error).message ?? err);
  } finally {
    await db.end();
  }
}
