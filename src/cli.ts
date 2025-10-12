#!/usr/bin/env node
import 'ts-node/register'
import { createQueryRunner, Migrator } from '.'
import { createLoggerContext } from './core/logger'

async function main() {
  const command = process.argv[2]
  const args = process.argv.slice(3)

  const options = {
    database: process.env.CLICKHOUSE_DATABASE,
    url: process.env.CLICKHOUSE_URL,
    username: process.env.CLICKHOUSE_USERNAME,
    password: process.env.CLICKHOUSE_PASSWORD,
    migrationsTableName: process.env.CLICKHOUSE_MIGRATIONS_TABLE_NAME,
    cluster: process.env.CLICKHOUSE_CLUSTER,
    allowMutations: process.env.CLICKHOUSE_ALLOW_MUTATIONS === 'true',
    dryRun: process.env.CLICKHOUSE_DRY_RUN === 'true',
  }

  if (
    options.database === undefined ||
    options.url === undefined ||
    options.username === undefined ||
    options.password === undefined
  ) {
    throw new Error(
      'Missing required environment variables: CLICKHOUSE_DATABASE, CLICKHOUSE_URL, CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD',
    )
  }

  const runner = createQueryRunner({
    url: options.url,
    username: options.username,
    password: options.password,
    database: options.database,
  })

  const migrator = new Migrator(runner, {
    migrationsTableName: options.migrationsTableName,
    cluster: options.cluster,
    allowMutations: options.allowMutations,
    dryRun: options.dryRun,
  })

  await migrator.init()
  const logger = createLoggerContext({ component: 'cli', operation: command })

  switch (command) {
    case 'migrate:up':
      logger.info('Applying migrations...')
      await migrator.up()
      logger.info('Migrations applied successfully')
      break
    case 'migrate:down':
      const count = parseInt(args[0]) || 1
      logger.info(`Rolling back ${count} migration(s)...`)
      await migrator.down(count)
      logger.info('Rollback completed')
      break
    case 'migrate:status':
      logger.info('Migration status:')
      const status = await migrator.status()
      status.forEach((m) => {
        const statusIcon = m.status === 'applied' ? '✅' : m.status === 'failed' ? '❌' : '⏳'
        logger.info(`  ${statusIcon} ${m.id} - ${m.appliedAt || 'Not applied'}`)
      })
      break
    case 'migrate:plan':
      logger.info('Migration plan:')
      const plan = await migrator.plan()
      plan.print()
      break
    case 'migrate:drift':
      logger.info('Detecting schema drift...')
      const drift = await migrator.detectDrift()
      if (drift.hasDrift) {
        logger.warn('Schema drift detected:')
        drift.differences.forEach((diff) => {
          logger.warn(`  - ${diff.type}: ${diff.table}${diff.column ? `.${diff.column}` : ''}`)
        })
      } else {
        logger.info('No schema drift detected')
      }
      break
    case 'migrate:repair':
      logger.info('Repairing schema drift...')
      await migrator.repairDrift()
      logger.info('Schema repair completed')
      break
    default:
      logger.info(`
ClickHouse Toolkit CLI

Usage: clickhouse-toolkit <command> [options]

Commands:
  migrate:up          Apply pending migrations
  migrate:down [n]    Rollback n migrations (default: 1)
  migrate:status      Show migration status
  migrate:plan        Show migration plan (dry-run)
  migrate:drift       Detect schema drift
  migrate:repair      Repair schema drift

Environment Variables:
  CLICKHOUSE_URL                    ClickHouse server URL
  CLICKHOUSE_USERNAME               Username
  CLICKHOUSE_PASSWORD               Password
  CLICKHOUSE_DATABASE               Database name
  CLICKHOUSE_MIGRATIONS_TABLE_NAME  Migrations table name (optional)
  CLICKHOUSE_CLUSTER                Cluster name for ON CLUSTER (optional)
  CLICKHOUSE_ALLOW_MUTATIONS        Allow mutations (true/false, optional)
  CLICKHOUSE_DRY_RUN                Dry run mode (true/false, optional)
      `)
  }

  await runner.close()
  process.exit(0)
}

main().catch((error) => {
  const logger = createLoggerContext({ component: 'cli', operation: 'main' })
  logger.error('Error:', { error: error.message })
  process.exit(1)
})
