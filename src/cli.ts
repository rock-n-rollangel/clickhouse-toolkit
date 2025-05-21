#!/usr/bin/env node
import 'ts-node/register'
import { ConnectionOptions } from './types/connection-options'
import { Connection } from './connection/connection'
import { ClickHouseToolkitError } from './errors/clickhouse-toolkit-error'

async function main() {
  const command = process.argv[2]
  const args = process.argv.slice(3)

  const options = {
    database: process.env.CLICKHOUSE_DATABASE,
    migrations: process.env.CLICKHOUSE_MIGRATIONS?.split(','),
    migrationsTableName: process.env.CLICKHOUSE_MIGRATIONS_TABLE_NAME,
    url: process.env.CLICKHOUSE_URL,
    username: process.env.CLICKHOUSE_USERNAME,
    password: process.env.CLICKHOUSE_PASSWORD,
    logging: true,
  } as ConnectionOptions

  if (!options.database || !options.migrations || !options.url || !options.username || !options.password) {
    throw new ClickHouseToolkitError('Missing required environment variables')
  }

  const connection = await Connection.initialize(options)

  await connection.migrator.init()

  switch (command) {
    case 'up':
      await connection.migrator.up()
      break
    case 'down':
      await connection.migrator.down(parseInt(args[0]))
      break
    case 'status':
      await connection.migrator.status()
      break
  }

  process.exit(0)
}

main()
