import { Connection } from '../../../src/connection/connection'
import { ConnectionOptions } from '../../things/connection-options'
import { Table } from '../../../src/schema-builder/table'
import { Schema } from '../../../src/decorators/schema/schema'
import { Column } from '../../../src/decorators/column/column'
import { countTables } from '../../things/count-tables'

const RENAME_STRING = 'query_runner_test_schema_renamed'

@Schema({ engine: 'MergeTree' })
class QueryRunnerTestSchema {
  @Column({ type: 'String' })
  someColumn: string
}

@Schema({ engine: 'MergeTree' })
class QueryRunnerTestViewStorageSchema {
  @Column({ type: 'String' })
  someColumn: string
}

@Schema({
  materialized: true,
  materializedQuery: (qb) => qb.select().from('query_runner_test_schema'),
  materializedTo: 'query_runner_test_schema',
})
class QueryRunnerTestViewSchema {}

describe('QueryRunner (integrational)', () => {
  let connection: Connection
  beforeAll(async () => {
    connection = await Connection.initialize({
      ...ConnectionOptions,
      schemas: [QueryRunnerTestSchema, QueryRunnerTestViewSchema, QueryRunnerTestViewStorageSchema],
    })
  })

  beforeEach(async () => {
    await connection.command(`
      DROP TABLE IF EXISTS ${connection.getMetadata(QueryRunnerTestSchema).tableMetadataArgs.name}
      `)

    await connection.command(`
      DROP TABLE IF EXISTS ${connection.getMetadata(QueryRunnerTestViewStorageSchema).tableMetadataArgs.name}
      `)

    await connection.command(`
      DROP VIEW IF EXISTS ${connection.getMetadata(QueryRunnerTestViewSchema).tableMetadataArgs.name}
      `)

    await connection.command(`
      DROP TABLE IF EXISTS ${RENAME_STRING}
      `)
  })

  it('should create table', async () => {
    const table = Table.create(connection.getMetadata(QueryRunnerTestSchema))
    expect(await connection.queryRunner.createTable(table)).toBeUndefined()

    expect(await countTables(connection, table.name)).toEqual(1)
  })

  it('should create materialized view', async () => {
    const tableView = Table.create(connection.getMetadata(QueryRunnerTestViewSchema))
    const tableStorage = Table.create(connection.getMetadata(QueryRunnerTestViewStorageSchema))
    const table = Table.create(connection.getMetadata(QueryRunnerTestSchema))

    await connection.queryRunner.createTable(table, true)
    await connection.queryRunner.createTable(tableStorage, true)

    expect(await connection.queryRunner.createTable(tableView)).toBeUndefined()
    expect(await countTables(connection, tableView.name)).toEqual(1)
  })

  it('should drop materialized view', async () => {
    const tableView = Table.create(connection.getMetadata(QueryRunnerTestViewSchema))
    const tableStorage = Table.create(connection.getMetadata(QueryRunnerTestViewStorageSchema))
    const table = Table.create(connection.getMetadata(QueryRunnerTestSchema))

    await connection.queryRunner.createTable(table, true)
    await connection.queryRunner.createTable(tableStorage, true)
    await connection.queryRunner.createTable(tableView, true)

    expect(await connection.queryRunner.dropTable(tableView.name)).toBeUndefined()
    expect(await countTables(connection, tableView.name)).toEqual(0)
  })

  it('should rename table', async () => {
    const table = Table.create(connection.getMetadata(QueryRunnerTestSchema))
    await connection.queryRunner.createTable(table, true)
    expect(await connection.queryRunner.renameTable(table.name, RENAME_STRING)).toBeUndefined()
    expect(await countTables(connection, RENAME_STRING)).toStrictEqual(1)
    expect(await countTables(connection, table.name)).toEqual(0)
  })

  it('should drop table', async () => {
    const table = Table.create(connection.getMetadata(QueryRunnerTestSchema))
    await connection.queryRunner.createTable(table, true)

    expect(await connection.queryRunner.dropTable(table.name)).toBeUndefined()
    expect(await countTables(connection, table.name)).toEqual(0)
  })
})
