import { Connection } from '../../../src/connection/connection'
import { Column } from '../../../src/decorators/column/column'
import { Schema } from '../../../src/decorators/schema/schema'
import { ConnectionOptions } from '../../things/connection-options'
import { SchemaBuilder } from '../../../src/schema-builder/schema-builder'
import { countTables } from '../../things/count-tables'
import { ClickHouseError } from '@clickhouse/client'
import { Table } from '../../../src/schema-builder/table'

@Schema({ engine: 'MergeTree' })
class SchemaBuilderTestSchema {
  @Column({ type: 'String' })
  someColumn: string

  @Column({ type: 'DateTime', orderBy: true, primary: true, function: 'toYYYYMM' })
  createdAt: string
}

@Schema({ engine: 'MergeTree' })
class SchemaBuilderTestViewStorageSchema {
  @Column({ type: 'String' })
  someColumn: string
}

@Schema({
  materialized: true,
  materializedQuery: (qb) => qb.select('someColumn').from('schema_builder_test_schema'),
  materializedTo: 'schema_builder_test_view_storage_schema',
})
class SchemaBuilderTestViewSchema {}

describe('SchemaBuilder (integrational)', () => {
  let connection: Connection
  let schemaBuilder: SchemaBuilder

  beforeAll(async () => {
    connection = await Connection.initialize({
      ...ConnectionOptions,
      schemas: [SchemaBuilderTestSchema, SchemaBuilderTestViewStorageSchema, SchemaBuilderTestViewSchema],
    })

    schemaBuilder = connection.createSchemaBuilder()
  })

  beforeEach(async () => {
    for (const target of [SchemaBuilderTestSchema, SchemaBuilderTestViewSchema, SchemaBuilderTestViewStorageSchema]) {
      const metadata = connection.getMetadata(target)
      await connection.queryRunner.dropTable(metadata.tableMetadataArgs.name, true)
    }
  })

  it('should create tables', async () => {
    const metadata = connection.getMetadata(SchemaBuilderTestSchema)

    expect(await schemaBuilder.createTable(metadata)).toBeUndefined()

    expect(await countTables(connection, metadata.tableMetadataArgs.name)).toEqual(1)
  })

  it('should rename table and save data', async () => {
    const metadata = connection.getMetadata(SchemaBuilderTestSchema)

    await connection.queryRunner.createTable(Table.create(metadata), true)

    await connection.insert([{ name: '1_' }, { name: '2_' }], metadata.tableMetadataArgs.name)

    expect(await schemaBuilder.backupTable(metadata))
    expect(await countTables(connection, SchemaBuilder.tempPrefix + metadata.tableMetadataArgs.name)).toEqual(1)
    expect(
      await connection.query<{ count: number }>(`
        SELECT COUNT(*) AS count FROM ${SchemaBuilder.tempPrefix + metadata.tableMetadataArgs.name}
      `),
    ).toStrictEqual([{ count: '2' }])
  })

  it('should restore backup', async () => {
    const metadata = connection.getMetadata(SchemaBuilderTestSchema)

    await connection.queryRunner.createTable(Table.create(metadata), true)

    await connection.insert([{ name: '1_' }, { name: '2_' }], metadata.tableMetadataArgs.name)

    expect(await schemaBuilder.backupTable(metadata)).toBeUndefined()
    expect(await schemaBuilder.createTable(metadata)).toBeUndefined()
    expect(await schemaBuilder.restoreBackup(metadata)).toBeUndefined()
    expect(
      await connection.query(`
        SELECT COUNT(*) AS count FROM ${metadata.tableMetadataArgs.name}
      `),
    ).toStrictEqual([{ count: '2' }])
  })

  it('should create and drop view', async () => {
    const fromMetadata = connection.getMetadata(SchemaBuilderTestSchema)
    const toMetadata = connection.getMetadata(SchemaBuilderTestViewStorageSchema)
    const viewMetadata = connection.getMetadata(SchemaBuilderTestViewSchema)

    await connection.queryRunner.createTable(Table.create(fromMetadata), true)
    await connection.queryRunner.createTable(Table.create(toMetadata))

    expect(await schemaBuilder.createMaterializedView(viewMetadata)).toBeUndefined()
    expect(await countTables(connection, viewMetadata.tableMetadataArgs.name)).toEqual(1)

    expect(await schemaBuilder.dropMaterializedView(viewMetadata)).toBeUndefined()
    expect(await countTables(connection, viewMetadata.tableMetadataArgs.name)).toEqual(0)
  })

  it('should run create tables and views saving data', async () => {
    const metadata = connection.getMetadata(SchemaBuilderTestSchema)
    expect(await schemaBuilder.synchronize()).toBeUndefined()

    await connection.insert([{ name: '1' }, { name: '2' }], metadata.tableMetadataArgs.name)

    // run synchronize again to run back up
    await schemaBuilder.synchronize()

    for (const metadata of connection.schemaMetadatas) {
      expect(await countTables(connection, metadata.tableMetadataArgs.name)).toEqual(1)
    }

    expect(
      await connection.query<{ count: string }>(`
        SELECT COUNT(*) AS count FROM ${metadata.tableMetadataArgs.name}
      `),
    ).toStrictEqual([{ count: '2' }])
  })

  /**
   * We can't use 'INSERT INTO <table> SELECT * FROM <table>', when tables have different columns count
   */
  it('should throw error', async () => {
    const metadata = connection.getMetadata(SchemaBuilderTestSchema)

    await connection.queryRunner.createTable(Table.create(metadata), true)

    metadata.columnMetadataArgs.push({
      propertyName: 'column_1',
      target: SchemaBuilderTestSchema,
      options: {
        name: 'column_1',
        type: 'Nullable(Boolean)',
      },
    })

    await schemaBuilder.backupTable(metadata)
    await schemaBuilder.createTable(metadata)

    try {
      await schemaBuilder.restoreBackup(metadata)
    } catch (e) {
      expect(e).toBeInstanceOf(ClickHouseError)
    }
  })
})
