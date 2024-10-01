import { Connection } from '../../../src/connection/connection'
import { ConnectionOptions } from '../../things/connection-options'
import { InsertQueryBuilder } from '../../../src/query-builder/insert-query-builder'
import { randomUUID } from 'crypto'
import { SchemaMetadata } from '../../../src/metadata/schema-metadata'
import { Table } from '../../../src/schema-builder/table'
import { Schema } from '../../../src/decorators/schema/schema'
import { Column } from '../../../src/decorators/column/column'

/**
 * Schema for this test file
 */
@Schema({ engine: 'MergeTree' })
class InsertQueryBuilderTestSchema {
  @Column({ type: 'UUID' })
  id: string

  @Column({ type: 'String' })
  name: string

  @Column({ type: 'DateTime' })
  dateOfBirth: string

  @Column({ type: 'Array(Int8)' })
  numericArray: number[]
}

describe('InsertQueryBuilder (integrational)', () => {
  let connection: Connection
  let queryBuilder: InsertQueryBuilder
  let metadata: SchemaMetadata
  let tableName: string

  beforeAll(async () => {
    connection = await Connection.initialize({
      ...ConnectionOptions,
      entities: [InsertQueryBuilderTestSchema],
    })

    metadata = connection.getMetadata(InsertQueryBuilderTestSchema)
    tableName = metadata.tableMetadataArgs.name
  })

  beforeEach(async () => {
    queryBuilder = connection.createQueryBuilder().insert()
    await connection.queryRunner.dropTable(metadata, true)
    await connection.queryRunner.createTable(Table.create(metadata))
  })

  it('should insert data', async () => {
    expect(
      await queryBuilder
        .into(tableName)
        .values({
          id: randomUUID(),
          name: 'name_1',
          dateOfBirth: '1970-01-01 00:00:00',
          numericArray: [1, 2, 3, 4],
        })
        .execute(),
    ).toBeUndefined()

    expect(await connection.query<{ count: string }>(`SELECT COUNT(*) AS count FROM ${tableName}`)).toStrictEqual([
      { count: '1' },
    ])
  })

  it('should insert multiple rows', async () => {
    expect(
      await queryBuilder
        .into(tableName)
        .values([
          {
            id: randomUUID(),
            name: 'name_1',
            dateOfBirth: '1970-01-01 00:00:00',
            numericArray: [1, 2, 3, 4],
          },
          {
            id: randomUUID(),
            name: 'name_2',
            dateOfBirth: '1970-01-01 00:00:01',
            numericArray: [1, 2, 3, 5],
          },
        ])
        .execute(),
    ).toBeUndefined()

    expect(await connection.query<{ count: string }>(`SELECT COUNT(*) AS count FROM ${tableName}`)).toStrictEqual([
      { count: '2' },
    ])
  })

  it('should insert with SELECT FROM', async () => {
    // preload data
    await connection.insert([{ name: 'name_1', id: randomUUID(), dateOfBirth: '1970-01-01 00:00:13' }], tableName)

    expect(
      await queryBuilder
        .into(tableName)
        .values((qb) => qb.select().from(tableName))
        .execute(),
    ).toBeUndefined()
    expect(await connection.query<{ count: string }>(`SELECT COUNT(*) AS count FROM ${tableName}`)).toStrictEqual([
      { count: '2' },
    ])
  })

  it('should insert without data provided', async () => {
    expect(await queryBuilder.into(tableName).values([{}, {}]).execute())
    expect(await connection.query<{ count: string }>(`SELECT COUNT(*) AS count FROM ${tableName}`)).toStrictEqual([
      { count: '2' },
    ])
  })
})
