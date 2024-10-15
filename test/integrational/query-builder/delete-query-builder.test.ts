import { Connection } from '../../../src/connection/connection'
import { SchemaMetadata } from '../../../src/metadata/schema-metadata'
import { SelectQueryBuilder } from '../../../src/query-builder/select-query-builder'
import { Table } from '../../../src/schema-builder/table'
import { randomUUID, randomInt } from 'crypto'
import { ConnectionOptions } from '../../things/connection-options'
import { TestSchema } from '../../things/schemas/test-schema'
import { ClickHouseError } from '@clickhouse/client'
import { Column } from '../../../src/decorators/column/column'
import { Schema } from '../../../src/decorators/schema/schema'

/**
 * Schema for this test file
 */
@Schema({ engine: 'MergeTree' })
class DeleteQueryBuilderTestSchema {
  @Column({ type: 'UUID' })
  id: string

  @Column({ type: 'String' })
  name: string

  @Column({ type: 'DateTime' })
  dateOfBirth: string

  @Column({ type: 'Array(Int8)' })
  numericArray: number[]
}

describe('DeleteQueryBuilder (intergrational)', () => {
  let connection: Connection
  let queryBuilder: SelectQueryBuilder
  let metadata: SchemaMetadata
  let tableName: string

  beforeAll(async () => {
    connection = await Connection.initialize({
      ...ConnectionOptions,
      schemas: [DeleteQueryBuilderTestSchema],
    })

    metadata = connection.getMetadata(TestSchema)
    tableName = metadata.tableMetadataArgs.name
  })

  beforeEach(async () => {
    queryBuilder = connection.createQueryBuilder()
    await connection.queryRunner.dropTable(metadata, true)
    await connection.queryRunner.createTable(Table.create(metadata))

    const inserts = [
      {
        id: randomUUID(),
        name: 'name_1',
        numericArray: [1, 1, 0, 3],
        dateOfBirth: '1970-01-02 00:00:45',
      },
    ]
    for (let i = 0; i < 50; i++) {
      inserts.push({
        id: randomUUID(),
        name: randomInt(999999).toString(32),
        numericArray: [i],
        dateOfBirth: '1970-01-01 00:00:30',
      })
    }
    await connection.insert(inserts, metadata.tableMetadataArgs.name)
  })

  it('should delete where name is', async () => {
    expect(
      await queryBuilder
        .delete(tableName)
        .where({
          name: 'name_1',
        })
        .execute(),
    ).toBeUndefined()

    expect(await queryBuilder.select().where({ name: 'name_1' }).execute()).toStrictEqual([])
  })

  it('should delete where array value lower than (plain sql)', async () => {
    expect(await queryBuilder.delete(tableName).where('numericArray[1] < 5').execute()).toBeUndefined()
    expect(await queryBuilder.select().where('numericArray[1] < 5').execute()).toStrictEqual([])
  })

  it('should fail if type is wrong', async () => {
    try {
      await queryBuilder.delete(tableName).where('dateOfBirth = "date of birth"').execute()
    } catch (e) {
      expect(e).toBeInstanceOf(ClickHouseError)
    }
  })

  it('should work with subqueries', async () => {
    expect(
      await queryBuilder
        .delete(tableName)
        .where('numericArray[1] < 5')
        .orWhere((qb) => qb.where('numericArray[1] > 6').andWhere('numericArray[1] < 10'))
        .execute(),
    ).toBeUndefined()
    expect(
      await queryBuilder.select().from(tableName).where('numericArray[1] > 6 AND numericArray[1] < 10').execute(),
    ).toStrictEqual([])
  })
})
