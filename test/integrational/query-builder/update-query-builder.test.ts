import { Connection } from '../../../src/connection/connection'
import { SchemaMetadata } from '../../../src/metadata/schema-metadata'
import { SelectQueryBuilder } from '../../../src/query-builder/select-query-builder'
import { Table } from '../../../src/schema-builder/table'
import { randomUUID, randomInt } from 'crypto'
import { ConnectionOptions } from '../../things/connection-options'
import { WrongParameterColumnNameError } from '../../../src/errors/wrong-parameter-column-name'
import { Column } from '../../../src/decorators/column/column'
import { Schema } from '../../../src/decorators/schema/schema'
import { UpdateValueNotSetError } from '../../../src/errors/update-value-not-set'
import { NotAllParametersWasPassedError } from '../../../src/errors/not-all-parameters-was-passed'
import { SchemaMetadataNotFoundError } from '../../../src/errors/schema-not-found'
import { TableNameError } from '../../../src/errors/table-name'

/**
 * Schema for this test file
 */
@Schema({ engine: 'MergeTree' })
class UpdateQueryBuilderTestSchema {
  @Column({ type: 'UUID' })
  id: string

  @Column({ type: 'String' })
  name: string

  @Column({ type: 'DateTime' })
  dateOfBirth: string

  @Column({ type: 'Array(Int8)' })
  numericArray: number[]
}

describe('UpdateQueryBuilder (integrational)', () => {
  let connection: Connection
  let queryBuilder: SelectQueryBuilder
  let metadata: SchemaMetadata
  let tableName: string

  beforeAll(async () => {
    connection = await Connection.initialize({
      ...ConnectionOptions,
      logging: true,
      entities: [UpdateQueryBuilderTestSchema],
    })

    metadata = connection.getMetadata(UpdateQueryBuilderTestSchema)
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
    await connection.insert(inserts, tableName)
  })

  it('should update by name', async () => {
    await queryBuilder
      .update(tableName)
      .set({
        name: 'name_2',
        numericArray: [1, 2, 3, 4, 5],
      })
      .where({
        name: 'name_1',
      })
      .execute()

    const result = await queryBuilder
      .select(['name', 'numericArray'])
      .from(tableName)
      .where({
        name: 'name_2',
      })
      .execute()

    expect(result).toStrictEqual([
      {
        name: 'name_2',
        numericArray: [1, 2, 3, 4, 5],
      },
    ])
  })

  it('should fail with wrong column name', async () => {
    try {
      await queryBuilder
        .update(tableName)
        .set({
          naming: 'wrong!',
        })
        .where({
          name: 'name_1',
        })
        .execute()
    } catch (e) {
      expect(e).toBeInstanceOf(WrongParameterColumnNameError)
    }
  })

  it('should work with subqueris', async () => {
    await queryBuilder
      .update(tableName)
      .set({
        numericArray: [1],
      })
      .where('numericArray[4] > 0')
      .andWhere((qb) => qb.where('numericArray[1] > 5').andWhere('numericArray[1] < 15'))
      .execute()

    expect(
      await queryBuilder
        .select()
        .where('numericArray[4] > 0')
        .andWhere((qb) => qb.where('numericArray[1] > 5').andWhere('numericArray[1] < 15'))
        .execute(),
    ).toStrictEqual([])
  })

  it('should fail without update set', async () => {
    try {
      await queryBuilder.update(tableName).execute()
    } catch (e) {
      expect(e).toBeInstanceOf(UpdateValueNotSetError)
    }
  })

  it('should fail without params', async () => {
    try {
      await queryBuilder.update(tableName).set({ name: 'name_2' }).where('name = :name', {}).execute()
    } catch (e) {
      expect(e).toBeInstanceOf(NotAllParametersWasPassedError)
    }
  })

  it('should fail coz wrong table name', async () => {
    try {
      await queryBuilder.update('_').set({ _: 1 }).execute()
    } catch (e) {
      expect(e).toBeInstanceOf(SchemaMetadataNotFoundError)
    }
  })

  it('should fail without table name', async () => {
    try {
      await queryBuilder.select().execute()
    } catch (e) {
      expect(e).toBeInstanceOf(TableNameError)
    }
  })
})
