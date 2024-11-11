import { Connection } from '../../../src/connection/connection'
import { SchemaMetadata } from '../../../src/metadata/schema-metadata'
import { Table } from '../../../src/schema-builder/table'
import { ConnectionOptions } from '../../things/connection-options'
import { SelectQueryBuilder } from '../../../src/query-builder/select-query-builder'
import { randomInt, randomUUID } from 'crypto'
import { Column } from '../../../src/decorators/column/column'
import { Schema } from '../../../src/decorators/schema/schema'
import { SelectStatementError } from '../../../src/errors/select-empty'

/**
 * Schema for this test file
 */
@Schema({ engine: 'MergeTree' })
class SelectQueryBuilderTestSchema {
  @Column({ type: 'UUID' })
  id: string

  @Column({ type: 'String' })
  name: string

  @Column({ type: 'DateTime' })
  dateOfBirth: string

  @Column({ type: 'Array(Int8)' })
  numericArray: number[]
}

@Schema({ engine: 'MergeTree' })
class SelectQueryBuilderTestJoinSchema {
  @Column({ type: 'UUID' })
  joinId: string
}

describe('SelectQueryBuilder (integrational)', () => {
  let connection: Connection
  let queryBuilder: SelectQueryBuilder
  let metadata: SchemaMetadata
  let joinMetadata: SchemaMetadata
  let tableName: string

  beforeAll(async () => {
    connection = await Connection.initialize({
      ...ConnectionOptions,
      schemas: [SelectQueryBuilderTestSchema, SelectQueryBuilderTestJoinSchema],
    })

    metadata = connection.getMetadata(SelectQueryBuilderTestSchema)
    joinMetadata = connection.getMetadata(SelectQueryBuilderTestJoinSchema)
    tableName = metadata.tableMetadataArgs.name
  })

  beforeEach(async () => {
    queryBuilder = connection.createQueryBuilder()
    await connection.queryRunner.dropTable(metadata, true)
    await connection.queryRunner.createTable(Table.create(metadata))

    await connection.queryRunner.dropTable(joinMetadata, true)
    await connection.queryRunner.createTable(Table.create(joinMetadata))

    const inserts = [
      {
        id: randomUUID(),
        name: 'name_1',
        numericArray: [1, 1, 0, 3],
        dateOfBirth: '1970-01-02 00:00:45',
      },
    ]
    for (let i = 0; i < 50; i++) {
      const id = randomUUID()
      inserts.push({
        id: id,
        name: randomInt(999999).toString(32),
        numericArray: [randomInt(100), randomInt(50), randomInt(25), randomInt(12)],
        dateOfBirth: '1970-01-01 00:00:30',
      })
    }
    await connection.insert(inserts, tableName)
    await connection.insert(
      inserts.map((insert) => ({ joinId: insert.id })),
      joinMetadata.tableMetadataArgs.name,
    )
  })

  it('should get given name', async () => {
    expect(
      await queryBuilder
        .select('name')
        .from(tableName)
        .where({
          name: 'name_1',
        })
        .execute(),
    ).toStrictEqual([{ name: 'name_1' }])
  })

  it('should get when array like given', async () => {
    expect(
      await queryBuilder
        .select('numericArray')
        .from(tableName)
        .where({
          numericArray: [1, 1, 0, 3],
        })
        .execute(),
    ).toStrictEqual([
      {
        numericArray: [1, 1, 0, 3],
      },
    ])
  })

  it('should get when array first element greater then 50 or where name is like and third element is', async () => {
    const result = await queryBuilder
      .select(['name', 'numericArray'])
      .from(tableName)
      .where('numericArray[1] > 50')
      .orWhere((qb) =>
        qb
          .where({
            name: 'name_1',
          })
          .orWhere('numericArray[3] = 0'),
      )
      .execute<{ name: string; numericArray: number[] }>()

    for (const item of result) {
      expect(item.name === 'name_1' || item.numericArray[0] > 50 || item.numericArray[2] === 0).toBeTruthy()
    }
  })

  it('should fail with empty select', async () => {
    try {
      await queryBuilder.select([]).from(tableName).execute()
    } catch (e) {
      expect(e).toBeInstanceOf(SelectStatementError)
    }
  })

  it('should join table', async () => {
    const result = await queryBuilder
      .select(['id', 't2.joinId'])
      .from(tableName)
      .innerJoin(joinMetadata.tableMetadataArgs.name, 't2', `t2.joinId = ${tableName}.id`)
      .execute<{ id: string; joinId: string }>()

    for (const item of result) {
      expect(item.id).toEqual(item.joinId)
    }
  })

  it('should give sql X times', () => {
    const qb = queryBuilder
      .select(['id'])
      .from(tableName, 't1')
      .where('id = :id', { id: '123' })
      .innerJoin(joinMetadata.tableMetadataArgs.name, 't2', 't2.joinId = t1.id')

    for (let x = 3; x > 0; x--) qb.toSql()
  })

  it('should work with values containing ":"', async () => {
    const result = await queryBuilder
      .select()
      .from(tableName, 't1')
      .where(`t1.dateOfBirth = '1970-01-02 00:00:45'`)
      .execute<{ name: string }>()

    expect(result.pop()?.name).toContain('name_1')
  })

  it('should work with multiple parameters', async () => {
    const result = await queryBuilder
      .select()
      .from(tableName, 't1')
      .where(`t1.dateOfBirth = :dateOfBirth`, { dateOfBirth: '1970-01-02 00:00:45' })
      .andWhere(`t1.name = :name`, { name: 'name_1' })
      .execute<{ name: string }>()

    expect(result.pop()?.name).toContain('name_1')
  })

  it('should order descending correctly', async () => {
    const result = await queryBuilder
      .select()
      .from(tableName, 't1')
      .orderBy('dateOfBirth', 'desc')
      .execute<{ name: string }>()

    expect(result[0].name).toBe('name_1')
  })

  it('should order ascending correctly', async () => {
    const result = await queryBuilder
      .select()
      .from(tableName, 't1')
      .orderBy('dateOfBirth', 'asc')
      .execute<{ name: string }>()

    expect(result[0].name === 'name_1').toBeFalsy()
  })
})
