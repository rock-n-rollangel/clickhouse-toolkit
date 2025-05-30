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
  let queryBuilder: SelectQueryBuilder<SelectQueryBuilderTestSchema>
  let metadata: SchemaMetadata
  let joinMetadata: SchemaMetadata
  let tableName: string
  let inserts: Array<any>

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

    inserts = [
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
      inserts.filter((v, i) => i % 2 === 0).map((insert) => ({ joinId: insert.id })),
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

  it('should offset properly', async () => {
    const result = await queryBuilder.select().from(tableName, 't1').offset(1).execute<{ name: string }>()
    expect(result.length).toBe(50)
  })

  it('should left join column and select all records', async () => {
    const result = await queryBuilder
      .select(['t1.id as id', 't2.joinId as ji'])
      .from(tableName, 't1')
      .leftJoin(joinMetadata.tableMetadataArgs.name, 't2', 't1.id = t2.joinId')
      .execute<{ id: string; ji: string }>()

    expect(result.length).toBe(inserts.length)

    result.forEach((v, i) => {
      // Because this field can not be null,
      // and default values for UUID is: 00000000-0000-0000-0000-000000000000
      if (i % 2 !== 0) expect(/[0-]+/.test(v.ji)).toBeTruthy()
    })
  })

  it('should add subqueries', async () => {
    const [result] = await queryBuilder
      .select((qb) => qb.select('COUNT(id)').from(tableName), 'count_id')
      .from(tableName, 't1')
      .groupBy('count_id')
      .orderBy('count_id')
      .limit(1)
      .execute<{ count_id: number }>()

    expect(+result.count_id).toBe(inserts.length)
  })

  it('should map result to target', async () => {
    const result = await queryBuilder.select().from(tableName).execute<SelectQueryBuilderTestSchema>(true)

    expect(result.length).toBe(inserts.length)
    expect(result[0].id).toBe(inserts[0].id)
    expect(result[0].name).toBe(inserts[0].name)
    expect(result[0].dateOfBirth).toBe(inserts[0].dateOfBirth)
    expect(result[0].numericArray).toStrictEqual(inserts[0].numericArray)
  })
})
