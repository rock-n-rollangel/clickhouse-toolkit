import { describe, it, expect, beforeAll, afterAll } from '@jest/globals'
import { select } from '../../../src/index'
import { testSetup } from '../setup/test-setup'

describe('ARRAY JOIN E2E', () => {
  let queryRunner: any

  beforeAll(async () => {
    await testSetup.setup()
    queryRunner = testSetup.getQueryRunner()

    await queryRunner.command({
      sql: `
        CREATE TABLE IF NOT EXISTS array_join_fixture (
          id UInt32,
          tags Array(String)
        ) ENGINE = MergeTree() ORDER BY id
      `,
    })
    await queryRunner.command({ sql: `TRUNCATE TABLE array_join_fixture` })
    await queryRunner.command({
      sql: `INSERT INTO array_join_fixture VALUES (1, ['a','b','c']), (2, ['x']), (3, [])`,
    })
  }, 60000)

  afterAll(async () => {
    try {
      await queryRunner.command({ sql: `DROP TABLE IF EXISTS array_join_fixture` })
    } catch {}
    await testSetup.teardown()
  }, 30000)

  it('ARRAY JOIN unfolds array elements; rows with empty arrays are dropped', async () => {
    const rows = await select(['id', 'tag'])
      .from('array_join_fixture')
      .arrayJoin({ tag: 'tags' })
      .run<any>(queryRunner)

    // id=1 → 3 rows, id=2 → 1 row, id=3 → 0 rows (empty array)
    expect(rows.length).toBe(4)
    const idCounts = rows.reduce<Record<number, number>>((acc: any, r: any) => {
      acc[r.id] = (acc[r.id] || 0) + 1
      return acc
    }, {})
    expect(idCounts[1]).toBe(3)
    expect(idCounts[2]).toBe(1)
    expect(idCounts[3]).toBeUndefined()
  })

  it('LEFT ARRAY JOIN keeps rows with empty arrays', async () => {
    const rows = await select(['id', 'tag'])
      .from('array_join_fixture')
      .leftArrayJoin({ tag: 'tags' })
      .run<any>(queryRunner)

    // 3 + 1 + 1 (empty kept once with default) = 5 rows
    expect(rows.length).toBe(5)
    const ids = rows.map((r: any) => Number(r.id)).sort((a: number, b: number) => a - b)
    expect(ids).toEqual([1, 1, 1, 2, 3])
  })
})
