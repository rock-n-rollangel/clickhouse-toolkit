import { describe, it, expect, beforeAll, afterAll } from '@jest/globals'
import { select, EqCol } from '../../../src/index'
import { testSetup } from '../setup/test-setup'

describe('Advanced JOIN E2E', () => {
  let queryRunner: any

  beforeAll(async () => {
    await testSetup.setup()
    queryRunner = testSetup.getQueryRunner()
  }, 60000)

  afterAll(async () => {
    await testSetup.teardown()
  }, 30000)

  it('LEFT SEMI JOIN returns only users that have at least one order', async () => {
    const rows = await select(['u.id', 'u.name'])
      .from('users', 'u')
      .leftSemiJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
      .run<any>(queryRunner)

    // All 5 fixture users have orders — each user appears at most once in LEFT SEMI output
    const ids = rows.map((r: any) => Number(r.id)).sort((a: number, b: number) => a - b)
    expect(ids).toEqual([1, 2, 3, 4, 5])
  })

  it('LEFT ANTI JOIN returns users with no orders (none in fixture)', async () => {
    const rows = await select(['u.id'])
      .from('users', 'u')
      .leftAntiJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
      .run<any>(queryRunner)

    expect(rows.length).toBe(0)
  })

  it('CROSS JOIN produces 5*5 = 25 rows for users x products', async () => {
    const rows = await select(['u.id', 'p.id'])
      .from('users', 'u')
      .crossJoin('products', 'p')
      .run<any>(queryRunner)

    expect(rows.length).toBe(25)
  })

  it('USING clause joins on shared column name', async () => {
    const rows = await select(['user_id'])
      .from('orders')
      .innerJoin('orders', {}, 'o2', { using: ['user_id'] })
      .limit(1)
      .run<any>(queryRunner)
    expect(rows.length).toBeGreaterThan(0)
  })
})
