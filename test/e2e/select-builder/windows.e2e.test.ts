/**
 * Window Function E2E Tests
 * Tests window function support with real ClickHouse instance
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals'
import { select, Sum, RowNumber } from '../../../src/index'
import { testSetup } from '../setup/test-setup'

describe('Window Function E2E Tests', () => {
  let queryRunner: any

  beforeAll(async () => {
    await testSetup.setup()
    queryRunner = testSetup.getQueryRunner()
  }, 60000)

  afterAll(async () => {
    await testSetup.teardown()
  }, 30000)

  beforeEach(() => {
    if (!queryRunner) {
      queryRunner = testSetup.getQueryRunner()
    }
  })

  it('row_number per user, ordered by amount DESC', async () => {
    const rows = await select({
      user_id: 'user_id',
      amount: 'amount',
      rn: RowNumber().over({
        partitionBy: ['user_id'],
        orderBy: [{ column: { type: 'column' as const, name: 'amount' }, direction: 'DESC' as const }],
      }),
    })
      .from('orders')
      .run<any>(queryRunner)

    // user 1 has two orders (999.99 and 29.99) — row numbers 1 and 2
    const userOneRows = rows
      .filter((r: any) => Number(r.user_id) === 1)
      .sort((a: any, b: any) => Number(a.rn) - Number(b.rn))
    expect(userOneRows.length).toBe(2)
    expect(Number(userOneRows[0].rn)).toBe(1)
    expect(Number(userOneRows[0].amount)).toBeCloseTo(999.99, 2)
    expect(Number(userOneRows[1].rn)).toBe(2)
    expect(Number(userOneRows[1].amount)).toBeCloseTo(29.99, 2)
  })

  it('running total per user with named window', async () => {
    const rows = await select({
      user_id: 'user_id',
      amount: 'amount',
      running: Sum('amount').over('w'),
    })
      .from('orders')
      .window('w', {
        partitionBy: ['user_id'],
        orderBy: [{ column: { type: 'column' as const, name: 'id' }, direction: 'ASC' as const }],
        frame: { type: 'ROWS', start: 'UNBOUNDED PRECEDING', end: 'CURRENT ROW' },
      })
      .run<any>(queryRunner)

    // For user 1, the running totals should be (in ascending id order): ~999.99 and ~1029.98
    const userOneRows = rows.filter((r: any) => Number(r.user_id) === 1)
    expect(userOneRows.length).toBe(2)
    const totals = userOneRows.map((r: any) => Number(r.running)).sort((a: number, b: number) => a - b)
    expect(totals[0]).toBeCloseTo(999.99, 2)
    expect(totals[1]).toBeCloseTo(999.99 + 29.99, 2)
  })
})
