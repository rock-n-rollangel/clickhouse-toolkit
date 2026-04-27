import { describe, it, expect } from '@jest/globals'
import { select, Sum, Count, RowNumber, Rank, Lag, FirstValue } from '../../../src/index'

describe('Window functions', () => {
  describe('inline OVER()', () => {
    it('renders RowNumber with PARTITION BY and ORDER BY', () => {
      const { sql } = select({
        rn: RowNumber().over({
          partitionBy: ['user_id'],
          orderBy: [{ column: { type: 'column', name: 'ts' }, direction: 'DESC' }],
        }),
      })
        .from('events')
        .toSQL()

      expect(sql).toBe(
        'SELECT row_number() OVER (PARTITION BY `user_id` ORDER BY `ts` DESC) AS `rn` FROM `events`',
      )
    })

    it('renders Sum().over() running total with frame', () => {
      const { sql } = select({
        running: Sum('amount').over({
          partitionBy: ['user_id'],
          orderBy: [{ column: { type: 'column', name: 'ts' }, direction: 'ASC' }],
          frame: { type: 'ROWS', start: 'UNBOUNDED PRECEDING', end: 'CURRENT ROW' },
        }),
      })
        .from('events')
        .toSQL()

      expect(sql).toBe(
        'SELECT sum(amount) OVER (PARTITION BY `user_id` ORDER BY `ts` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `running` FROM `events`',
      )
    })

    it('renders Rank with single-bound ROWS frame', () => {
      const { sql } = select({
        r: Rank().over({
          orderBy: [{ column: { type: 'column', name: 'score' }, direction: 'DESC' }],
          frame: { type: 'ROWS', start: { preceding: 5 } },
        }),
      })
        .from('scores')
        .toSQL()

      expect(sql).toBe(
        'SELECT rank() OVER (ORDER BY `score` DESC ROWS 5 PRECEDING) AS `r` FROM `scores`',
      )
    })

    it('renders Lag with offset and default', () => {
      const { sql } = select({
        prev: Lag('amount', 1, 0).over({
          partitionBy: ['user_id'],
          orderBy: [{ column: { type: 'column', name: 'ts' }, direction: 'ASC' }],
        }),
      })
        .from('events')
        .toSQL()

      expect(sql).toBe(
        'SELECT lagInFrame(amount, 1, 0) OVER (PARTITION BY `user_id` ORDER BY `ts` ASC) AS `prev` FROM `events`',
      )
    })

    it('renders empty spec as empty parens', () => {
      const { sql } = select({ rn: RowNumber().over({}) })
        .from('t')
        .toSQL()

      expect(sql).toBe('SELECT row_number() OVER () AS `rn` FROM `t`')
    })
  })

  describe('named windows', () => {
    it('renders WINDOW clause and OVER name reference', () => {
      const { sql } = select({
        rn: RowNumber().over('w'),
        rk: Rank().over('w'),
      })
        .from('events')
        .window('w', {
          partitionBy: ['user_id'],
          orderBy: [{ column: { type: 'column', name: 'ts' }, direction: 'DESC' }],
        })
        .toSQL()

      expect(sql).toBe(
        'SELECT row_number() OVER w AS `rn`, rank() OVER w AS `rk` FROM `events` WINDOW w AS (PARTITION BY `user_id` ORDER BY `ts` DESC)',
      )
    })

    it('throws when referencing an undeclared named window', () => {
      const builder = select({ rn: RowNumber().over('missing') }).from('t')
      expect(() => builder.toSQL()).toThrow(/Window 'missing' referenced but not declared/)
    })
  })

  describe('frame validation', () => {
    it('rejects FOLLOWING start with PRECEDING end', () => {
      const builder = select({
        s: Sum('x').over({
          orderBy: [{ column: { type: 'column', name: 'id' }, direction: 'ASC' }],
          frame: { type: 'ROWS', start: { following: 1 }, end: { preceding: 1 } },
        }),
      }).from('t')
      expect(() => builder.toSQL()).toThrow(/end cannot be PRECEDING when start is FOLLOWING/)
    })

    it('rejects swapped PRECEDING offsets', () => {
      const builder = select({
        s: Sum('x').over({
          orderBy: [{ column: { type: 'column', name: 'id' }, direction: 'ASC' }],
          frame: { type: 'ROWS', start: { preceding: 2 }, end: { preceding: 5 } },
        }),
      }).from('t')
      expect(() => builder.toSQL()).toThrow(/PRECEDING offset must be <= start/)
    })
  })

  describe('mixed with aggregates', () => {
    it('aggregate function used with .over() does not break plain GROUP BY use', () => {
      const { sql } = select({ total: Sum('amount') }).from('orders').groupBy(['user_id']).toSQL()
      expect(sql).toBe('SELECT sum(amount) AS `total` FROM `orders` GROUP BY `user_id`')
    })
  })
})
