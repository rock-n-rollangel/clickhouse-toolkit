/**
 * Select Builder Tests - Simplified
 * Tests that match the current API implementation
 */

import { describe, it, expect, beforeEach, jest } from '@jest/globals'
import { select, Eq, Gt, Lt, Like, In, Raw } from '../../../src/index'

describe('SelectBuilder - Basic Functionality', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })

  describe('Basic Query Building', () => {
    it('should build a simple SELECT query', () => {
      const query = select(['id', 'name', 'email']).from('users')

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `id`, `name`, `email` FROM `users`')
    })

    it('should build a SELECT query with single column', () => {
      const query = select(['id']).from('users')

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `id` FROM `users`')
    })

    it('should build a SELECT * query', () => {
      const query = select(['*']).from('users')

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT * FROM `users`')
    })
  })

  describe('WHERE Clause Building', () => {
    it('should build WHERE clause with Eq operator', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ status: Eq('active') })

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id`, `name` FROM `users` WHERE `status` = 'active'")
    })

    it('should build WHERE clause with Gt operator', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ age: Gt(18) })

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `id`, `name` FROM `users` WHERE `age` > 18')
    })

    it('should build WHERE clause with Lt operator', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ age: Lt(65) })

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `id`, `name` FROM `users` WHERE `age` < 65')
    })

    it('should build WHERE clause with Like operator', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ name: Like('John%') })

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id`, `name` FROM `users` WHERE `name` LIKE 'John%'")
    })

    it('should build WHERE clause with In operator', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ status: In(['active', 'pending']) })

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id`, `name` FROM `users` WHERE `status` IN ('active', 'pending')")
    })

    it('should build multiple WHERE clauses (AND)', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({
          status: Eq('active'),
          age: Gt(18),
        })

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id`, `name` FROM `users` WHERE `status` = 'active' AND `age` > 18")
    })
  })

  describe('ORDER BY Clause', () => {
    it('should build ORDER BY clause', () => {
      const query = select(['id', 'name'])
        .from('users')
        .orderBy([{ column: 'name', direction: 'ASC' }])

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `id`, `name` FROM `users` ORDER BY `name` ASC')
    })

    it('should build ORDER BY clause with DESC', () => {
      const query = select(['id', 'name'])
        .from('users')
        .orderBy([{ column: 'created_at', direction: 'DESC' }])

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `id`, `name` FROM `users` ORDER BY `created_at` DESC')
    })

    it('should build multiple ORDER BY clauses', () => {
      const query = select(['id', 'name'])
        .from('users')
        .orderBy([
          { column: 'status', direction: 'ASC' },
          { column: 'created_at', direction: 'DESC' },
        ])

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `id`, `name` FROM `users` ORDER BY `status` ASC, `created_at` DESC')
    })
  })

  describe('LIMIT and OFFSET', () => {
    it('should build LIMIT clause', () => {
      const query = select(['id', 'name']).from('users').limit(10)

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `id`, `name` FROM `users` LIMIT 10')
    })

    it('should build LIMIT and OFFSET clauses', () => {
      const query = select(['id', 'name']).from('users').limit(10, 20)

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `id`, `name` FROM `users` LIMIT 10 OFFSET 20')
    })

    // `limit(0)` means "no rows". A truthiness test used to drop the clause
    // entirely, so a paginator that computed 0 silently received every row.
    it('renders LIMIT 0 rather than dropping the clause', () => {
      expect(select(['id']).from('users').limit(0).toSQL().sql).toBe('SELECT `id` FROM `users` LIMIT 0')
      expect(select(['id']).from('users').limit(0, 5).toSQL().sql).toBe(
        'SELECT `id` FROM `users` LIMIT 0 OFFSET 5',
      )
    })

    it('omits the clause entirely when no limit was set', () => {
      expect(select(['id']).from('users').toSQL().sql).toBe('SELECT `id` FROM `users`')
    })

    // LIMIT/OFFSET are interpolated straight into the SQL string, so a caller
    // reaching the renderer without passing through TypeScript — a JS consumer,
    // a value out of JSON or a query string, a hand-built AST — could inject.
    // These use the real payloads, not toy strings.
    it('rejects a string LIMIT instead of interpolating it as SQL', () => {
      expect(() =>
        select(['id'])
          .from('users')
          .limit('1 UNION ALL SELECT 999' as unknown as number)
          .toSQL(),
      ).toThrow(/Invalid limit/)
    })

    it('rejects a string OFFSET instead of appending a second statement', () => {
      expect(() =>
        select(['id'])
          .from('users')
          .limit(10, '5; DROP TABLE x' as unknown as number)
          .toSQL(),
      ).toThrow(/Invalid offset/)
    })

    it.each([
      ['negative', -5],
      ['fractional', 2.5],
      ['beyond safe-integer range', 1e21],
      ['NaN', NaN],
      ['Infinity', Infinity],
    ])('rejects a %s limit', (_label, value) => {
      expect(() =>
        select(['id'])
          .from('users')
          .limit(value as number)
          .toSQL(),
      ).toThrow(/Invalid limit/)
    })
  })

  describe('GROUP BY and HAVING', () => {
    it('should build GROUP BY clause', () => {
      const query = select(['status', Raw('count()')]).from('users').groupBy(['status'])

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `status`, count() FROM `users` GROUP BY `status`')
    })

    it('should build GROUP BY with HAVING clause', () => {
      const query = select(['status', Raw('count()')])
        .from('users')
        .groupBy(['status'])
        .having(Raw('count() > 5'))

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `status`, count() FROM `users` GROUP BY `status` HAVING count() > 5')
    })
  })

  describe('ClickHouse Specific Features', () => {
    it('should build FINAL modifier', () => {
      const query = select(['id', 'name']).from('users').final()

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `id`, `name` FROM `users` FINAL')
    })

    it('should build SETTINGS clause', () => {
      const query = select(['id', 'name']).from('users').settings({ max_execution_time: 30 })

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `id`, `name` FROM `users` SETTINGS max_execution_time = 30')
    })
  })
})
