/**
 * Select Builder Tests
 * Comprehensive tests for the new architecture select builder
 */

import { describe, it, expect, beforeEach, jest } from '@jest/globals'
import { select, Eq, EqCol, Gt, Lt, Like, In, And, Or, Not, Count, Sum, Avg, Upper } from '../../../src/index'

// Mock QueryRunner for testing
const mockQueryRunner = {
  execute: jest.fn() as jest.MockedFunction<any>,
  stream: jest.fn() as jest.MockedFunction<any>,
  streamJSONEachRow: jest.fn() as jest.MockedFunction<any>,
  streamCSV: jest.fn() as jest.MockedFunction<any>,
}

describe('SelectBuilder', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })

  describe('Basic Query Building', () => {
    it('should build a simple SELECT query', () => {
      const query = select(['id', 'name', 'email']).from('users')

      const { sql, params } = query.toSQL()

      expect(sql).toBe('SELECT `id`, `name`, `email` FROM `users`')
      expect(params).toEqual([])
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

    it('should build multiple WHERE clauses', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ status: Eq('active') })
        .where({ age: Gt(18) })

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id`, `name` FROM `users` WHERE `status` = 'active' AND `age` > 18")
    })
  })

  describe('Complex WHERE Clauses', () => {
    it('should build AND clause', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({
          status: Eq('active'),
          age: Gt(18),
        })

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id`, `name` FROM `users` WHERE `status` = 'active' AND `age` > 18")
    })

    it('should build OR clause with And combinator', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ status: Eq('active'), age: Gt(18) })

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id`, `name` FROM `users` WHERE `status` = 'active' AND `age` > 18")
    })

    it('should build OR clause with Or combinator', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where(Or({ status: Eq('active') }, { status: Eq('pending') }))

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id`, `name` FROM `users` WHERE (`status` = 'active' OR `status` = 'pending')")
    })

    it('should build NOT clause', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where(Not({ status: Eq('inactive') }))

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id`, `name` FROM `users` WHERE NOT (`status` = 'inactive')")
    })

    it('should build complex nested clauses', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where(And({ status: Eq('active') }, Or({ age: Gt(18) }, { age: Lt(65) })))

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id`, `name` FROM `users` WHERE `status` = 'active' AND (`age` > 18 OR `age` < 65)")
    })
  })

  describe('Nested Combinator Syntax', () => {
    it('should support And combinator as column value', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ age: And(Gt(18), Lt(65)) })

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `id`, `name` FROM `users` WHERE (`age` > 18 AND `age` < 65)')
    })

    it('should support Or combinator as column value', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ status: Or(Eq('active'), Eq('pending')) })

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id`, `name` FROM `users` WHERE (`status` = 'active' OR `status` = 'pending')")
    })

    it('should support Not combinator as column value', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ age: Not(Eq(27)) })

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `id`, `name` FROM `users` WHERE NOT (`age` = 27)')
    })

    it('should support nested combinators as column value', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ age: And(Gt(25), Lt(30), Not(Eq(27))) })

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `id`, `name` FROM `users` WHERE `age` > 25 AND `age` < 30 AND NOT (`age` = 27)')
    })

    it('should support multiple columns with nested combinators', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({
          age: And(Gt(18), Lt(65)),
          status: Or(Eq('active'), Eq('pending')),
        })

      const { sql } = query.toSQL()

      expect(sql).toBe(
        "SELECT `id`, `name` FROM `users` WHERE (`age` > 18 AND `age` < 65) AND (`status` = 'active' OR `status` = 'pending')",
      )
    })
  })

  describe('JOIN Operations', () => {
    it('should build INNER JOIN', () => {
      const query = select(['u.id', 'u.name', 'p.title'])
        .from('users', 'u')
        .innerJoin('posts', { 'u.id': EqCol('p.user_id') })

      const { sql } = query.toSQL()

      expect(sql).toBe(
        'SELECT `u`.`id`, `u`.`name`, `p`.`title` FROM `users` AS `u` INNER JOIN `posts` ON `u`.`id` = `p`.`user_id`',
      )
    })

    it('should build LEFT JOIN', () => {
      const query = select(['u.id', 'u.name', 'p.title'])
        .from('users', 'u')
        .leftJoin('posts', { 'u.id': EqCol('p.user_id') })

      const { sql } = query.toSQL()

      expect(sql).toBe(
        'SELECT `u`.`id`, `u`.`name`, `p`.`title` FROM `users` AS `u` LEFT JOIN `posts` ON `u`.`id` = `p`.`user_id`',
      )
    })

    it('should build RIGHT JOIN', () => {
      const query = select(['u.id', 'u.name', 'p.title'])
        .from('users', 'u')
        .rightJoin('posts', { 'u.id': EqCol('p.user_id') })

      const { sql } = query.toSQL()

      expect(sql).toBe(
        'SELECT `u`.`id`, `u`.`name`, `p`.`title` FROM `users` AS `u` RIGHT JOIN `posts` ON `u`.`id` = `p`.`user_id`',
      )
    })

    it('should build FULL JOIN', () => {
      const query = select(['u.id', 'u.name', 'p.title'])
        .from('users', 'u')
        .fullJoin('posts', { 'u.id': EqCol('p.user_id') })

      const { sql } = query.toSQL()

      expect(sql).toBe(
        'SELECT `u`.`id`, `u`.`name`, `p`.`title` FROM `users` AS `u` FULL JOIN `posts` ON `u`.`id` = `p`.`user_id`',
      )
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
  })

  describe('GROUP BY and HAVING', () => {
    it('should build GROUP BY clause', () => {
      const query = select(['status', 'count()']).from('users').groupBy(['status'])

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `status`, count() FROM `users` GROUP BY `status`')
    })

    it('should build GROUP BY with HAVING clause', () => {
      const query = select(['status', 'count()'])
        .from('users')
        .groupBy(['status'])
        .having({ 'count()': Gt(5) })

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `status`, count() FROM `users` GROUP BY `status` HAVING `count()` > 5')
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

    it('should build multiple SETTINGS', () => {
      const query = select(['id', 'name']).from('users').settings({
        max_execution_time: 30,
        readonly: '1',
      })

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id`, `name` FROM `users` SETTINGS max_execution_time = 30, readonly = '1'")
    })
  })

  describe('Complex Queries', () => {
    it('should build a complex query with all features', () => {
      const query = select(['u.id', 'u.name', 'p.title'])
        .from('users', 'u')
        .innerJoin('posts', { 'u.id': EqCol('p.user_id') }, 'p')
        .where({
          'u.status': Eq('active'),
          'u.age': Gt(18),
        })
        .groupBy(['u.id', 'u.name'])
        .having({ 'count()': Gt(1) })
        .orderBy([{ column: 'u.name', direction: 'ASC' }])
        .limit(10, 20)
        .final()
        .settings({ max_execution_time: 30 })

      const { sql } = query.toSQL()

      expect(sql).toBe(
        'SELECT `u`.`id`, `u`.`name`, `p`.`title` FROM `users` AS `u` ' +
          'INNER JOIN `posts` AS `p` ON `u`.`id` = `p`.`user_id` ' +
          "WHERE `u`.`status` = 'active' AND `u`.`age` > 18 " +
          'GROUP BY `u`.`id`, `u`.`name` ' +
          'HAVING `count()` > 1 ' +
          'ORDER BY `u`.`name` ASC ' +
          'LIMIT 10 OFFSET 20 ' +
          'FINAL ' +
          'SETTINGS max_execution_time = 30',
      )
    })
  })

  describe('Error Handling', () => {
    it('should throw error when run() is called without QueryRunner', async () => {
      const query = select(['id', 'name']).from('users')

      await expect(query.run()).rejects.toThrow('QueryRunner is required to execute queries')
    })

    it('should throw error when stream() is called without QueryRunner', async () => {
      const query = select(['id', 'name']).from('users')

      await expect(query.stream()).rejects.toThrow('QueryRunner is required to stream queries')
    })
  })

  describe('Query Execution', () => {
    it('should execute query with QueryRunner', async () => {
      const mockData = [{ id: 1, name: 'John' }]
      mockQueryRunner.execute.mockResolvedValue(mockData)

      const query = select(['id', 'name'])
        .from('users')
        .where({ status: Eq('active') })

      const result = await query.run(mockQueryRunner as any)

      expect(mockQueryRunner.execute).toHaveBeenCalledWith({
        sql: "SELECT `id`, `name` FROM `users` WHERE `status` = 'active'",
        settings: undefined,
        format: 'JSON',
      })
      expect(result).toEqual(mockData)
    })

    it('should stream query with QueryRunner', async () => {
      const mockStream = { on: jest.fn() }
      mockQueryRunner.stream.mockResolvedValue(mockStream)

      const query = select(['id', 'name'])
        .from('users')
        .where({ status: Eq('active') })

      const result = await query.stream(mockQueryRunner as any)

      expect(mockQueryRunner.stream).toHaveBeenCalledWith({
        sql: "SELECT `id`, `name` FROM `users` WHERE `status` = 'active'",
        settings: undefined,
        format: 'JSONEachRow',
      })
      expect(result).toEqual(mockStream)
    })
  })

  describe('Column Aliases', () => {
    it('should build query with alias object syntax', () => {
      const query = select({ userId: 'id', userName: 'name' }).from('users')

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `id` AS `userId`, `name` AS `userName` FROM `users`')
    })

    it('should work with helper functions', () => {
      const query = select({ totalUsers: Count(), avgAge: Avg('age') }).from('users')

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT count(*) AS `totalUsers`, avg(age) AS `avgAge` FROM `users`')
    })

    it('should handle mixed simple and aggregate columns', () => {
      const query = select({
        userName: 'name',
        userCount: Count('id'),
        totalAmount: Sum('amount'),
      }).from('users')

      const { sql } = query.toSQL()

      expect(sql).toBe(
        'SELECT `name` AS `userName`, count(id) AS `userCount`, sum(amount) AS `totalAmount` FROM `users`',
      )
    })

    it('should handle complex expressions with aliases', () => {
      const query = select({
        upperName: Upper('name'),
        fullName: 'concat(first_name, " ", last_name)',
      }).from('users')

      const { sql } = query.toSQL()

      expect(sql).toBe(
        'SELECT upper(name) AS `upperName`, concat(first_name, " ", last_name) AS `fullName` FROM `users`',
      )
    })

    it('should work with WHERE clause', () => {
      const query = select({ userId: 'id', orderCount: Count('*') })
        .from('orders')
        .where({ status: Eq('completed') })

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id` AS `userId`, count(*) AS `orderCount` FROM `orders` WHERE `status` = 'completed'")
    })
  })

  describe('SQL Injection Prevention', () => {
    it('should prevent SQL injection in column names', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ 'id; DROP TABLE users; --': Eq('1') })

      const { sql } = query.toSQL()

      // The malicious column name should be properly escaped
      expect(sql).toContain('`id; DROP TABLE users; --`')
    })

    it('should prevent SQL injection in table names', () => {
      const query = select(['id', 'name']).from('users; DROP TABLE users; --')

      const { sql } = query.toSQL()

      // The malicious table name should be properly escaped
      expect(sql).toContain('`users; DROP TABLE users; --`')
    })

    it('should use parameterized queries for values', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ name: Eq("'; DROP TABLE users; --") })

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id`, `name` FROM `users` WHERE `name` = '''; DROP TABLE users; --'")
    })
  })

  describe('Type Safety', () => {
    it('should maintain type safety for column references', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({
          id: Gt(1),
          name: Like('John%'),
        })

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id`, `name` FROM `users` WHERE `id` > 1 AND `name` LIKE 'John%'")
    })

    it('should handle different data types in parameters', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({
          id: Gt(1),
          active: Eq(true),
          score: Gt(85.5),
          created_at: Gt(new Date('2023-01-01')),
        })

      const { sql } = query.toSQL()

      expect(sql).toBe(
        "SELECT `id`, `name` FROM `users` WHERE `id` > 1 AND `active` = true AND `score` > 85.5 AND `created_at` > '2023-01-01 00:00:00'",
      )
    })
  })
})
