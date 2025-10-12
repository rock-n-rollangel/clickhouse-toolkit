/**
 * SelectBuilder E2E Tests
 * Tests SelectBuilder functionality with real ClickHouse instance
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals'
import {
  select,
  Eq,
  Gt,
  Gte,
  Lt,
  In,
  Between,
  Like,
  IsNotNull,
  And,
  Or,
  Not,
  EqCol,
  Count,
  Sum,
} from '../../../src/index'
import { testSetup } from '../setup/test-setup'

describe('SelectBuilder E2E Tests', () => {
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

  describe('Basic Select Operations', () => {
    it('should execute simple SELECT queries', async () => {
      const results = await select(['id', 'name']).from('users').run(queryRunner)

      expect(Array.isArray(results)).toBe(true)
      expect(results.length).toBeGreaterThan(0)
      expect(results[0]).toHaveProperty('id')
      expect(results[0]).toHaveProperty('name')
    })

    it('should select all columns with wildcard', async () => {
      const results = await select(['*']).from('users').run(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      expect(results[0]).toHaveProperty('id')
      expect(results[0]).toHaveProperty('name')
      expect(results[0]).toHaveProperty('email')
      expect(results[0]).toHaveProperty('age')
      expect(results[0]).toHaveProperty('country')
    })

    it('should handle table aliases', async () => {
      const results = await select(['u.id', 'u.name']).from('users', 'u').run(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      expect(results[0]).toHaveProperty('id')
      expect(results[0]).toHaveProperty('name')
    })

    it('should apply LIMIT clauses', async () => {
      const results = await select(['id', 'name']).from('users').limit(2).run(queryRunner)

      expect(results.length).toBe(2)
    })

    it('should apply ORDER BY clauses', async () => {
      const results = await select(['id', 'name', 'age'])
        .from('users')
        .orderBy([{ column: 'age', direction: 'DESC' }])
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(1)
      expect(results[0].age).toBeGreaterThanOrEqual(results[1].age)
    })

    it('should apply multiple ORDER BY clauses', async () => {
      const results = await select(['id', 'name', 'age', 'country'])
        .from('users')
        .orderBy([
          { column: 'country', direction: 'ASC' },
          { column: 'age', direction: 'DESC' },
        ])
        .run(queryRunner)

      expect(results.length).toBeGreaterThan(1)
      // Results should be ordered by country first, then by age
    })
  })

  describe('WHERE Clause Operations', () => {
    it('should filter with equality operator', async () => {
      const results = await select(['id', 'name'])
        .from('users')
        .where({ id: Eq(1) })
        .run<any>(queryRunner)

      expect(results).toHaveLength(1)
      expect(results[0].id).toBe(1)
      expect(results[0].name).toBe('John Doe')
    })

    it('should filter with multiple conditions', async () => {
      const results = await select(['id', 'name', 'age', 'country'])
        .from('users')
        .where({
          age: Gt(25),
          country: Eq('US'),
        })
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      results.forEach((result) => {
        expect(result.age).toBeGreaterThan(25)
        expect(result.country).toBe('US')
      })
    })

    it('should filter with IN operator', async () => {
      const results = await select(['id', 'name', 'country'])
        .from('users')
        .where({ country: In(['US', 'CA']) })
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      results.forEach((result) => {
        expect(['US', 'CA']).toContain(result.country)
      })
    })

    it('should filter with BETWEEN operator', async () => {
      const results = await select(['id', 'name', 'age'])
        .from('users')
        .where({ age: Between(25, 35) })
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      results.forEach((result) => {
        expect(result.age).toBeGreaterThanOrEqual(25)
        expect(result.age).toBeLessThanOrEqual(35)
      })
    })

    it('should filter with LIKE operator', async () => {
      const results = await select(['id', 'name'])
        .from('users')
        .where({ name: Like('%John%') })
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      results.forEach((result) => {
        expect(result.name.toLowerCase()).toContain('john')
      })
    })

    it('should filter with IS NULL and IS NOT NULL', async () => {
      // Test with a column that might have nulls
      const results = await select(['id', 'name']).from('users').where({ name: IsNotNull() }).run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      results.forEach((result) => {
        expect(result.name).not.toBeNull()
      })
    })

    it('should filter with comparison operators', async () => {
      const results = await select(['id', 'name', 'age'])
        .from('users')
        .where({
          age: And(Gte(30), Lt(50)),
        })
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      results.forEach((result) => {
        expect(result.age).toBeGreaterThanOrEqual(30)
        expect(result.age).toBeLessThan(50)
      })
    })
  })

  describe('Complex WHERE Operations', () => {
    it('should handle AND conditions', async () => {
      const results = await select(['id', 'name', 'age', 'country'])
        .from('users')
        .where({
          age: And(Gt(25), Lt(40)),
          country: Eq('US'),
        })
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      results.forEach((result) => {
        expect(result.age).toBeGreaterThan(25)
        expect(result.age).toBeLessThan(40)
        expect(result.country).toBe('US')
      })
    })

    it('should handle OR conditions', async () => {
      const results = await select(['id', 'name', 'country'])
        .from('users')
        .where({
          country: Or(Eq('US'), Eq('CA')),
        })
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      results.forEach((result) => {
        expect(['US', 'CA']).toContain(result.country)
      })
    })

    it('should handle NOT conditions', async () => {
      const results = await select(['id', 'name', 'country'])
        .from('users')
        .where({
          country: Not(Eq('US')),
        })
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      results.forEach((result) => {
        expect(result.country).not.toBe('US')
      })
    })
  })

  describe('JOIN Operations', () => {
    it('should perform INNER JOIN', async () => {
      const results = await select({ id: 'u.id', name: 'u.name', productName: 'o.product_name', amount: 'o.amount' })
        .from('users', 'u')
        .innerJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      results.forEach((result) => {
        expect(result).toHaveProperty('id')
        expect(result).toHaveProperty('name')
        expect(result).toHaveProperty('productName')
        expect(result).toHaveProperty('amount')
      })
    })

    it('should perform LEFT JOIN', async () => {
      const results = await select(['u.id', 'u.name', 'o.product_name'])
        .from('users', 'u')
        .leftJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      // Should include all users, even those without orders
    })

    it('should perform complex multi-table JOIN', async () => {
      const results = await select({ name: 'u.name', productName: 'o.product_name', price: 'p.price' })
        .from('users', 'u')
        .innerJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
        .innerJoin('products', { 'p.name': EqCol('o.product_name') }, 'p')
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      results.forEach((result) => {
        expect(result).toHaveProperty('name')
        expect(result).toHaveProperty('productName')
        expect(result).toHaveProperty('price')
      })
    })
  })

  describe('Aggregation Operations', () => {
    it('should perform GROUP BY with COUNT', async () => {
      const results = await select({ country: 'country', userCount: Count() })
        .from('users')
        .groupBy(['country'])
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      results.forEach((result) => {
        expect(result).toHaveProperty('country')
        expect(result).toHaveProperty('userCount')
      })
    })

    it('should perform GROUP BY with SUM', async () => {
      const results = await select({
        userId: 'user_id',
        totalAmount: Sum('amount'),
        orderCount: Count(),
      })
        .from('orders')
        .groupBy(['user_id'])
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      results.forEach((result) => {
        expect(result).toHaveProperty('userId')
        expect(result).toHaveProperty('totalAmount')
        expect(result).toHaveProperty('orderCount')
      })
    })

    it('should perform HAVING clause', async () => {
      const results = await select(['user_id', 'sum(amount) as total_amount'])
        .from('orders')
        .groupBy(['user_id'])
        .having({ total_amount: Gt(50) })
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      results.forEach((result) => {
        expect(result.total_amount).toBeGreaterThan(50)
      })
    })
  })

  describe('ClickHouse-Specific Features', () => {
    it('should use FINAL modifier', async () => {
      const results = await select(['id', 'name']).from('users').final().run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
    })

    it('should apply query settings', async () => {
      const results = await select(['id', 'name'])
        .from('users')
        .settings({
          max_execution_time: 30,
          max_threads: 2,
        })
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
    })
  })

  describe('Complex Queries', () => {
    it('should execute complex analytical query', async () => {
      const results = await select([
        'u.country',
        'count(DISTINCT u.id) as user_count',
        'sum(o.amount) as total_revenue',
        'avg(o.amount) as avg_order_value',
      ])
        .from('users', 'u')
        .innerJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
        .where({
          'o.status': Eq('completed'),
        })
        .groupBy(['u.country'])
        .having({ total_revenue: Gt(0) })
        .orderBy([{ column: 'total_revenue', direction: 'DESC' }])
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      results.forEach((result) => {
        expect(result).toHaveProperty('country')
        expect(result).toHaveProperty('user_count')
        expect(result).toHaveProperty('total_revenue')
        expect(result).toHaveProperty('avg_order_value')
      })
    })

    it('should execute subquery', async () => {
      // Find users who have made orders above average order value
      const results = await select(['u.id', 'u.name', 'o.amount'])
        .from('users', 'u')
        .innerJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
        .where({
          'o.amount': Gt(select(['avg(amount) as avg_amount']).from('orders')),
        })
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThanOrEqual(0)
    })
  })

  describe('Error Handling', () => {
    it('should handle invalid table names', async () => {
      await expect(select(['*']).from('nonexistent_table').run(queryRunner)).rejects.toThrow()
    })

    it('should handle invalid column names', async () => {
      await expect(select(['invalid_column']).from('users').run(queryRunner)).rejects.toThrow()
    })

    it('should handle malformed WHERE conditions', async () => {
      await expect(
        select(['*'])
          .from('users')
          .where({ invalid_column: Eq('test') })
          .run(queryRunner),
      ).rejects.toThrow()
    })
  })

  describe('Performance and Edge Cases', () => {
    it('should handle large result sets', async () => {
      const results = await select(['number']).from('system.numbers').limit(1000).run(queryRunner)

      expect(results.length).toBe(1000)
    })

    it('should handle empty result sets gracefully', async () => {
      const results = await select(['id', 'name'])
        .from('users')
        .where({ id: Eq(99999) })
        .run(queryRunner)

      expect(results.length).toBe(0)
      expect(Array.isArray(results)).toBe(true)
    })

    it('should handle complex nested conditions', async () => {
      const results = await select(['id', 'name', 'age', 'country'])
        .from('users')
        .where(Or({ age: And(Gte(25), Lt(30), Not(Eq(27))) }, { country: In(['US', 'CA', 'UK']) }))
        .run(queryRunner)

      expect(results.length).toBeGreaterThanOrEqual(0)
    })

    it('should handle IN with subquery', async () => {
      // Get users who have placed orders
      const userIdsWithOrders = select(['user_id']).from('orders')

      const results = await select(['id', 'name'])
        .from('users')
        .where({ id: In(userIdsWithOrders) })
        .run<any>(queryRunner)

      expect(Array.isArray(results)).toBe(true)
      expect(results.length).toBeGreaterThanOrEqual(0)
    })

    /**
     * Test works, but fails with:
     *  Query execution failed
     *  sql: "SELECT `id` AS `id`, `name` AS `name`, (SELECT count(*) FROM `orders` WHERE `orders`.`user_id` = `users`.`id`) AS `orderCount` FROM `users` LIMIT 5 SETTINGS allow_experimental_correlated_subqueries = 1"
     *  error: "Trying to execute PLACEHOLDER action: while executing 'PLACEHOLDER id -> id UInt32 : 1'. "
     *
     * Looks like ClickHouse is not supporting this yet.
     */
    it.skip('should handle scalar subquery in SELECT', async () => {
      const orderCount = select([Count()])
        .from('orders')
        .where({ 'orders.user_id': EqCol('users.id') })

      const results = await select({
        id: 'id',
        name: 'name',
        orderCount: orderCount,
      })
        .from('users')
        .limit(5)
        .settings({ allow_experimental_correlated_subqueries: 1 })
        .run<any>(queryRunner)

      expect(Array.isArray(results)).toBe(true)
      expect(results.length).toBeGreaterThanOrEqual(0)
      if (results.length > 0) {
        expect(results[0]).toHaveProperty('id')
        expect(results[0]).toHaveProperty('name')
        expect(results[0]).toHaveProperty('orderCount')
      }
    })
  })
})
