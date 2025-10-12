/**
 * Subqueries E2E Tests
 * Tests subquery functionality with real ClickHouse instance
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals'
import { select, Eq, Gt, Gte, In, Exists, NotExists, EqCol, Count, Sum, Avg } from '../../../src/index'
import { testSetup } from '../setup/test-setup'

describe('Subqueries E2E Tests', () => {
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

  describe('Subqueries with IN', () => {
    it('should execute IN with subquery', async () => {
      // Get users who have placed orders
      const orderUserIds = select(['user_id']).from('orders')

      const results = await select(['id', 'name'])
        .from('users')
        .where({ id: In(orderUserIds) })
        .run<any>(queryRunner)

      expect(Array.isArray(results)).toBe(true)
      // All returned users should have orders
      if (results.length > 0) {
        expect(results[0]).toHaveProperty('id')
        expect(results[0]).toHaveProperty('name')
      }
    })

    it('should execute IN with filtered subquery', async () => {
      // Get users who have completed orders
      const completedOrderUserIds = select(['user_id'])
        .from('orders')
        .where({ status: Eq('completed') })

      const results = await select(['id', 'name'])
        .from('users')
        .where({ id: In(completedOrderUserIds) })
        .run<any>(queryRunner)

      expect(Array.isArray(results)).toBe(true)
    })

    it('should execute nested IN subqueries', async () => {
      // Get users who have completed orders
      const completedUserIds = select(['user_id'])
        .from('orders')
        .where({ status: Eq('completed') })

      // Get orders from those users
      const orderIds = select(['id'])
        .from('orders')
        .where({ user_id: In(completedUserIds) })

      // Get users who placed those orders
      const results = await select(['id', 'name'])
        .from('users')
        .where({ id: In(orderIds) })
        .run<any>(queryRunner)

      expect(Array.isArray(results)).toBe(true)
      expect(results.length).toBeGreaterThan(0)
    })
  })

  describe('Subqueries with EXISTS', () => {
    /**
     * Looks like ClickHouse is not supporting correlated subqueries properly yet.
     * Even with allow_experimental_correlated_subqueries = 1, EXISTS queries fail with PLACEHOLDER errors.
     */
    it.skip('should execute EXISTS subquery', async () => {
      // Get users who have at least one order
      const hasOrders = select(['1'])
        .from('orders')
        .where({ 'orders.user_id': EqCol('users.id') })

      const results = await select(['id', 'name'])
        .from('users')
        .where({ _exists: Exists(hasOrders) })
        .run<any>(queryRunner)

      expect(Array.isArray(results)).toBe(true)
    })

    /**
     * Looks like ClickHouse is not supporting correlated subqueries properly yet.
     */
    it.skip('should execute NOT EXISTS subquery', async () => {
      // Get users who have no orders
      const hasOrders = select(['1'])
        .from('orders')
        .where({ 'orders.user_id': EqCol('users.id') })

      const results = await select(['id', 'name'])
        .from('users')
        .where({ _notExists: NotExists(hasOrders) })
        .run<any>(queryRunner)

      expect(Array.isArray(results)).toBe(true)
    })

    /**
     * Looks like ClickHouse is not supporting correlated subqueries properly yet.
     */
    it.skip('should execute correlated EXISTS with conditions', async () => {
      // Get users who have completed orders
      const hasCompletedOrders = select(['1'])
        .from('orders')
        .where({ 'orders.user_id': EqCol('users.id'), status: Eq('completed') })

      const results = await select(['id', 'name'])
        .from('users')
        .where({ _exists: Exists(hasCompletedOrders) })
        .run<any>(queryRunner)

      expect(Array.isArray(results)).toBe(true)
    })
  })

  describe('Scalar subqueries in SELECT', () => {
    it('should execute scalar subquery for aggregate', async () => {
      const avgAge = select([Avg('age')]).from('users')

      const results = await select({
        id: 'id',
        name: 'name',
        age: 'age',
        avgAge: avgAge,
      })
        .from('users')
        .limit(5)
        .run<any>(queryRunner)

      expect(Array.isArray(results)).toBe(true)
      if (results.length > 0) {
        expect(results[0]).toHaveProperty('id')
        expect(results[0]).toHaveProperty('name')
        expect(results[0]).toHaveProperty('age')
        expect(results[0]).toHaveProperty('avgAge')
      }
    })

    /**
     * Looks like ClickHouse is not supporting correlated subqueries properly yet.
     */
    it.skip('should execute correlated subquery in SELECT', async () => {
      const userOrderCount = select([Count()])
        .from('orders')
        .where({ 'orders.user_id': EqCol('users.id') })

      const results = await select({
        id: 'id',
        name: 'name',
        orderCount: userOrderCount,
      })
        .from('users')
        .limit(5)
        .settings({ allow_experimental_correlated_subqueries: 1 })
        .run<any>(queryRunner)

      expect(Array.isArray(results)).toBe(true)
      if (results.length > 0) {
        expect(results[0]).toHaveProperty('id')
        expect(results[0]).toHaveProperty('name')
        expect(results[0]).toHaveProperty('orderCount')
      }
    })

    /**
     * Looks like ClickHouse is not supporting correlated subqueries properly yet.
     */
    it.skip('should execute multiple correlated subqueries', async () => {
      const totalOrders = select([Count()])
        .from('orders')
        .where({ 'orders.user_id': EqCol('users.id') })

      const totalSpent = select([Sum('amount')])
        .from('orders')
        .where({ 'orders.user_id': EqCol('users.id'), status: Eq('completed') })

      const results = await select({
        id: 'id',
        name: 'name',
        totalOrders: totalOrders,
        totalSpent: totalSpent,
      })
        .from('users')
        .limit(5)
        .settings({ allow_experimental_correlated_subqueries: 1 })
        .run<any>(queryRunner)

      expect(Array.isArray(results)).toBe(true)
      if (results.length > 0) {
        expect(results[0]).toHaveProperty('id')
        expect(results[0]).toHaveProperty('name')
        expect(results[0]).toHaveProperty('totalOrders')
        expect(results[0]).toHaveProperty('totalSpent')
      }
    })
  })

  describe('Subqueries with comparison operators', () => {
    it('should execute comparison with scalar subquery', async () => {
      const avgAge = select([Avg('age')]).from('users')

      const results = await select(['id', 'name', 'age'])
        .from('users')
        .where({ age: Gt(avgAge) })
        .run<any>(queryRunner)

      expect(Array.isArray(results)).toBe(true)
      // All returned users should have age > avg
    })

    it('should execute Eq with scalar subquery', async () => {
      const maxAge = select(['max(age)']).from('users')

      const results = await select(['id', 'name', 'age'])
        .from('users')
        .where({ age: Eq(maxAge) })
        .run<any>(queryRunner)

      expect(Array.isArray(results)).toBe(true)
    })
  })

  describe('Complex scenarios', () => {
    /**
     * Looks like ClickHouse is not supporting correlated subqueries properly yet.
     */
    it.skip('should handle subquery in WHERE and SELECT together', async () => {
      const avgAge = select([Avg('age')]).from('users')
      const orderCount = select([Count()])
        .from('orders')
        .where({ 'orders.user_id': EqCol('users.id') })

      const results = await select({
        id: 'id',
        name: 'name',
        age: 'age',
        orderCount: orderCount,
      })
        .from('users')
        .where({ age: Gte(avgAge) })
        .limit(10)
        .settings({ allow_experimental_correlated_subqueries: 1 })
        .run<any>(queryRunner)

      expect(Array.isArray(results)).toBe(true)
    })
  })
})
