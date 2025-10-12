/**
 * Subquery Tests
 * Tests for subquery support in SELECT, WHERE, and FROM clauses
 */

import { describe, it, expect } from '@jest/globals'
import { select, Eq, Gt, In, Exists, NotExists, EqCol, Count, Avg } from '../../../src/index'

describe('Subqueries', () => {
  describe('Subqueries in WHERE with IN', () => {
    it('should build IN with subquery', () => {
      const subquery = select(['user_id'])
        .from('orders')
        .where({ status: Eq('completed') })

      const query = select(['id', 'name'])
        .from('users')
        .where({ id: In(subquery) })

      const { sql } = query.toSQL()

      expect(sql).toBe(
        "SELECT `id`, `name` FROM `users` WHERE `id` IN (SELECT `user_id` FROM `orders` WHERE `status` = 'completed')",
      )
    })

    it('should build IN with complex subquery', () => {
      const subquery = select(['user_id'])
        .from('orders')
        .where({ amount: Gt(100), status: Eq('paid') })

      const query = select(['id', 'name'])
        .from('users')
        .where({ id: In(subquery) })

      const { sql } = query.toSQL()

      expect(sql).toContain('SELECT `user_id` FROM `orders`')
      expect(sql).toContain("WHERE `amount` > 100 AND `status` = 'paid'")
    })
  })

  describe('Subqueries with EXISTS', () => {
    it('should build EXISTS subquery', () => {
      const subquery = select(['1'])
        .from('orders')
        .where({ 'orders.user_id': EqCol('users.id') })

      const query = select(['id', 'name'])
        .from('users')
        .where({ _exists: Exists(subquery) })

      const { sql } = query.toSQL()

      expect(sql).toContain('EXISTS')
      expect(sql).toContain('SELECT 1 FROM `orders`')
    })

    it('should build NOT EXISTS subquery', () => {
      const subquery = select(['1'])
        .from('orders')
        .where({ 'orders.user_id': EqCol('users.id') })

      const query = select(['id', 'name'])
        .from('users')
        .where({ _notExists: NotExists(subquery) })

      const { sql } = query.toSQL()

      expect(sql).toContain('NOT EXISTS')
      expect(sql).toContain('SELECT 1 FROM `orders`')
    })

    it('should build correlated EXISTS subquery', () => {
      const subquery = select(['1'])
        .from('orders')
        .where({ 'orders.user_id': EqCol('users.id'), status: Eq('completed') })

      const query = select(['id', 'name'])
        .from('users')
        .where({ _exists: Exists(subquery) })

      const { sql } = query.toSQL()

      expect(sql).toContain('EXISTS')
      expect(sql).toContain('`orders`.`user_id` = `users`.`id`')
      expect(sql).toContain("'completed'")
    })
  })

  describe('Subqueries in SELECT', () => {
    it('should build scalar subquery in SELECT', () => {
      const avgAgeQuery = select([Avg('age')]).from('users')

      const query = select({ id: 'id', name: 'name', avgAge: avgAgeQuery }).from('users')

      const { sql } = query.toSQL()

      expect(sql).toContain('SELECT `id` AS `id`, `name` AS `name`')
      expect(sql).toContain('(SELECT avg(age) FROM `users`) AS `avgAge`')
    })

    it('should build correlated subquery in SELECT', () => {
      const orderCountQuery = select([Count()])
        .from('orders')
        .where({ 'orders.user_id': EqCol('users.id') })

      const query = select({ id: 'id', name: 'name', orderCount: orderCountQuery }).from('users')

      const { sql } = query.toSQL()

      expect(sql).toContain('`id` AS `id`, `name` AS `name`')
      expect(sql).toContain('(SELECT count(*) FROM `orders`')
      expect(sql).toContain('AS `orderCount`')
    })

    it('should handle multiple subqueries in SELECT', () => {
      const totalOrders = select([Count()])
        .from('orders')
        .where({ 'orders.user_id': EqCol('users.id') })

      const completedOrders = select([Count()])
        .from('orders')
        .where({ 'orders.user_id': EqCol('users.id'), status: Eq('completed') })

      const query = select({
        id: 'id',
        name: 'name',
        totalOrders: totalOrders,
        completedOrders: completedOrders,
      }).from('users')

      const { sql } = query.toSQL()

      expect(sql).toContain('`id` AS `id`, `name` AS `name`')
      expect(sql).toContain('AS `totalOrders`')
      expect(sql).toContain('AS `completedOrders`')
    })
  })

  describe('Subqueries with comparison operators', () => {
    it('should build subquery with Gt', () => {
      const avgPriceQuery = select([Avg('price')]).from('products')

      const query = select(['id', 'name', 'price'])
        .from('products')
        .where({ price: Gt(avgPriceQuery) })

      const { sql } = query.toSQL()

      expect(sql).toContain('WHERE `price` > (SELECT avg(price) FROM `products`)')
    })

    it('should build subquery with Eq', () => {
      const maxPriceQuery = select(['max(price)']).from('products')

      const query = select(['id', 'name', 'price'])
        .from('products')
        .where({ price: Eq(maxPriceQuery) })

      const { sql } = query.toSQL()

      expect(sql).toContain('WHERE `price` = (SELECT max(price) FROM `products`)')
    })
  })

  describe('Nested subqueries', () => {
    it('should handle nested subqueries', () => {
      const innerSubquery = select(['category_id'])
        .from('categories')
        .where({ active: Eq(1) })

      const outerSubquery = select(['product_id'])
        .from('products')
        .where({ category_id: In(innerSubquery) })

      const query = select(['id', 'name'])
        .from('orders')
        .where({ product_id: In(outerSubquery) })

      const { sql } = query.toSQL()

      expect(sql).toContain('SELECT `id`, `name` FROM `orders`')
      expect(sql).toContain('SELECT `product_id` FROM `products`')
      expect(sql).toContain('SELECT `category_id` FROM `categories`')
    })
  })

  describe('Subquery aliasing', () => {
    it('should support .as() for subquery aliasing', () => {
      const subquery = select({ userId: 'user_id', orderCount: Count() }).from('orders').as('user_orders')

      expect(subquery.getAlias()).toBe('user_orders')
    })

    it('should chain .as() with other methods', () => {
      const subquery = select({ userId: 'user_id', orderCount: Count() })
        .from('orders')
        .groupBy(['user_id'])
        .as('user_stats')

      expect(subquery.getAlias()).toBe('user_stats')
    })
  })

  describe('Edge cases', () => {
    it('should handle empty subquery results', () => {
      const subquery = select(['user_id'])
        .from('orders')
        .where({ id: Eq(999999) })

      const query = select(['id', 'name'])
        .from('users')
        .where({ id: In(subquery) })

      const { sql } = query.toSQL()

      expect(sql).toContain('IN (SELECT')
    })

    it('should handle subquery with multiple columns in SELECT', () => {
      const subquery = select(['user_id', 'status']).from('orders')

      const query = select(['id', 'name'])
        .from('users')
        .where({ id: In(subquery) })

      const { sql } = query.toSQL()

      expect(sql).toContain('SELECT `user_id`, `status` FROM `orders`')
    })
  })
})
