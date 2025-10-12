/**
 * Update Builder Tests
 * Comprehensive tests for the update builder functionality
 */

import { describe, it, expect, beforeEach, jest } from '@jest/globals'
import { update, UpdateBuilder } from '../../../src/builder/update-builder'
import { Eq, Gt, Lt, And, Or, Not, In } from '../../../src/core/operators'

// Mock QueryRunner for testing
const mockQueryRunner = {
  command: jest.fn() as jest.MockedFunction<any>,
  execute: jest.fn() as jest.MockedFunction<any>,
  stream: jest.fn() as jest.MockedFunction<any>,
  streamJSONEachRow: jest.fn() as jest.MockedFunction<any>,
  streamCSV: jest.fn() as jest.MockedFunction<any>,
  testConnection: jest.fn() as jest.MockedFunction<any>,
  getServerVersion: jest.fn() as jest.MockedFunction<any>,
  close: jest.fn() as jest.MockedFunction<any>,
} as any

describe('UpdateBuilder', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })

  describe('Basic Query Building', () => {
    it('should build a simple UPDATE query', () => {
      const query = update('users')
        .set({ name: 'John Updated', email: 'john.updated@example.com' })
        .where({ id: Eq(1) })

      const { sql, params } = query.toSQL()

      expect(sql).toBe(
        "ALTER TABLE `users` UPDATE `name` = 'John Updated', `email` = 'john.updated@example.com' WHERE `id` = 1",
      )
      expect(params).toEqual([])
    })

    it('should build an UPDATE query with single SET value', () => {
      const query = update('users')
        .set({ status: 'active' })
        .where({ id: Eq(1) })

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` UPDATE `status` = 'active' WHERE `id` = 1")
    })

    it('should build an UPDATE query with multiple SET values', () => {
      const query = update('products')
        .set({
          name: 'Updated Product',
          price: 199.99,
          quantity: 50,
          is_active: true,
        })
        .where({ id: Eq(123) })

      const { sql } = query.toSQL()

      expect(sql).toBe(
        "ALTER TABLE `products` UPDATE `name` = 'Updated Product', `price` = 199.99, `quantity` = 50, `is_active` = true WHERE `id` = 123",
      )
    })

    it('should build an UPDATE query without WHERE clause', () => {
      const query = update('users').set({ last_login: new Date('2023-01-01') })

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` UPDATE `last_login` = '2023-01-01 00:00:00'")
    })
  })

  describe('WHERE Clause Building', () => {
    it('should handle simple equality WHERE clause', () => {
      const query = update('users')
        .set({ status: 'inactive' })
        .where({ id: Eq(1) })

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` UPDATE `status` = 'inactive' WHERE `id` = 1")
    })

    it('should handle multiple WHERE conditions', () => {
      const query = update('users')
        .set({ status: 'active' })
        .where({
          age: Gt(18),
          status: Eq('pending'),
        })

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` UPDATE `status` = 'active' WHERE `age` > 18 AND `status` = 'pending'")
    })

    it('should handle complex WHERE clause with AND', () => {
      const query = update('users')
        .set({ status: 'verified' })
        .where(And({ age: Gt(18) }, { email: Eq('user@example.com') }))

      const { sql } = query.toSQL()

      expect(sql).toBe(
        "ALTER TABLE `users` UPDATE `status` = 'verified' WHERE (`age` > 18 AND `email` = 'user@example.com')",
      )
    })

    it('should handle complex WHERE clause with OR', () => {
      const query = update('users')
        .set({ status: 'archived' })
        .where(Or({ status: Eq('inactive') }, { last_login: Lt(new Date('2023-01-01')) }))

      const { sql } = query.toSQL()

      expect(sql).toBe(
        "ALTER TABLE `users` UPDATE `status` = 'archived' WHERE (`status` = 'inactive' OR `last_login` < '2023-01-01 00:00:00')",
      )
    })

    it('should handle IN clause in WHERE', () => {
      const query = update('users')
        .set({ status: 'bulk_updated' })
        .where({ id: In([1, 2, 3, 4, 5]) })

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` UPDATE `status` = 'bulk_updated' WHERE `id` IN (1, 2, 3, 4, 5)")
    })

    it('should handle NOT clause in WHERE', () => {
      const query = update('users')
        .set({ status: 'active' })
        .where(Not({ status: Eq('deleted') }))

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` UPDATE `status` = 'active' WHERE NOT (`status` = 'deleted')")
    })
  })

  describe('Data Type Handling', () => {
    it('should handle string values in SET', () => {
      const query = update('users')
        .set({ name: 'John Doe', description: 'Updated user' })
        .where({ id: Eq(1) })

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` UPDATE `name` = 'John Doe', `description` = 'Updated user' WHERE `id` = 1")
    })

    it('should handle numeric values in SET', () => {
      const query = update('products')
        .set({ price: 99.99, quantity: 10, rating: 4.5 })
        .where({ id: Eq(1) })

      const { sql } = query.toSQL()

      expect(sql).toBe('ALTER TABLE `products` UPDATE `price` = 99.99, `quantity` = 10, `rating` = 4.5 WHERE `id` = 1')
    })

    it('should handle boolean values in SET', () => {
      const query = update('settings')
        .set({ is_active: true, is_public: false })
        .where({ id: Eq(1) })

      const { sql } = query.toSQL()

      expect(sql).toBe('ALTER TABLE `settings` UPDATE `is_active` = true, `is_public` = false WHERE `id` = 1')
    })

    it('should handle null values in SET', () => {
      const query = update('users')
        .set({ middle_name: null, deleted_at: null })
        .where({ id: Eq(1) })

      const { sql } = query.toSQL()

      expect(sql).toBe('ALTER TABLE `users` UPDATE `middle_name` = NULL, `deleted_at` = NULL WHERE `id` = 1')
    })

    it('should handle date values in SET', () => {
      const date = new Date('2023-01-01T00:00:00Z')
      const query = update('users')
        .set({ updated_at: date, last_login: date })
        .where({ id: Eq(1) })

      const { sql } = query.toSQL()

      expect(sql).toBe(
        "ALTER TABLE `users` UPDATE `updated_at` = '2023-01-01 00:00:00', `last_login` = '2023-01-01 00:00:00' WHERE `id` = 1",
      )
    })
  })

  describe('Settings', () => {
    it('should add settings to query', () => {
      const query = update('users')
        .set({ status: 'active' })
        .where({ id: Eq(1) })
        .settings({ max_execution_time: 30 })

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` UPDATE `status` = 'active' WHERE `id` = 1 SETTINGS max_execution_time = 30")
    })

    it('should handle multiple settings', () => {
      const query = update('users')
        .set({ status: 'active' })
        .where({ id: Eq(1) })
        .settings({
          max_execution_time: 30,
          max_memory_usage: 1000000,
          optimize_skip_unused_shards: 1,
        })

      const { sql } = query.toSQL()

      expect(sql).toContain('SETTINGS')
      expect(sql).toContain('max_execution_time = 30')
      expect(sql).toContain('max_memory_usage = 1000000')
      expect(sql).toContain('optimize_skip_unused_shards = 1')
    })
  })

  describe('Query Execution', () => {
    it('should execute query with QueryRunner', async () => {
      const query = update('users')
        .set({ status: 'active' })
        .where({ id: Eq(1) })
        .settings({ max_execution_time: 30 })

      mockQueryRunner.command.mockResolvedValue(undefined)

      await query.run(mockQueryRunner)

      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: "ALTER TABLE `users` UPDATE `status` = 'active' WHERE `id` = 1 SETTINGS max_execution_time = 30",
        settings: { max_execution_time: 30 },
      })
    })

    it('should throw error when QueryRunner is not provided', async () => {
      const query = update('users')
        .set({ status: 'active' })
        .where({ id: Eq(1) })

      await expect(query.run()).rejects.toThrow('QueryRunner is required to execute queries')
    })
  })

  describe('Edge Cases', () => {
    it('should handle empty SET object', () => {
      const query = update('users')
        .set({})
        .where({ id: Eq(1) })

      const { sql } = query.toSQL()

      expect(sql).toBe('ALTER TABLE `users` UPDATE WHERE `id` = 1')
    })

    it('should handle special characters in values', () => {
      const query = update('users')
        .set({ name: "O'Connor", description: "User's description" })
        .where({ id: Eq(1) })

      const { sql } = query.toSQL()

      expect(sql).toBe(
        "ALTER TABLE `users` UPDATE `name` = 'O''Connor', `description` = 'User''s description' WHERE `id` = 1",
      )
    })

    it('should handle column names with underscores', () => {
      const query = update('user_profiles')
        .set({ user_id: 123, profile_data: 'data' })
        .where({ id: Eq(1) })

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `user_profiles` UPDATE `user_id` = 123, `profile_data` = 'data' WHERE `id` = 1")
    })

    it('should handle table names with special characters', () => {
      const query = update('user-profiles')
        .set({ status: 'active' })
        .where({ id: Eq(1) })

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `user-profiles` UPDATE `status` = 'active' WHERE `id` = 1")
    })
  })

  describe('Factory Function', () => {
    it('should create UpdateBuilder instance', () => {
      const builder = update('users')

      expect(builder).toBeInstanceOf(UpdateBuilder)
    })

    it('should accept table name as parameter', () => {
      const builder = update('custom_table')

      const { sql } = builder
        .set({ name: 'Test' })
        .where({ id: Eq(1) })
        .toSQL()

      expect(sql).toBe("ALTER TABLE `custom_table` UPDATE `name` = 'Test' WHERE `id` = 1")
    })
  })

  describe('Method Chaining', () => {
    it('should support method chaining', () => {
      const query = update('users')
        .set({ name: 'John', email: 'john@example.com' })
        .where({ id: Eq(1) })
        .settings({ max_execution_time: 30 })

      expect(query).toBeInstanceOf(UpdateBuilder)
      expect(() => query.toSQL()).not.toThrow()
    })

    it('should support multiple set calls', () => {
      const query = update('users')
        .set({ name: 'John' })
        .set({ email: 'john@example.com' })
        .set({ age: 25 })
        .where({ id: Eq(1) })

      const { sql } = query.toSQL()

      expect(sql).toBe(
        "ALTER TABLE `users` UPDATE `name` = 'John', `email` = 'john@example.com', `age` = 25 WHERE `id` = 1",
      )
    })
  })

  describe('Complex WHERE Conditions', () => {
    it('should handle nested AND/OR combinations', () => {
      const query = update('users')
        .set({ status: 'complex_updated' })
        .where(And({ age: Gt(18) }, Or({ status: Eq('active') }, { status: Eq('pending') })))

      const { sql } = query.toSQL()

      expect(sql).toBe(
        "ALTER TABLE `users` UPDATE `status` = 'complex_updated' WHERE `age` > 18 AND (`status` = 'active' OR `status` = 'pending')",
      )
    })

    it('should handle multiple levels of nesting', () => {
      const query = update('users')
        .set({ status: 'deep_nested' })
        .where(
          And(
            { age: Gt(18) },
            Or(And({ status: Eq('active') }, { email: Eq('user@example.com') }), {
              last_login: Gt(new Date('2023-01-01')),
            }),
          ),
        )

      const { sql } = query.toSQL()

      expect(sql).toBe(
        "ALTER TABLE `users` UPDATE `status` = 'deep_nested' WHERE `age` > 18 AND ((`status` = 'active' AND `email` = 'user@example.com') OR `last_login` > '2023-01-01 00:00:00')",
      )
    })
  })
})
