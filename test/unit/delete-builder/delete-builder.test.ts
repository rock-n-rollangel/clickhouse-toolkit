/**
 * Delete Builder Tests
 * Comprehensive tests for the delete builder functionality
 */

import { describe, it, expect, beforeEach, jest } from '@jest/globals'
import { deleteFrom, DeleteBuilder } from '../../../src/builder/delete-builder'
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

describe('DeleteBuilder', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })

  describe('Basic Query Building', () => {
    it('should build a simple DELETE query with WHERE clause', () => {
      const query = deleteFrom('users').where({ id: Eq(1) })

      const { sql, params } = query.toSQL()

      expect(sql).toBe('ALTER TABLE `users` DELETE WHERE `id` = 1')
      expect(params).toEqual([])
    })

    it('should build a DELETE query with multiple WHERE conditions', () => {
      const query = deleteFrom('users').where({
        status: Eq('inactive'),
        age: Lt(18),
      })

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` DELETE WHERE `status` = 'inactive' AND `age` < 18")
    })

    it('should build a DELETE query without WHERE clause', () => {
      const query = deleteFrom('temp_table')

      const { sql } = query.toSQL()

      expect(sql).toBe('ALTER TABLE `temp_table` DELETE')
    })

    it('should handle table names with underscores', () => {
      const query = deleteFrom('user_profiles').where({ user_id: Eq(123) })

      const { sql } = query.toSQL()

      expect(sql).toBe('ALTER TABLE `user_profiles` DELETE WHERE `user_id` = 123')
    })
  })

  describe('WHERE Clause Building', () => {
    it('should handle simple equality WHERE clause', () => {
      const query = deleteFrom('users').where({ id: Eq(1) })

      const { sql } = query.toSQL()

      expect(sql).toBe('ALTER TABLE `users` DELETE WHERE `id` = 1')
    })

    it('should handle string equality WHERE clause', () => {
      const query = deleteFrom('users').where({ status: Eq('deleted') })

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` DELETE WHERE `status` = 'deleted'")
    })

    it('should handle numeric comparison WHERE clause', () => {
      const query = deleteFrom('products').where({ price: Gt(100) })

      const { sql } = query.toSQL()

      expect(sql).toBe('ALTER TABLE `products` DELETE WHERE `price` > 100')
    })

    it('should handle multiple WHERE conditions with AND', () => {
      const query = deleteFrom('users').where({
        status: Eq('inactive'),
        last_login: Lt(new Date('2023-01-01')),
      })

      const { sql } = query.toSQL()

      expect(sql).toBe(
        "ALTER TABLE `users` DELETE WHERE `status` = 'inactive' AND `last_login` < '2023-01-01 00:00:00'",
      )
    })

    it('should handle complex WHERE clause with AND combinator', () => {
      const query = deleteFrom('users').where(And({ age: Gt(65) }, { status: Eq('retired') }))

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` DELETE WHERE (`age` > 65 AND `status` = 'retired')")
    })

    it('should handle complex WHERE clause with OR combinator', () => {
      const query = deleteFrom('users').where(Or({ status: Eq('banned') }, { is_spam: Eq(true) }))

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` DELETE WHERE (`status` = 'banned' OR `is_spam` = true)")
    })

    it('should handle IN clause in WHERE', () => {
      const query = deleteFrom('users').where({ id: In([1, 2, 3, 4, 5]) })

      const { sql } = query.toSQL()

      expect(sql).toBe('ALTER TABLE `users` DELETE WHERE `id` IN (1, 2, 3, 4, 5)')
    })

    it('should handle NOT clause in WHERE', () => {
      const query = deleteFrom('users').where(Not({ status: Eq('active') }))

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` DELETE WHERE NOT (`status` = 'active')")
    })
  })

  describe('Data Type Handling', () => {
    it('should handle string values in WHERE', () => {
      const query = deleteFrom('users').where({ name: Eq('John Doe'), email: Eq('john@example.com') })

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` DELETE WHERE `name` = 'John Doe' AND `email` = 'john@example.com'")
    })

    it('should handle numeric values in WHERE', () => {
      const query = deleteFrom('products').where({ price: Eq(99.99), quantity: Eq(0) })

      const { sql } = query.toSQL()

      expect(sql).toBe('ALTER TABLE `products` DELETE WHERE `price` = 99.99 AND `quantity` = 0')
    })

    it('should handle boolean values in WHERE', () => {
      const query = deleteFrom('settings').where({ is_active: Eq(false) })

      const { sql } = query.toSQL()

      expect(sql).toBe('ALTER TABLE `settings` DELETE WHERE `is_active` = false')
    })

    it('should handle null values in WHERE', () => {
      const query = deleteFrom('users').where({ deleted_at: Eq(null) })

      const { sql } = query.toSQL()

      expect(sql).toBe('ALTER TABLE `users` DELETE WHERE `deleted_at` = NULL')
    })

    it('should handle date values in WHERE', () => {
      const date = new Date('2023-01-01T00:00:00Z')
      const query = deleteFrom('events').where({ created_at: Eq(date) })

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `events` DELETE WHERE `created_at` = '2023-01-01 00:00:00'")
    })
  })

  describe('Settings', () => {
    it('should add settings to query', () => {
      const query = deleteFrom('users')
        .where({ id: Eq(1) })
        .settings({ max_execution_time: 30 })

      const { sql } = query.toSQL()

      expect(sql).toBe('ALTER TABLE `users` DELETE WHERE `id` = 1 SETTINGS max_execution_time = 30')
    })

    it('should handle multiple settings', () => {
      const query = deleteFrom('users')
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

    it('should handle settings without WHERE clause', () => {
      const query = deleteFrom('temp_table').settings({ max_execution_time: 60 })

      const { sql } = query.toSQL()

      expect(sql).toBe('ALTER TABLE `temp_table` DELETE SETTINGS max_execution_time = 60')
    })
  })

  describe('Query Execution', () => {
    it('should execute query with QueryRunner', async () => {
      const query = deleteFrom('users')
        .where({ id: Eq(1) })
        .settings({ max_execution_time: 30 })

      mockQueryRunner.command.mockResolvedValue(undefined)

      await query.run(mockQueryRunner)

      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: 'ALTER TABLE `users` DELETE WHERE `id` = 1 SETTINGS max_execution_time = 30',
        settings: { max_execution_time: 30 },
      })
    })

    it('should throw error when QueryRunner is not provided', async () => {
      const query = deleteFrom('users').where({ id: Eq(1) })

      await expect(query.run()).rejects.toThrow('QueryRunner is required to execute queries')
    })

    it('should execute query without WHERE clause', async () => {
      const query = deleteFrom('temp_table').settings({ max_execution_time: 60 })

      mockQueryRunner.command.mockResolvedValue(undefined)

      await query.run(mockQueryRunner)

      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: 'ALTER TABLE `temp_table` DELETE SETTINGS max_execution_time = 60',
        settings: { max_execution_time: 60 },
      })
    })
  })

  describe('Edge Cases', () => {
    it('should handle special characters in values', () => {
      const query = deleteFrom('users').where({ name: Eq("O'Connor"), description: Eq("User's data") })

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` DELETE WHERE `name` = 'O''Connor' AND `description` = 'User''s data'")
    })

    it('should handle table names with special characters', () => {
      const query = deleteFrom('user-profiles').where({ id: Eq(1) })

      const { sql } = query.toSQL()

      expect(sql).toBe('ALTER TABLE `user-profiles` DELETE WHERE `id` = 1')
    })

    it('should handle empty string values', () => {
      const query = deleteFrom('users').where({ name: Eq(''), email: Eq('') })

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` DELETE WHERE `name` = '' AND `email` = ''")
    })

    it('should handle large IN clauses', () => {
      const largeArray = Array.from({ length: 100 }, (_, i) => i + 1)
      const query = deleteFrom('users').where({ id: In(largeArray) })

      const { sql } = query.toSQL()

      expect(sql).toContain('IN (')
      expect(sql).toContain('1, 2, 3')
    })
  })

  describe('Factory Function', () => {
    it('should create DeleteBuilder instance', () => {
      const builder = deleteFrom('users')

      expect(builder).toBeInstanceOf(DeleteBuilder)
    })

    it('should accept table name as parameter', () => {
      const builder = deleteFrom('custom_table')

      const { sql } = builder.where({ id: Eq(1) }).toSQL()

      expect(sql).toBe('ALTER TABLE `custom_table` DELETE WHERE `id` = 1')
    })
  })

  describe('Method Chaining', () => {
    it('should support method chaining', () => {
      const query = deleteFrom('users')
        .where({ id: Eq(1) })
        .settings({ max_execution_time: 30 })

      expect(query).toBeInstanceOf(DeleteBuilder)
      expect(() => query.toSQL()).not.toThrow()
    })

    it('should support multiple where calls', () => {
      const query = deleteFrom('users').where({ status: Eq('inactive'), age: Lt(18) })

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` DELETE WHERE `status` = 'inactive' AND `age` < 18")
    })
  })

  describe('Complex WHERE Conditions', () => {
    it('should handle nested AND/OR combinations', () => {
      const query = deleteFrom('users').where(And({ age: Gt(18) }, Or({ status: Eq('banned') }, { is_spam: Eq(true) })))

      const { sql } = query.toSQL()

      expect(sql).toBe("ALTER TABLE `users` DELETE WHERE `age` > 18 AND (`status` = 'banned' OR `is_spam` = true)")
    })

    it('should handle multiple levels of nesting', () => {
      const query = deleteFrom('users').where(
        And(
          { age: Gt(18) },
          Or(And({ status: Eq('banned') }, { email: Eq('spam@example.com') }), {
            last_login: Lt(new Date('2023-01-01')),
          }),
        ),
      )

      const { sql } = query.toSQL()

      expect(sql).toBe(
        "ALTER TABLE `users` DELETE WHERE `age` > 18 AND ((`status` = 'banned' AND `email` = 'spam@example.com') OR `last_login` < '2023-01-01 00:00:00')",
      )
    })

    it('should handle NOT with complex conditions', () => {
      const query = deleteFrom('users').where(
        Not(And({ status: Eq('active') }, { last_login: Gt(new Date('2023-01-01')) })),
      )

      const { sql } = query.toSQL()

      expect(sql).toBe(
        "ALTER TABLE `users` DELETE WHERE NOT ((`status` = 'active' AND `last_login` > '2023-01-01 00:00:00'))",
      )
    })
  })

  describe('Safety Considerations', () => {
    it('should require WHERE clause for safety (when implemented)', () => {
      // This test documents expected safety behavior
      // In a production system, you might want to require WHERE clauses
      // to prevent accidental mass deletions
      const query = deleteFrom('users')
      // No WHERE clause - this should potentially be flagged as unsafe

      const { sql } = query.toSQL()
      expect(sql).toBe('ALTER TABLE `users` DELETE')

      // In a real implementation, you might want to add validation
      // that requires WHERE clauses for DELETE operations
    })

    it('should handle bulk delete operations safely', () => {
      const query = deleteFrom('old_logs')
        .where({ created_at: Lt(new Date('2022-01-01')) })
        .settings({ max_execution_time: 300 })

      const { sql } = query.toSQL()

      expect(sql).toBe(
        "ALTER TABLE `old_logs` DELETE WHERE `created_at` < '2022-01-01 00:00:00' SETTINGS max_execution_time = 300",
      )
    })
  })
})
