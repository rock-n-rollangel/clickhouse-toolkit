/**
 * Insert Builder Tests
 * Comprehensive tests for the insert builder functionality
 */

import { describe, it, expect, beforeEach, jest } from '@jest/globals'
import { insertInto, InsertBuilder } from '../../../src/builder/insert-builder'
import { select } from '../../../src/builder/select-builder'

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

describe('InsertBuilder', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })

  describe('Basic Query Building', () => {
    it('should build a simple INSERT query', () => {
      const query = insertInto('users')
        .columns(['name', 'email', 'age'])
        .values([['John Doe', 'john@example.com', 25]])

      const { sql, params } = query.toSQL()

      expect(sql).toBe("INSERT INTO `users` (`name`, `email`, `age`) VALUES ('John Doe', 'john@example.com', 25)")
      expect(params).toEqual([])
    })

    it('should build an INSERT query with single value', () => {
      const query = insertInto('products').columns(['name', 'price']).value(['Laptop', 999.99])

      const { sql } = query.toSQL()

      expect(sql).toBe("INSERT INTO `products` (`name`, `price`) VALUES ('Laptop', 999.99)")
    })

    it('should build an INSERT query with multiple values', () => {
      const query = insertInto('users')
        .columns(['name', 'email'])
        .values([
          ['John Doe', 'john@example.com'],
          ['Jane Smith', 'jane@example.com'],
          ['Bob Johnson', 'bob@example.com'],
        ])

      const { sql } = query.toSQL()

      expect(sql).toBe(
        "INSERT INTO `users` (`name`, `email`) VALUES ('John Doe', 'john@example.com'), ('Jane Smith', 'jane@example.com'), ('Bob Johnson', 'bob@example.com')",
      )
    })

    it('should build an INSERT query without column specification', () => {
      const query = insertInto('users').values([['John Doe', 'john@example.com', 25]])

      const { sql } = query.toSQL()

      expect(sql).toBe("INSERT INTO `users` VALUES ('John Doe', 'john@example.com', 25)")
    })
  })

  describe('Data Type Handling', () => {
    it('should handle string values', () => {
      const query = insertInto('users').columns(['name']).value(['John Doe'])

      const { sql } = query.toSQL()

      expect(sql).toBe("INSERT INTO `users` (`name`) VALUES ('John Doe')")
    })

    it('should handle numeric values', () => {
      const query = insertInto('products').columns(['price', 'quantity']).value([99.99, 10])

      const { sql } = query.toSQL()

      expect(sql).toBe('INSERT INTO `products` (`price`, `quantity`) VALUES (99.99, 10)')
    })

    it('should handle boolean values', () => {
      const query = insertInto('settings').columns(['is_active', 'is_public']).value([true, false])

      const { sql } = query.toSQL()

      expect(sql).toBe('INSERT INTO `settings` (`is_active`, `is_public`) VALUES (true, false)')
    })

    it('should handle null values', () => {
      const query = insertInto('users').columns(['name', 'middle_name', 'age']).value(['John Doe', null, 25])

      const { sql } = query.toSQL()

      expect(sql).toBe("INSERT INTO `users` (`name`, `middle_name`, `age`) VALUES ('John Doe', NULL, 25)")
    })

    it('should handle date values', () => {
      const date = new Date('2023-01-01T00:00:00Z')
      const query = insertInto('events').columns(['name', 'created_at']).value(['Event Name', date])

      const { sql } = query.toSQL()

      expect(sql).toBe("INSERT INTO `events` (`name`, `created_at`) VALUES ('Event Name', '2023-01-01 00:00:00')")
    })
  })

  describe('Format Specification', () => {
    it('should add format specification', () => {
      const query = insertInto('users')
        .columns(['name', 'email'])
        .value(['John Doe', 'john@example.com'])
        .format('JSONEachRow')

      const { sql } = query.toSQL()

      expect(sql).toBe("INSERT INTO `users` (`name`, `email`) VALUES ('John Doe', 'john@example.com')")
    })

    it('should handle different format types', () => {
      const query = insertInto('users').columns(['name', 'email']).value(['John Doe', 'john@example.com']).format('CSV')

      const { sql } = query.toSQL()

      expect(sql).toBe("INSERT INTO `users` (`name`, `email`) VALUES ('John Doe', 'john@example.com')")
    })
  })

  describe('Query Execution', () => {
    it('should execute query with QueryRunner', async () => {
      const query = insertInto('users').columns(['name', 'email']).value(['John Doe', 'john@example.com'])

      mockQueryRunner.command.mockResolvedValue(undefined)

      await query.run(mockQueryRunner)

      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: "INSERT INTO `users` (`name`, `email`) VALUES ('John Doe', 'john@example.com')",
      })
    })

    it('should throw error when QueryRunner is not provided', async () => {
      const query = insertInto('users').columns(['name', 'email']).value(['John Doe', 'john@example.com'])

      await expect(query.run()).rejects.toThrow('QueryRunner is required to execute queries')
    })
  })

  describe('Insert from Select', () => {
    it('should build INSERT ... SELECT queries', () => {
      const sourceQuery = select(['id', 'name']).from('users')

      const query = insertInto('archived_users').columns(['id', 'name']).fromSelect(sourceQuery)

      const { sql } = query.toSQL()

      expect(sql).toBe('INSERT INTO `archived_users` (`id`, `name`) SELECT `id`, `name` FROM `users`')
    })

    it('should execute INSERT ... SELECT operations', async () => {
      const sourceQuery = select(['id']).from('users')
      const query = insertInto('archived_users').columns(['id']).fromSelect(sourceQuery)

      mockQueryRunner.command.mockResolvedValue(undefined)

      await query.run(mockQueryRunner)

      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: 'INSERT INTO `archived_users` (`id`) SELECT `id` FROM `users`',
        format: undefined,
      })
    })
  })

  describe('Edge Cases', () => {
    it('should handle empty values array', () => {
      const query = insertInto('users').columns(['name', 'email']).values([])

      const { sql } = query.toSQL()

      expect(sql).toBe('INSERT INTO `users` (`name`, `email`) VALUES')
    })

    it('should handle single column insert', () => {
      const query = insertInto('users').columns(['name']).value(['John Doe'])

      const { sql } = query.toSQL()

      expect(sql).toBe("INSERT INTO `users` (`name`) VALUES ('John Doe')")
    })

    it('should handle empty string values', () => {
      const query = insertInto('users').columns(['name', 'email']).value(['', ''])

      const { sql } = query.toSQL()

      expect(sql).toBe("INSERT INTO `users` (`name`, `email`) VALUES ('', '')")
    })

    it('should handle special characters in strings', () => {
      const query = insertInto('users').columns(['name']).value(["O'Connor & Associates"])

      const { sql } = query.toSQL()

      expect(sql).toBe("INSERT INTO `users` (`name`) VALUES ('O''Connor & Associates')")
    })
  })

  describe('Factory Function', () => {
    it('should create InsertBuilder instance', () => {
      const builder = insertInto('users')

      expect(builder).toBeInstanceOf(InsertBuilder)
    })

    it('should accept table name as parameter', () => {
      const builder = insertInto('custom_table')

      const { sql } = builder.columns(['id', 'name']).value([1, 'Test']).toSQL()

      expect(sql).toBe("INSERT INTO `custom_table` (`id`, `name`) VALUES (1, 'Test')")
    })
  })

  describe('Method Chaining', () => {
    it('should support method chaining', () => {
      const query = insertInto('users')
        .columns(['name', 'email', 'age'])
        .value(['John Doe', 'john@example.com', 25])
        .format('JSONEachRow')

      expect(query).toBeInstanceOf(InsertBuilder)
      expect(() => query.toSQL()).not.toThrow()
    })

    it('should support multiple value calls', () => {
      const query = insertInto('users')
        .columns(['name', 'email'])
        .value(['John Doe', 'john@example.com'])
        .value(['Jane Smith', 'jane@example.com'])

      const { sql } = query.toSQL()

      expect(sql).toBe(
        "INSERT INTO `users` (`name`, `email`) VALUES ('John Doe', 'john@example.com'), ('Jane Smith', 'jane@example.com')",
      )
    })
  })

  describe('Table Name Handling', () => {
    it('should handle table names with underscores', () => {
      const query = insertInto('user_profiles').columns(['user_id', 'profile_data']).value([1, 'data'])

      const { sql } = query.toSQL()

      expect(sql).toBe("INSERT INTO `user_profiles` (`user_id`, `profile_data`) VALUES (1, 'data')")
    })

    it('should handle table names with special characters', () => {
      const query = insertInto('user-profiles').columns(['user_id']).value([1])

      const { sql } = query.toSQL()

      expect(sql).toBe('INSERT INTO `user-profiles` (`user_id`) VALUES (1)')
    })
  })
})
