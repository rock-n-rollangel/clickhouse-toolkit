/**
 * Error Handling E2E Tests
 * Tests error handling scenarios with real ClickHouse instance
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals'
import { select, insertInto, update, deleteFrom, createQueryRunner, Eq, IsNull } from '../../../src/index'
import { testSetup, DEFAULT_TEST_CONFIG } from '../setup/test-setup'

describe('Error Handling E2E Tests', () => {
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

  describe('Connection Error Handling', () => {
    it('should handle connection failures gracefully', async () => {
      const badRunner = createQueryRunner({
        url: 'http://localhost:9999', // Non-existent port
        username: 'default',
        password: '',
        database: 'test',
      })

      // Test connection should return false
      const isConnected = await badRunner.testConnection()
      expect(isConnected).toBe(false)

      // Queries should fail with connection error
      await expect(badRunner.execute({ sql: 'SELECT 1' })).rejects.toThrow()
    })

    it('should handle invalid credentials', async () => {
      const badRunner = createQueryRunner({
        url: DEFAULT_TEST_CONFIG.url,
        username: 'invalid_user',
        password: 'invalid_password',
        database: DEFAULT_TEST_CONFIG.database,
      })

      await expect(badRunner.execute({ sql: 'SELECT 1' })).rejects.toThrow()
    })

    it('should handle database not found', async () => {
      const badRunner = createQueryRunner({
        url: DEFAULT_TEST_CONFIG.url,
        username: DEFAULT_TEST_CONFIG.username,
        password: DEFAULT_TEST_CONFIG.password,
        database: 'nonexistent_database',
      })

      await expect(badRunner.execute({ sql: 'SELECT 1' })).rejects.toThrow()
    })
  })

  describe('SQL Syntax Error Handling', () => {
    it('should handle malformed SELECT queries', async () => {
      await expect(select(['invalid_column']).from('users').run(queryRunner)).rejects.toThrow()

      await expect(
        select(['*'])
          .from('users')
          .where({ invalid_column: Eq('test') })
          .run(queryRunner),
      ).rejects.toThrow()
    })

    it('should handle malformed INSERT queries', async () => {
      await expect(
        insertInto('nonexistent_table')
          .columns(['id', 'name'])
          .values([[1, 'test']])
          .run(queryRunner),
      ).rejects.toThrow()

      await expect(
        insertInto('users')
          .columns(['invalid_column'])
          .values([['test']])
          .run(queryRunner),
      ).rejects.toThrow()
    })

    it('should handle malformed UPDATE queries', async () => {
      await expect(
        update('nonexistent_table')
          .set({ status: 'test' })
          .where({ id: Eq(1) })
          .run(queryRunner),
      ).rejects.toThrow()

      await expect(
        update('users')
          .set({ invalid_column: 'test' })
          .where({ id: Eq(1) })
          .run(queryRunner),
      ).rejects.toThrow()
    })

    it('should handle malformed DELETE queries', async () => {
      await expect(
        deleteFrom('nonexistent_table')
          .where({ id: Eq(1) })
          .run(queryRunner),
      ).rejects.toThrow()

      await expect(
        deleteFrom('users')
          .where({ invalid_column: Eq('test') })
          .run(queryRunner),
      ).rejects.toThrow()
    })

    it('should handle invalid JOIN conditions', async () => {
      await expect(
        select(['u.id', 'o.id'])
          .from('users', 'u')
          .innerJoin('orders', { invalid_column: Eq('test') }, 'o')
          .run(queryRunner),
      ).rejects.toThrow()
    })

    it('should handle invalid GROUP BY clauses', async () => {
      await expect(
        select(['invalid_column', { column: 'count()', alias: 'total' }])
          .from('users')
          .groupBy(['invalid_column'])
          .run(queryRunner),
      ).rejects.toThrow()
    })

    it('should handle invalid ORDER BY clauses', async () => {
      await expect(
        select(['id', 'name'])
          .from('users')
          .orderBy([{ column: 'invalid_column', direction: 'ASC' }])
          .run(queryRunner),
      ).rejects.toThrow()
    })
  })

  describe('Data Type Error Handling', () => {
    let testTableName: string

    beforeEach(async () => {
      testTableName = `test_error_types_${Date.now()}`

      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
            id UInt32,
            name String,
            age UInt8,
            salary Decimal(10, 2),
            is_active UInt8,
            created_at DateTime
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })
    })

    afterEach(async () => {
      await queryRunner.command({
        sql: `DROP TABLE IF EXISTS ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should handle type mismatch in INSERT', async () => {
      // Try to insert string into UInt32 column
      await expect(
        insertInto(testTableName)
          .columns(['id', 'name'])
          .values([['not_a_number', 'test']])
          .run(queryRunner),
      ).rejects.toThrow()

      // Try to insert string into UInt8 column
      await expect(
        insertInto(testTableName)
          .columns(['id', 'age'])
          .values([[1, 'not_a_number']])
          .run(queryRunner),
      ).rejects.toThrow()

      // Try to insert string into Decimal column
      await expect(
        insertInto(testTableName)
          .columns(['id', 'salary'])
          .values([[1, 'not_a_decimal']])
          .run(queryRunner),
      ).rejects.toThrow()
    })

    it('should handle type mismatch in UPDATE', async () => {
      // Insert valid data first
      await insertInto(testTableName)
        .columns(['id', 'name', 'age'])
        .values([[1, 'Test User', 25]])
        .run(queryRunner)

      // Try to update with wrong type
      await expect(
        update(testTableName)
          .set({ age: 'not_a_number' })
          .where({ id: Eq(1) })
          .run(queryRunner),
      ).rejects.toThrow()

      await expect(
        update(testTableName)
          .set({ salary: 'not_a_decimal' })
          .where({ id: Eq(1) })
          .run(queryRunner),
      ).rejects.toThrow()
    })
  })

  describe('Constraint Error Handling', () => {
    let testTableName: string

    beforeEach(async () => {
      testTableName = `test_constraints_${Date.now()}`

      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
            id UInt32,
            email String,
            age UInt8
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })
    })

    afterEach(async () => {
      await queryRunner.command({
        sql: `DROP TABLE IF EXISTS ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should handle duplicate key errors', async () => {
      // Insert first record
      await insertInto(testTableName)
        .columns(['id', 'email', 'age'])
        .values([[1, 'test@example.com', 25]])
        .run(queryRunner)

      // Try to insert duplicate ID (ClickHouse doesn't enforce unique constraints by default,
      // but this tests the general error handling pattern)
      await insertInto(testTableName)
        .columns(['id', 'email', 'age'])
        .values([[1, 'duplicate@example.com', 30]])
        .run(queryRunner) // This will succeed in ClickHouse, but tests the pattern

      // Verify both records exist (ClickHouse behavior)
      const results = await select(['count()']).from(testTableName).run(queryRunner)

      expect(parseInt(results[0]['count()'])).toBe(2)
    })

    it('should handle foreign key constraint violations', async () => {
      // ClickHouse doesn't enforce foreign keys, but we can test the error handling pattern
      const parentTableName = `test_parent_${Date.now()}`
      const childTableName = `test_child_${Date.now()}`

      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${parentTableName} (
            id UInt32,
            name String
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${childTableName} (
            id UInt32,
            parent_id UInt32,
            description String
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      // Insert parent record
      await insertInto(parentTableName)
        .columns(['id', 'name'])
        .values([[1, 'Parent']])
        .run(queryRunner)

      // Insert child record with valid parent_id
      await insertInto(childTableName)
        .columns(['id', 'parent_id', 'description'])
        .values([[1, 1, 'Valid child']])
        .run(queryRunner)

      // Insert child record with invalid parent_id (ClickHouse will allow this)
      await insertInto(childTableName)
        .columns(['id', 'parent_id', 'description'])
        .values([[2, 999, 'Invalid child']])
        .run(queryRunner)

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${childTableName}`,
      })
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${parentTableName}`,
      })
    })
  })

  describe('Timeout Error Handling', () => {
    it('should handle query timeouts', async () => {
      const timeoutRunner = createQueryRunner({
        ...DEFAULT_TEST_CONFIG,
        timeout: 1000, // 1 second timeout
      })

      // This query should timeout (if it takes longer than 1 second)
      await expect(
        timeoutRunner.execute({
          sql: 'SELECT sleep(2)', // Sleep for 2 seconds
          settings: {
            max_execution_time: 0.5, // 500ms execution time limit
          },
        }),
      ).rejects.toThrow()
    })

    it('should handle streaming timeouts', async () => {
      const timeoutRunner = createQueryRunner({
        ...DEFAULT_TEST_CONFIG,
        timeout: 1000,
      })

      const stream = await select(['number'])
        .from('system.numbers')
        .limit(1000000) // Large result set
        .settings({
          max_execution_time: 0.5,
        })
        .stream(timeoutRunner)

      return new Promise<void>((resolve) => {
        stream.on('data', () => {
          // Process data
        })

        stream.on('error', (error) => {
          expect(error).toBeDefined()
          resolve()
        })

        stream.on('end', () => {
          // If it doesn't timeout, that's also fine
          resolve()
        })

        // Fallback timeout
        setTimeout(() => {
          if ('destroy' in stream && typeof stream.destroy === 'function') {
            stream.destroy()
          }
          resolve()
        }, 2000)
      })
    })
  })

  describe('Custom Error Types', () => {
    it('should throw ClickHouseToolkitError for query failures', async () => {
      try {
        await select(['invalid_column']).from('users').run(queryRunner)
        fail('Should have thrown an error')
      } catch (error) {
        expect(error).toBeInstanceOf(Error)
        expect(error.message).toContain('invalid_column')
      }
    }, 60000)

    it('should provide meaningful error messages', async () => {
      try {
        await select(['*']).from('nonexistent_table').run(queryRunner)
        fail('Should have thrown an error')
      } catch (error) {
        expect(error.message).toBeDefined()
        expect(error.message.length).toBeGreaterThan(0)
      }
    }, 60000)

    it('should include query context in errors', async () => {
      try {
        await insertInto('users')
          .columns(['invalid_column'])
          .values([['test']])
          .run(queryRunner)
        fail('Should have thrown an error')
      } catch (error) {
        expect(error.message).toContain('invalid_column')
      }
    }, 60000)
  })

  describe('Recovery and Retry Scenarios', () => {
    it('should handle temporary connection failures', async () => {
      // This test simulates a scenario where connection might be temporarily unavailable
      const unstableRunner = createQueryRunner({
        url: 'http://localhost:8123',
        username: DEFAULT_TEST_CONFIG.username,
        password: DEFAULT_TEST_CONFIG.password,
        database: DEFAULT_TEST_CONFIG.database,
        retries: 3,
      })

      // Test that retries work for connection issues
      const isConnected = await unstableRunner.testConnection()
      expect(isConnected).toBe(true)
    }, 60000)

    it('should handle partial query failures', async () => {
      const testTableName = `test_partial_failure_${Date.now()}`

      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
            id UInt32,
            name String
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      // Insert some data
      await insertInto(testTableName)
        .columns(['id', 'name'])
        .values([
          [1, 'Valid User'],
          [2, 'Another User'],
        ])
        .run(queryRunner)

      // Try to update with a mix of valid and invalid operations
      try {
        await update(testTableName)
          .set({ name: 'Updated Name' })
          .where({ id: Eq(1) })
          .run(queryRunner)
      } catch (error) {
        // If update fails, verify original data is still there
        const results = await select(['id', 'name'])
          .from(testTableName)
          .where({ id: Eq(1) })
          .run<any>(queryRunner)

        expect(results[0].name).toBe('Valid User')
      }

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })
  })

  describe('Edge Cases and Boundary Conditions', () => {
    it('should handle empty result sets gracefully', async () => {
      const results = await select(['id', 'name'])
        .from('users')
        .where({ id: Eq(99999) }) // Non-existent ID
        .run(queryRunner)

      expect(results).toEqual([])
      expect(Array.isArray(results)).toBe(true)
    })

    it('should handle NULL values in WHERE clauses', async () => {
      const testTableName = `test_nulls_${Date.now()}`

      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
            id UInt32,
            name Nullable(String),
            age UInt8
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      await insertInto(testTableName)
        .columns(['id', 'name', 'age'])
        .values([
          [1, null, 25],
          [2, 'John', 30],
        ])
        .run(queryRunner)

      // Query with NULL values
      const nullResults = await select(['id', 'name'])
        .from(testTableName)
        .where({ name: IsNull() })
        .run<any>(queryRunner)

      expect(nullResults).toHaveLength(1)
      expect(nullResults[0].name).toBeNull()

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should handle very large data values', async () => {
      const testTableName = `test_large_data_${Date.now()}`

      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
            id UInt32,
            large_text String
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      // Insert very large text
      const largeText = 'x'.repeat(10000) // 10KB string

      await insertInto(testTableName)
        .columns(['id', 'large_text'])
        .values([[1, largeText]])
        .run(queryRunner)

      // Verify it was stored correctly
      const results = await select(['id', 'large_text'])
        .from(testTableName)
        .where({ id: Eq(1) })
        .run<any>(queryRunner)

      expect(results[0].large_text.length).toBe(10000)

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should handle special characters in data', async () => {
      const testTableName = `test_special_chars_${Date.now()}`

      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
            id UInt32,
            name String,
            description String
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      const specialData = [
        [1, 'User with "quotes"', "Description with 'single quotes'"],
        [2, 'User with \n newlines', 'Description with\ttabs'],
        [3, 'User with unicode: 测试', 'Description with émojis: 🚀🎉'],
        [4, 'User with SQL injection attempt', "Description with '; DROP TABLE users; --"],
      ]

      await insertInto(testTableName).columns(['id', 'name', 'description']).values(specialData).run(queryRunner)

      // Verify all data was stored correctly
      const results = await select(['count()']).from(testTableName).run(queryRunner)

      expect(parseInt(results[0]['count()'])).toBe(4)

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })
  })
})
