/**
 * QueryRunner E2E Tests
 * Tests QueryRunner functionality with real ClickHouse instance
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals'
import { QueryRunner } from '../../../src/runner/query-runner'
import { testSetup, DEFAULT_TEST_CONFIG } from '../setup/test-setup'

describe('QueryRunner E2E Tests', () => {
  let queryRunner: QueryRunner

  beforeAll(async () => {
    await testSetup.setup()
    queryRunner = testSetup.getQueryRunner()
  }, 60000) // 60 second timeout for container startup

  afterAll(async () => {
    await testSetup.teardown()
  }, 30000)

  beforeEach(async () => {
    // Ensure we have a fresh connection for each test
    if (!queryRunner) {
      queryRunner = testSetup.getQueryRunner()
    }
  })

  describe('Connection Management', () => {
    it('should establish connection to ClickHouse', async () => {
      const isConnected = await queryRunner.testConnection()
      expect(isConnected).toBe(true)
    })

    it('should close connection gracefully', async () => {
      const testRunner = new QueryRunner(DEFAULT_TEST_CONFIG)
      await testRunner.testConnection() // Ensure connection works
      await testRunner.close() // Should not throw
    })
  })

  describe('Command Execution', () => {
    it('should handle command errors gracefully', async () => {
      await expect(
        queryRunner.command({
          sql: 'INVALID SQL STATEMENT',
        }),
      ).rejects.toThrow()
    })
  })

  describe('Query Execution', () => {
    it('should execute SELECT queries and return results', async () => {
      const results = await queryRunner.execute<{ count(): string }>({
        sql: 'SELECT count() as count FROM system.databases',
      })

      expect(Array.isArray(results)).toBe(true)
      expect(results.length).toBeGreaterThan(0)
    })

    it('should handle empty result sets', async () => {
      const results = await queryRunner.execute<{ id: number }>({
        sql: 'SELECT id FROM test_db.users WHERE id = 99999',
      })

      expect(results).toHaveLength(0)
      expect(Array.isArray(results)).toBe(true)
    })
  })

  describe('Error Handling', () => {
    it('should handle connection errors gracefully', async () => {
      const badRunner = new QueryRunner({
        url: 'http://localhost:9999',
        username: 'default',
        password: '',
        database: 'test',
      })

      await expect(badRunner.testConnection()).resolves.toBe(false)
    })
  })

  describe('New Insert Methods', () => {
    let testTableName: string

    beforeEach(() => {
      testTableName = `test_insert_${Date.now()}`
    })

    it('should insert objects using insert() method', async () => {
      // Create test table
      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
            id UInt32,
            name String,
            email String,
            age UInt8
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      // Insert objects using new insert() method
      const testData = [
        { id: 1, name: 'Alice', email: 'alice@example.com', age: 25 },
        { id: 2, name: 'Bob', email: 'bob@example.com', age: 30 },
      ]

      await queryRunner.insert({
        table: testTableName,
        values: testData,
        format: 'JSONEachRow',
      })

      // Verify insertion
      const results = await queryRunner.execute({
        sql: `SELECT * FROM ${DEFAULT_TEST_CONFIG.database}.${testTableName} ORDER BY id`,
        format: 'JSON',
      })

      expect(results).toHaveLength(2)
      expect((results[0] as any).id).toBe(1)
      expect((results[0] as any).name).toBe('Alice')
      expect((results[1] as any).id).toBe(2)
      expect((results[1] as any).name).toBe('Bob')

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should insert from stream using insert() method', async () => {
      // Create test table
      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
            id UInt32,
            name String,
            value UInt32
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      // Create CSV stream
      const { Readable } = await import('stream')
      const csvData = '1,Test1,100\n2,Test2,200\n3,Test3,300'
      const csvStream = Readable.from([csvData], { objectMode: false })

      // Insert from stream using new insert() method
      await queryRunner.insert({
        table: testTableName,
        values: csvStream,
        format: 'CSV',
      })

      // Verify insertion
      const results = await queryRunner.execute({
        sql: `SELECT * FROM ${DEFAULT_TEST_CONFIG.database}.${testTableName} ORDER BY id`,
        format: 'JSON',
      })

      expect(results).toHaveLength(3)
      expect((results[0] as any).id).toBe(1)
      expect((results[0] as any).name).toBe('Test1')
      expect((results[0] as any).value).toBe(100)
      expect((results[2] as any).id).toBe(3)
      expect((results[2] as any).name).toBe('Test3')
      expect((results[2] as any).value).toBe(300)

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })
  })

  describe('Format Integration', () => {
    it('should execute query with format parameter', async () => {
      const results = await queryRunner.execute({
        sql: "SELECT 1 as id, 'test' as name",
        format: 'JSONCompactEachRow',
      })

      expect(Array.isArray(results)).toBe(true)
      expect(results.length).toBeGreaterThan(0)
    })

    it('should stream query with format parameter', async () => {
      const stream = await queryRunner.stream({
        sql: "SELECT 1 as id, 'test' as name",
        format: 'JSONEachRow',
      })

      const results: any[] = []

      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          if (results.length > 0) {
            expect(results[0]).toHaveProperty('id')
            expect(results[0]).toHaveProperty('name')
            resolve()
          } else {
            reject(new Error('No data received'))
          }
        }, 2000)

        stream.on('data', (rows: any[]) => {
          results.push(...rows)
          if (results.length >= 1) {
            clearTimeout(timeout)
            resolve()
          }
        })

        stream.on('error', (error) => {
          clearTimeout(timeout)
          reject(error)
        })
      })
    })

    it('should validate streamable formats for streaming', async () => {
      // CSV should be streamable
      const csvStream = await queryRunner.stream({
        sql: 'SELECT 1 as id',
        format: 'CSV',
      })
      expect(csvStream).toBeDefined()

      // JSONEachRow should be streamable
      const jsonStream = await queryRunner.stream({
        sql: 'SELECT 1 as id',
        format: 'JSONEachRow',
      })
      expect(jsonStream).toBeDefined()
    })

    it('should throw error for non-streamable format in stream', async () => {
      // JSON format is not streamable
      await expect(
        queryRunner.stream({
          sql: 'SELECT 1 as id',
          format: 'JSON',
        }),
      ).rejects.toThrow()
    })
  })
})
