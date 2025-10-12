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
})
