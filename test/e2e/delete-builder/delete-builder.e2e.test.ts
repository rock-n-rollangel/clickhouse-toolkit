/**
 * DeleteBuilder E2E Tests
 * Tests DeleteBuilder query generation and execution (without verifying mutations due to async nature)
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals'
import { deleteFrom, insertInto, Eq, Gt, In, Between, IsNotNull } from '../../../src/index'
import { testSetup, DEFAULT_TEST_CONFIG } from '../setup/test-setup'

describe('DeleteBuilder E2E Tests', () => {
  let queryRunner: any
  let testTableName: string

  beforeAll(async () => {
    await testSetup.setup()
    queryRunner = testSetup.getQueryRunner()
  }, 60000)

  afterAll(async () => {
    await testSetup.teardown()
  }, 30000)

  beforeEach(async () => {
    if (!queryRunner) {
      queryRunner = testSetup.getQueryRunner()
    }
    testTableName = `test_delete_${Date.now()}`

    // Create test table
    await queryRunner.command({
      sql: `
        CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
          id UInt32,
          name String,
          email String,
          age UInt8,
          status String DEFAULT 'active',
          salary Decimal(10, 2)
        ) ENGINE = MergeTree() ORDER BY id
      `,
    })

    // Insert test data
    await insertInto(testTableName)
      .columns(['id', 'name', 'email', 'age', 'status', 'salary'])
      .values([
        [1, 'John Doe', 'john@example.com', 30, 'active', 50000.0],
        [2, 'Jane Smith', 'jane@example.com', 25, 'active', 45000.0],
        [3, 'Bob Johnson', 'bob@example.com', 35, 'inactive', 60000.0],
        [4, 'Alice Brown', 'alice@example.com', 28, 'active', 55000.0],
        [5, 'Charlie Wilson', 'charlie@example.com', 42, 'pending', 70000.0],
      ])
      .run(queryRunner)
  })

  afterEach(async () => {
    await queryRunner.command({
      sql: `DROP TABLE IF EXISTS ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
    })
  })

  describe('Basic Delete Operations', () => {
    it('should execute delete with simple condition', async () => {
      await expect(
        deleteFrom(testTableName)
          .where({ id: Eq(1) })
          .run(queryRunner),
      ).resolves.not.toThrow()
    })

    it('should execute delete with complex conditions', async () => {
      await expect(
        deleteFrom(testTableName)
          .where({ status: Eq('inactive') })
          .run(queryRunner),
      ).resolves.not.toThrow()
    })

    it('should execute delete with no matching rows', async () => {
      await expect(
        deleteFrom(testTableName)
          .where({ id: Eq(999) })
          .run(queryRunner),
      ).resolves.not.toThrow()
    })
  })

  describe('Delete with Complex Conditions', () => {
    it('should execute delete with range condition', async () => {
      await expect(
        deleteFrom(testTableName)
          .where({ age: Gt(40) })
          .run(queryRunner),
      ).resolves.not.toThrow()
    })

    it('should execute delete with IN condition', async () => {
      await expect(
        deleteFrom(testTableName)
          .where({ id: In([2, 4]) })
          .run(queryRunner),
      ).resolves.not.toThrow()
    })

    it('should execute delete with BETWEEN condition', async () => {
      await expect(
        deleteFrom(testTableName)
          .where({ salary: Between(45000, 55000) })
          .run(queryRunner),
      ).resolves.not.toThrow()
    })

    it('should execute delete with IS NOT NULL condition', async () => {
      await expect(deleteFrom(testTableName).where({ email: IsNotNull() }).run(queryRunner)).resolves.not.toThrow()
    })
  })

  describe('Error Handling', () => {
    it('should handle delete with invalid table name', async () => {
      await expect(
        deleteFrom('nonexistent_table')
          .where({ id: Eq(1) })
          .run(queryRunner),
      ).rejects.toThrow()
    })

    it('should handle delete with invalid column in WHERE clause', async () => {
      await expect(
        deleteFrom(testTableName)
          .where({ invalid_column: Eq('test') })
          .run(queryRunner),
      ).rejects.toThrow()
    })
  })
})
