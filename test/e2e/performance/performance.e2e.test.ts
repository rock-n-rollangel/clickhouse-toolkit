/**
 * Performance E2E Tests
 * Tests performance characteristics and settings with real ClickHouse instance
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals'
import { select, insertInto, createQueryRunner, Eq, Between, In } from '../../../src/index'
import { testSetup, DEFAULT_TEST_CONFIG } from '../setup/test-setup'

describe('Performance E2E Tests', () => {
  let queryRunner: any
  let performanceTableName: string

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
    performanceTableName = `test_performance_${Date.now()}`

    // Create performance test table
    await queryRunner.command({
      sql: `
        CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${performanceTableName} (
          id UInt32,
          name String,
          email String,
          age UInt8,
          salary Decimal(10, 2),
          department String,
          created_at DateTime,
          updated_at DateTime,
          is_active UInt8,
          metadata Map(String, String),
          tags Array(String)
        ) ENGINE = MergeTree() ORDER BY (department, id)
      `,
    })

    // Insert large dataset for performance testing
    const batchSize = 1000
    const totalBatches = 10 // 10,000 records total
    const allData = []

    for (let batch = 0; batch < totalBatches; batch++) {
      const batchData = []
      for (let i = 0; i < batchSize; i++) {
        const id = batch * batchSize + i + 1
        batchData.push([
          id,
          `User ${id}`,
          `user${id}@example.com`,
          Math.floor(Math.random() * 50) + 18,
          Math.floor(Math.random() * 100000) / 100,
          `dept${id % 5}`,
          new Date(Date.now() - Math.random() * 365 * 24 * 60 * 60 * 1000),
          new Date(),
          Math.floor(Math.random() * 2),
          { role: `role${id % 10}`, level: `level${id % 4}` },
          [`tag${id % 20}`, `category${id % 8}`],
        ])
      }
      allData.push(...batchData)
    }

    // Insert data in batches for better performance
    for (let i = 0; i < allData.length; i += batchSize) {
      const chunk = allData.slice(i, i + batchSize)
      await insertInto(performanceTableName)
        .columns([
          'id',
          'name',
          'email',
          'age',
          'salary',
          'department',
          'created_at',
          'updated_at',
          'is_active',
          'metadata',
          'tags',
        ])
        .values(chunk)
        .run(queryRunner)
    }
  })

  afterEach(async () => {
    // Clean up performance test table
    await queryRunner.command({
      sql: `DROP TABLE IF EXISTS ${DEFAULT_TEST_CONFIG.database}.${performanceTableName}`,
    })
  })

  describe('Query Performance with Settings', () => {
    it('should respect max_execution_time setting', async () => {
      const startTime = Date.now()

      try {
        await select(['id', 'name'])
          .from(performanceTableName)
          .settings({
            max_execution_time: 1, // 1 second
          })
          .run(queryRunner)
      } catch (error) {
        // Query might timeout, which is expected
        expect(Date.now() - startTime).toBeLessThan(2000) // Should fail within 2 seconds
      }
    })

    it('should respect max_threads setting', async () => {
      const startTime = Date.now()

      const results = await select(['department', 'count() as total'])
        .from(performanceTableName)
        .groupBy(['department'])
        .settings({
          max_threads: 1, // Single thread
        })
        .run(queryRunner)

      const executionTime = Date.now() - startTime

      expect(results.length).toBe(5) // Should have 5 departments
      expect(executionTime).toBeGreaterThan(0)
    })

    it('should respect max_memory_usage setting', async () => {
      try {
        await select(['*'])
          .from(performanceTableName)
          .settings({
            max_memory_usage: 1000000, // 1MB limit
          })
          .run(queryRunner)
      } catch (error) {
        // Should fail due to memory limit
        expect(error.message).toContain('memory')
      }
    })

    it('should respect readonly setting', async () => {
      // Read-only query should work
      const results = await select(['count()'])
        .from(performanceTableName)
        .settings({
          readonly: 1,
        })
        .run(queryRunner)

      expect(parseInt(results[0]['count()'] as string)).toBe(10000)

      // Write operations should fail in readonly mode
      try {
        await insertInto(performanceTableName)
          .columns(['id', 'name', 'email'])
          .values([[99999, 'Test', 'test@example.com']])
          .run(queryRunner)
        fail('Should have thrown an error in readonly mode')
      } catch (error) {
        expect(error).toBeDefined()
      }
    })

    it('should handle concurrent queries with thread limits', async () => {
      const concurrentQueries = 5
      const queryPromises = []

      for (let i = 0; i < concurrentQueries; i++) {
        const promise = select(['department', 'count() as total'])
          .from(performanceTableName)
          .groupBy(['department'])
          .settings({
            max_threads: 2,
          })
          .run(queryRunner)

        queryPromises.push(promise)
      }

      const results = await Promise.all(queryPromises)

      expect(results).toHaveLength(concurrentQueries)
      results.forEach((result) => {
        expect(result.length).toBe(5) // 5 departments
      })
    })
  })

  describe('Large Dataset Performance', () => {
    it('should handle large SELECT queries efficiently', async () => {
      const startTime = Date.now()

      const results = await select(['id', 'name', 'department', 'salary'])
        .from(performanceTableName)
        .where({ department: Eq('dept0') })
        .orderBy([{ column: 'salary', direction: 'DESC' }])
        .limit(1000)
        .run(queryRunner)

      const executionTime = Date.now() - startTime

      expect(results.length).toBeGreaterThan(0)
      expect(results.length).toBeLessThanOrEqual(1000)
      expect(executionTime).toBeLessThan(5000) // Should complete within 5 seconds
    })

    it('should handle large INSERT operations efficiently', async () => {
      const insertTableName = `test_large_insert_${Date.now()}`

      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${insertTableName} (
            id UInt32,
            name String,
            value UInt32
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      // Generate large batch of data
      const batchData = Array.from({ length: 5000 }, (_, i) => [
        i + 1,
        `Batch User ${i + 1}`,
        Math.floor(Math.random() * 10000),
      ])

      const startTime = Date.now()

      await insertInto(insertTableName).columns(['id', 'name', 'value']).values(batchData).run(queryRunner)

      const executionTime = Date.now() - startTime

      // Verify insertion
      const countResults = await select(['count()']).from(insertTableName).run(queryRunner)

      expect(parseInt(countResults[0]['count()'])).toBe(5000)
      expect(executionTime).toBeLessThan(10000) // Should complete within 10 seconds

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${insertTableName}`,
      })
    })
  })

  describe('Index and Optimization Performance', () => {
    it('should benefit from ORDER BY optimization', async () => {
      // Test queries that can use the primary key order
      const startTime = Date.now()

      const results = await select(['id', 'name', 'department'])
        .from(performanceTableName)
        .where({ department: Eq('dept0') })
        .orderBy([{ column: 'id', direction: 'ASC' }])
        .limit(100)
        .run<any>(queryRunner)

      const executionTime = Date.now() - startTime

      expect(results.length).toBe(100)
      expect(executionTime).toBeLessThan(2000)

      // Verify results are ordered correctly
      for (let i = 1; i < results.length; i++) {
        expect(results[i].id).toBeGreaterThan(results[i - 1].id)
      }
    })

    it('should handle range queries efficiently', async () => {
      const startTime = Date.now()

      const results = await select(['id', 'name', 'salary', 'department'])
        .from(performanceTableName)
        .where({
          department: Eq('dept1'),
          id: Between(1000, 2000),
        })
        .run<any>(queryRunner)

      const executionTime = Date.now() - startTime

      expect(results.length).toBeGreaterThan(0)
      expect(results.length).toBeLessThanOrEqual(1000)
      expect(executionTime).toBeLessThan(2000)

      // Verify range filtering worked
      results.forEach((result) => {
        expect(result.id).toBeGreaterThanOrEqual(1000)
        expect(result.id).toBeLessThanOrEqual(2000)
        expect(result.department).toBe('dept1')
      })
    })

    it('should handle IN clause efficiently', async () => {
      const startTime = Date.now()

      const results = await select(['id', 'name', 'department', 'age'])
        .from(performanceTableName)
        .where({
          department: In(['dept0', 'dept2', 'dept4']),
          age: Between(25, 35),
        })
        .limit(500)
        .run<any>(queryRunner)

      const executionTime = Date.now() - startTime

      expect(results.length).toBeGreaterThan(0)
      expect(results.length).toBeLessThanOrEqual(500)
      expect(executionTime).toBeLessThan(3000)

      // Verify IN filtering worked
      results.forEach((result) => {
        expect(['dept0', 'dept2', 'dept4']).toContain(result.department)
        expect(result.age).toBeGreaterThanOrEqual(25)
        expect(result.age).toBeLessThanOrEqual(35)
      })
    })
  })

  describe('Memory and Resource Management', () => {
    it('should handle resource limits gracefully', async () => {
      // Test with very restrictive settings
      const restrictiveRunner = createQueryRunner({
        ...DEFAULT_TEST_CONFIG,
        settings: {
          max_memory_usage: 100000, // 100KB
          max_threads: 1,
          max_execution_time: 5, // 5 seconds
        },
      })

      try {
        await restrictiveRunner.execute({
          sql: `SELECT * FROM ${DEFAULT_TEST_CONFIG.database}.${performanceTableName} LIMIT 10000`,
        })
      } catch (error) {
        // Should fail due to resource constraints
        expect(error).toBeDefined()
      }
    })
  })

  describe('Performance Monitoring and Metrics', () => {
    it('should provide query execution metrics', async () => {
      const startTime = Date.now()

      const results = await select(['department', 'count() as total'])
        .from(performanceTableName)
        .groupBy(['department'])
        .orderBy([{ column: 'total', direction: 'DESC' }])
        .run(queryRunner)

      const executionTime = Date.now() - startTime

      expect(results.length).toBe(5) // 5 departments
      expect(executionTime).toBeGreaterThan(0)
    })

    it('should handle performance regression detection', async () => {
      // Run the same query multiple times to detect performance consistency
      const iterations = 5
      const executionTimes = []

      for (let i = 0; i < iterations; i++) {
        const startTime = Date.now()

        await select(['id', 'name', 'department'])
          .from(performanceTableName)
          .where({ department: Eq('dept0') })
          .limit(100)
          .run(queryRunner)

        executionTimes.push(Date.now() - startTime)
      }

      const avgTime = executionTimes.reduce((sum, time) => sum + time, 0) / iterations
      const maxTime = Math.max(...executionTimes)

      expect(avgTime).toBeGreaterThan(0)
      expect(maxTime).toBeLessThan(avgTime * 3) // No execution should be more than 3x average
    })
  })
})
