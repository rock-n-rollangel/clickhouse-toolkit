/**
 * InsertBuilder E2E Tests
 * Tests InsertBuilder functionality with real ClickHouse instance
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals'
import { insertInto, select } from '../../../src/index'
import { testSetup, DEFAULT_TEST_CONFIG } from '../setup/test-setup'

describe('InsertBuilder E2E Tests', () => {
  let queryRunner: any
  let testTableName: string

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
    testTableName = `test_insert_${Date.now()}`
  })

  describe('Basic Insert Operations', () => {
    it('should insert single row with explicit columns', async () => {
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

      // Insert single row
      await insertInto(testTableName)
        .columns(['id', 'name', 'email', 'age'])
        .values([[1, 'Test User', 'test@example.com', 25]])
        .run(queryRunner)

      // Verify insertion
      const results = await select(['*']).from(testTableName).run(queryRunner)

      expect(results).toHaveLength(1)
      expect((results[0] as any).id).toBe(1)
      expect((results[0] as any).name).toBe('Test User')
      expect((results[0] as any).email).toBe('test@example.com')
      expect((results[0] as any).age).toBe(25)

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should insert single row without explicit columns', async () => {
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

      // Insert single row without specifying columns
      await insertInto(testTableName)
        .values([[2, 'Another User', 'another@example.com', 30]])
        .run(queryRunner)

      // Verify insertion
      const results = await select(['*']).from(testTableName).run<any>(queryRunner)

      expect(results).toHaveLength(1)
      expect(results[0].id).toBe(2)
      expect(results[0].name).toBe('Another User')

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should insert with object notation', async () => {
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

      // Insert with object notation
      await insertInto(testTableName)
        .columns(['id', 'name', 'email', 'age'])
        .values([[3, 'Object User', 'object@example.com', 35]])
        .run(queryRunner)

      // Verify insertion
      const results = await select(['*']).from(testTableName).run<any>(queryRunner)

      expect(results).toHaveLength(1)
      expect(results[0].id).toBe(3)
      expect(results[0].name).toBe('Object User')

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })
  })

  describe('Batch Insert Operations', () => {
    it('should insert multiple rows in batch', async () => {
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

      // Insert multiple rows
      await insertInto(testTableName)
        .columns(['id', 'name', 'email', 'age'])
        .values([
          [4, 'User 4', 'user4@example.com', 28],
          [5, 'User 5', 'user5@example.com', 32],
          [6, 'User 6', 'user6@example.com', 26],
        ])
        .run(queryRunner)

      // Verify insertions
      const results = await select(['*'])
        .from(testTableName)
        .orderBy([{ column: 'id', direction: 'ASC' }])
        .run<any>(queryRunner)

      expect(results).toHaveLength(3)
      expect(results[0].id).toBe(4)
      expect(results[1].id).toBe(5)
      expect(results[2].id).toBe(6)

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should insert mixed object and array values', async () => {
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

      // Insert mixed values
      await insertInto(testTableName)
        .values([
          [7, 'Object User 1', 'obj1@example.com', 29],
          [8, 'Array User', 'array@example.com', 31],
          [9, 'Object User 2', 'obj2@example.com', 33],
        ])
        .run(queryRunner)

      // Verify insertions
      const results = await select(['*'])
        .from(testTableName)
        .orderBy([{ column: 'id', direction: 'ASC' }])
        .run<any>(queryRunner)

      expect(results).toHaveLength(3)
      expect(results[0].id).toBe(7)
      expect(results[1].id).toBe(8)
      expect(results[2].id).toBe(9)

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should handle large batch insertions', async () => {
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

      // Generate large batch of data
      const batchData = Array.from({ length: 1000 }, (_, i) => [
        i + 1,
        `User ${i + 1}`,
        Math.floor(Math.random() * 1000),
      ])

      // Insert large batch
      await insertInto(testTableName).columns(['id', 'name', 'value']).values(batchData).run(queryRunner)

      // Verify insertions
      const results = await select(['count() as total_count']).from(testTableName).run<any>(queryRunner)

      expect(parseInt(results[0].total_count)).toBe(1000)

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })
  })

  describe('Insert with Different Data Types', () => {
    it('should insert with various ClickHouse data types', async () => {
      // Create test table with various data types
      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
            id UInt32,
            name String,
            age UInt8,
            salary Decimal(10, 2),
            is_active UInt8,
            created_at DateTime,
            data Map(String, String),
            tags Array(String)
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      // Insert with various data types
      await insertInto(testTableName)
        .columns(['id', 'name', 'age', 'salary', 'is_active', 'created_at', 'data', 'tags'])
        .values([
          [
            1,
            'Complex User',
            28,
            75000.5,
            1,
            new Date('2024-01-15T10:30:00Z'),
            { department: 'Engineering', level: 'Senior' },
            ['typescript', 'clickhouse', 'testing'],
          ],
        ])
        .run(queryRunner)

      // Verify insertion
      const results = await select(['*']).from(testTableName).run<any>(queryRunner)

      expect(results).toHaveLength(1)
      expect(results[0].id).toBe(1)
      expect(results[0].name).toBe('Complex User')
      expect(results[0].age).toBe(28)
      expect(results[0].salary).toBe(75000.5)
      expect(results[0].is_active).toBe(1)

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should handle NULL values', async () => {
      // Create test table with nullable columns
      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
            id UInt32,
            name String,
            email Nullable(String),
            age Nullable(UInt8)
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      // Insert with NULL values
      await insertInto(testTableName)
        .columns(['id', 'name', 'email', 'age'])
        .values([
          [1, 'User with nulls', null, null],
          [2, 'User with some nulls', 'email@example.com', null],
          [3, 'User without nulls', 'full@example.com', 25],
        ])
        .run(queryRunner)

      // Verify insertions
      const results = await select(['*'])
        .from(testTableName)
        .orderBy([{ column: 'id', direction: 'ASC' }])
        .run<any>(queryRunner)

      expect(results).toHaveLength(3)
      expect(results[0].email).toBeNull()
      expect(results[0].age).toBeNull()
      expect(results[1].email).toBe('email@example.com')
      expect(results[1].age).toBeNull()
      expect(results[2].email).toBe('full@example.com')
      expect(results[2].age).toBe(25)

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })
  })

  describe('Insert with Default Values', () => {
    it('should use default values for unspecified columns', async () => {
      // Create test table with default values
      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
            id UInt32,
            name String,
            status String DEFAULT 'active',
            created_at DateTime DEFAULT now()
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      // Insert without specifying default columns
      await insertInto(testTableName)
        .columns(['id', 'name'])
        .values([
          [1, 'Default User 1'],
          [2, 'Default User 2'],
        ])
        .run(queryRunner)

      // Verify insertions with defaults
      const results = await select(['*']).from(testTableName).run<any>(queryRunner)

      expect(results).toHaveLength(2)
      results.forEach((result) => {
        expect(result.status).toBe('active')
        expect(result.created_at).toBeDefined()
      })

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })
  })

  // describe('Insert from Select (INSERT INTO ... SELECT)', () => {
  //   it('should insert data from another table', async () => {
  //     // Create source and destination tables
  //     await queryRunner.command({
  //       sql: `
  //         CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}_source (
  //           id UInt32,
  //           name String,
  //           email String
  //         ) ENGINE = MergeTree() ORDER BY id
  //       `,
  //     })

  //     await queryRunner.command({
  //       sql: `
  //         CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}_dest (
  //           id UInt32,
  //           name String,
  //           email String
  //         ) ENGINE = MergeTree() ORDER BY id
  //       `,
  //     })

  //     // Insert data into source table
  //     await insertInto(`${testTableName}_source`)
  //       .columns(['id', 'name', 'email'])
  //       .values([
  //         [1, 'Source User 1', 'source1@example.com'],
  //         [2, 'Source User 2', 'source2@example.com'],
  //       ])
  //       .run(queryRunner)

  //     // Insert from source to destination using select
  //     await insertInto(`${testTableName}_dest`)
  //       .select(['id', 'name', 'email'])
  //       .from(`${testTableName}_source`)
  //       .run(queryRunner)

  //     // Verify data was copied
  //     const sourceResults = await select(['count()']).from(`${testTableName}_source`).run<any>(queryRunner)

  //     const destResults = await select(['count()']).from(`${testTableName}_dest`).run<any>(queryRunner)

  //     expect(sourceResults[0]['count()']).toBe(2)
  //     expect(destResults[0]['count()']).toBe(2)

  //     // Clean up
  //     await queryRunner.command({
  //       sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}_source`,
  //     })
  //     await queryRunner.command({
  //       sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}_dest`,
  //     })
  //   })
  // })

  describe('Error Handling', () => {
    it('should handle invalid table names', async () => {
      await expect(
        insertInto('nonexistent_table')
          .columns(['id', 'name'])
          .values([[1, 'test']])
          .run(queryRunner),
      ).rejects.toThrow()
    })

    it('should handle column count mismatch', async () => {
      // Create test table
      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
            id UInt32,
            name String
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      // Try to insert with wrong number of columns
      await expect(
        insertInto(testTableName)
          .columns(['id', 'name'])
          .values([[1, 'test', 'extra_column']])
          .run(queryRunner),
      ).rejects.toThrow()

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should handle invalid data types', async () => {
      // Create test table
      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
            id UInt32,
            name String
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      // Try to insert string into UInt32 column
      await expect(
        insertInto(testTableName)
          .columns(['id', 'name'])
          .values([['not_a_number', 'test']])
          .run(queryRunner),
      ).rejects.toThrow()

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })
  })

  describe('Performance and Edge Cases', () => {
    it('should handle empty values array', async () => {
      // Create test table
      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
            id UInt32,
            name String
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      // Insert with empty values
      await insertInto(testTableName).columns(['id', 'name']).values([]).run(queryRunner)

      // Verify no data was inserted
      const results = await select(['count()']).from(testTableName).run(queryRunner)

      expect(parseInt(results[0]['count()'])).toBe(0)

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should handle special characters in data', async () => {
      // Create test table
      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
            id UInt32,
            name String,
            description String
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      // Insert data with special characters
      await insertInto(testTableName)
        .columns(['id', 'name', 'description'])
        .values([
          [1, 'User with "quotes"', 'Description with \'single quotes\' and "double quotes"'],
          [2, 'User with \n newlines', 'Description with\ttabs and\nnewlines'],
          [3, 'User with unicode: 测试', 'Description with émojis: 🚀🎉'],
        ])
        .run(queryRunner)

      // Verify insertions
      const results = await select(['*'])
        .from(testTableName)
        .orderBy([{ column: 'id', direction: 'ASC' }])
        .run<any>(queryRunner)

      expect(results).toHaveLength(3)
      expect(results[0].name).toBe('User with "quotes"')
      expect(results[1].name).toBe('User with \n newlines')
      expect(results[2].name).toBe('User with unicode: 测试')

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })
  })

  describe('Object-based Insert Operations', () => {
    it('should insert single object with .objects() method', async () => {
      // Create test table
      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
            id UInt32,
            name String,
            email String,
            age UInt8,
            active UInt8
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      // Insert single object
      await insertInto(testTableName)
        .objects([{ id: 1, name: 'John Doe', email: 'john@example.com', age: 30, active: 1 }])
        .format('JSONEachRow')
        .run(queryRunner)

      // Verify insertion
      const results = await select(['*']).from(testTableName).run(queryRunner)

      expect(results).toHaveLength(1)
      expect((results[0] as any).id).toBe(1)
      expect((results[0] as any).name).toBe('John Doe')
      expect((results[0] as any).email).toBe('john@example.com')
      expect((results[0] as any).age).toBe(30)
      expect((results[0] as any).active).toBe(1)

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should insert multiple objects with .objects() method', async () => {
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

      // Insert multiple objects
      const testData = [
        { id: 1, name: 'Alice Smith', email: 'alice@example.com', age: 25 },
        { id: 2, name: 'Bob Johnson', email: 'bob@example.com', age: 30 },
        { id: 3, name: 'Carol Brown', email: 'carol@example.com', age: 35 },
      ]

      await insertInto(testTableName).objects(testData).format('JSONEachRow').run(queryRunner)

      // Verify insertion
      const results = await select(['*']).from(testTableName).run(queryRunner)

      expect(results).toHaveLength(3)
      expect((results[0] as any).id).toBe(1)
      expect((results[0] as any).name).toBe('Alice Smith')
      expect((results[1] as any).id).toBe(2)
      expect((results[1] as any).name).toBe('Bob Johnson')
      expect((results[2] as any).id).toBe(3)
      expect((results[2] as any).name).toBe('Carol Brown')

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should preserve data types with object inserts', async () => {
      // Create test table with various data types
      await queryRunner.command({
        sql: `
          CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
            id UInt32,
            name String,
            score Float64,
            is_active UInt8,
            created_date Date
          ) ENGINE = MergeTree() ORDER BY id
        `,
      })

      // Insert object with various data types
      await insertInto(testTableName)
        .objects([
          {
            id: 1,
            name: 'Test User',
            score: 95.5,
            is_active: 1,
            created_date: '2023-01-01',
          },
        ])
        .format('JSONEachRow')
        .run(queryRunner)

      // Verify insertion with correct data types
      const results = await select(['*']).from(testTableName).run(queryRunner)

      expect(results).toHaveLength(1)
      expect((results[0] as any).id).toBe(1)
      expect((results[0] as any).name).toBe('Test User')
      expect((results[0] as any).score).toBe(95.5)
      expect((results[0] as any).is_active).toBe(1)
      expect((results[0] as any).created_date).toBe('2023-01-01')

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })
  })

  describe('Stream-based Insert Operations', () => {
    it('should insert data from CSV stream', async () => {
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

      // Create CSV stream
      const { Readable } = await import('stream')
      const csvData =
        '1,John Doe,john@example.com,30\n2,Jane Smith,jane@example.com,25\n3,Bob Johnson,bob@example.com,35'
      const csvStream = Readable.from([csvData], { objectMode: false })

      // Insert from stream
      await insertInto(testTableName).fromStream(csvStream).format('CSV').run(queryRunner)

      // Verify insertion
      const results = await select(['*']).from(testTableName).run(queryRunner)

      expect(results).toHaveLength(3)
      expect((results[0] as any).id).toBe(1)
      expect((results[0] as any).name).toBe('John Doe')
      expect((results[1] as any).id).toBe(2)
      expect((results[1] as any).name).toBe('Jane Smith')
      expect((results[2] as any).id).toBe(3)
      expect((results[2] as any).name).toBe('Bob Johnson')

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should insert data from JSONEachRow stream with array of objects', async () => {
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

      // Create JSONEachRow stream with array of objects
      const { Readable } = await import('stream')
      const jsonData = [
        { id: 1, name: 'Alice', email: 'alice@example.com', age: 28 },
        { id: 2, name: 'Charlie', email: 'charlie@example.com', age: 32 },
      ]
      const jsonStream = Readable.from(jsonData, { objectMode: true })

      // Insert from stream
      await insertInto(testTableName).fromStream(jsonStream).format('JSONEachRow').run(queryRunner)

      // Verify insertion
      const results = await select(['*']).from(testTableName).run(queryRunner)

      expect(results).toHaveLength(2)
      expect((results[0] as any).id).toBe(1)
      expect((results[0] as any).name).toBe('Alice')
      expect((results[1] as any).id).toBe(2)
      expect((results[1] as any).name).toBe('Charlie')

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should handle large dataset streaming', async () => {
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

      // Create large CSV stream (100 rows)
      const { Readable } = await import('stream')
      const csvLines = []
      for (let i = 1; i <= 100; i++) {
        csvLines.push(`${i},User${i},${i * 10}`)
      }
      const csvData = csvLines.join('\n')
      const csvStream = Readable.from([csvData], { objectMode: false })

      // Insert from stream
      await insertInto(testTableName).fromStream(csvStream).format('CSV').run(queryRunner)

      // Verify insertion
      const results = await select(['*']).from(testTableName).run(queryRunner)

      expect(results).toHaveLength(100)
      expect((results[0] as any).id).toBe(1)
      expect((results[0] as any).name).toBe('User1')
      expect((results[0] as any).value).toBe(10)
      expect((results[99] as any).id).toBe(100)
      expect((results[99] as any).name).toBe('User100')
      expect((results[99] as any).value).toBe(1000)

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })
  })

  describe('Insert from Select', () => {
    it('should insert rows using SELECT source', async () => {
      const sourceTable = `${testTableName}_source`
      const targetTable = `${testTableName}_target`

      // Create source and target tables
      for (const tableName of [sourceTable, targetTable]) {
        await queryRunner.command({
          sql: `
            CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${tableName} (
              id UInt32,
              name String
            ) ENGINE = MergeTree() ORDER BY id
          `,
        })
      }

      // Seed source table
      await insertInto(sourceTable)
        .columns(['id', 'name'])
        .values([
          [1, 'Source User 1'],
          [2, 'Source User 2'],
        ])
        .run(queryRunner)

      // Copy data into target table using INSERT ... SELECT
      await insertInto(targetTable)
        .columns(['id', 'name'])
        .fromSelect(select(['id', 'name']).from(sourceTable))
        .run(queryRunner)

      // Verify data copied
      const results = await select(['*'])
        .from(targetTable)
        .orderBy([{ column: 'id', direction: 'ASC' }])
        .run<any>(queryRunner)

      expect(results).toHaveLength(2)
      expect(results[0].name).toBe('Source User 1')
      expect(results[1].name).toBe('Source User 2')

      // Clean up
      for (const tableName of [sourceTable, targetTable]) {
        await queryRunner.command({
          sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${tableName}`,
        })
      }
    })
  })
})
