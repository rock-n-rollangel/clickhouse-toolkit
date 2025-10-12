/**
 * DriftDetection Tests
 * Comprehensive tests for the DriftDetection class
 */

import { describe, it, expect, beforeEach, jest } from '@jest/globals'
import { DriftDetection, DriftResult } from '../../../src/migrate/drift-detection'

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

describe('DriftDetection', () => {
  let driftDetection: DriftDetection

  beforeEach(() => {
    jest.clearAllMocks()
    driftDetection = new DriftDetection(mockQueryRunner)
  })

  describe('detect()', () => {
    it('should detect no drift when schemas match', async () => {
      const currentSchema = {
        tables: [
          {
            name: 'users',
            columns: [
              { name: 'id', type: 'UInt32' },
              { name: 'name', type: 'String' },
            ],
          },
        ],
      }

      const expectedSchema = {
        tables: [
          {
            name: 'users',
            columns: [
              { name: 'id', type: 'UInt32' },
              { name: 'name', type: 'String' },
            ],
          },
        ],
      }

      jest.spyOn(driftDetection as any, 'getCurrentSchema').mockResolvedValue(currentSchema)
      jest.spyOn(driftDetection as any, 'getExpectedSchema').mockResolvedValue(expectedSchema)

      const result = await driftDetection.detect()

      expect(result.hasDrift).toBe(false)
      expect(result.differences).toEqual([])
      expect(result.repairSQL).toEqual([])
    })

    it('should detect missing table', async () => {
      const currentSchema = {
        tables: [],
      }

      const expectedSchema = {
        tables: [
          {
            name: 'users',
            columns: [
              { name: 'id', type: 'UInt32' },
              { name: 'name', type: 'String' },
            ],
          },
        ],
      }

      jest.spyOn(driftDetection as any, 'getCurrentSchema').mockResolvedValue(currentSchema)
      jest.spyOn(driftDetection as any, 'getExpectedSchema').mockResolvedValue(expectedSchema)

      const result = await driftDetection.detect()

      expect(result.hasDrift).toBe(true)
      expect(result.differences).toHaveLength(1)
      expect(result.differences[0]).toEqual({
        type: 'table_missing',
        table: 'users',
        expected: expectedSchema.tables[0],
      })
      expect(result.repairSQL).toContain('CREATE TABLE users (...)')
    })

    it('should detect extra table', async () => {
      const currentSchema = {
        tables: [
          {
            name: 'users',
            columns: [
              { name: 'id', type: 'UInt32' },
              { name: 'name', type: 'String' },
            ],
          },
          {
            name: 'extra_table',
            columns: [{ name: 'id', type: 'UInt32' }],
          },
        ],
      }

      const expectedSchema = {
        tables: [
          {
            name: 'users',
            columns: [
              { name: 'id', type: 'UInt32' },
              { name: 'name', type: 'String' },
            ],
          },
        ],
      }

      jest.spyOn(driftDetection as any, 'getCurrentSchema').mockResolvedValue(currentSchema)
      jest.spyOn(driftDetection as any, 'getExpectedSchema').mockResolvedValue(expectedSchema)

      const result = await driftDetection.detect()

      expect(result.hasDrift).toBe(true)
      expect(result.differences).toHaveLength(1)
      expect(result.differences[0]).toEqual({
        type: 'table_extra',
        table: 'extra_table',
        actual: currentSchema.tables[1],
      })
    })

    it('should detect missing column', async () => {
      const currentSchema = {
        tables: [
          {
            name: 'users',
            columns: [{ name: 'id', type: 'UInt32' }],
          },
        ],
      }

      const expectedSchema = {
        tables: [
          {
            name: 'users',
            columns: [
              { name: 'id', type: 'UInt32' },
              { name: 'name', type: 'String' },
            ],
          },
        ],
      }

      jest.spyOn(driftDetection as any, 'getCurrentSchema').mockResolvedValue(currentSchema)
      jest.spyOn(driftDetection as any, 'getExpectedSchema').mockResolvedValue(expectedSchema)

      const result = await driftDetection.detect()

      expect(result.hasDrift).toBe(true)
      expect(result.differences).toHaveLength(1)
      expect(result.differences[0]).toEqual({
        type: 'column_missing',
        table: 'users',
        column: 'name',
        expected: { name: 'name', type: 'String' },
      })
      expect(result.repairSQL).toContain('ALTER TABLE users ADD COLUMN name String')
    })

    it('should detect extra column', async () => {
      const currentSchema = {
        tables: [
          {
            name: 'users',
            columns: [
              { name: 'id', type: 'UInt32' },
              { name: 'name', type: 'String' },
              { name: 'extra_column', type: 'String' },
            ],
          },
        ],
      }

      const expectedSchema = {
        tables: [
          {
            name: 'users',
            columns: [
              { name: 'id', type: 'UInt32' },
              { name: 'name', type: 'String' },
            ],
          },
        ],
      }

      jest.spyOn(driftDetection as any, 'getCurrentSchema').mockResolvedValue(currentSchema)
      jest.spyOn(driftDetection as any, 'getExpectedSchema').mockResolvedValue(expectedSchema)

      const result = await driftDetection.detect()

      expect(result.hasDrift).toBe(true)
      expect(result.differences).toHaveLength(1)
      expect(result.differences[0]).toEqual({
        type: 'column_extra',
        table: 'users',
        column: 'extra_column',
        actual: { name: 'extra_column', type: 'String' },
      })
    })

    it('should detect column type mismatch', async () => {
      const currentSchema = {
        tables: [
          {
            name: 'users',
            columns: [
              { name: 'id', type: 'UInt32' },
              { name: 'name', type: 'UInt32' }, // Wrong type
            ],
          },
        ],
      }

      const expectedSchema = {
        tables: [
          {
            name: 'users',
            columns: [
              { name: 'id', type: 'UInt32' },
              { name: 'name', type: 'String' }, // Expected type
            ],
          },
        ],
      }

      jest.spyOn(driftDetection as any, 'getCurrentSchema').mockResolvedValue(currentSchema)
      jest.spyOn(driftDetection as any, 'getExpectedSchema').mockResolvedValue(expectedSchema)

      const result = await driftDetection.detect()

      expect(result.hasDrift).toBe(true)
      expect(result.differences).toHaveLength(1)
      expect(result.differences[0]).toEqual({
        type: 'column_type_mismatch',
        table: 'users',
        column: 'name',
        expected: 'String',
        actual: 'UInt32',
      })
      expect(result.repairSQL).toContain('ALTER TABLE users MODIFY COLUMN name String')
    })

    it('should detect multiple drift issues', async () => {
      const currentSchema = {
        tables: [
          {
            name: 'users',
            columns: [
              { name: 'id', type: 'UInt32' },
              { name: 'name', type: 'UInt32' }, // Wrong type
            ],
          },
          {
            name: 'extra_table',
            columns: [{ name: 'id', type: 'UInt32' }],
          },
        ],
      }

      const expectedSchema = {
        tables: [
          {
            name: 'users',
            columns: [
              { name: 'id', type: 'UInt32' },
              { name: 'name', type: 'String' },
              { name: 'email', type: 'String' }, // Missing column
            ],
          },
          {
            name: 'posts',
            columns: [
              { name: 'id', type: 'UInt32' },
              { name: 'title', type: 'String' },
            ],
          },
        ],
      }

      jest.spyOn(driftDetection as any, 'getCurrentSchema').mockResolvedValue(currentSchema)
      jest.spyOn(driftDetection as any, 'getExpectedSchema').mockResolvedValue(expectedSchema)

      const result = await driftDetection.detect()

      expect(result.hasDrift).toBe(true)
      expect(result.differences).toHaveLength(4) // Missing table, extra table, missing column, type mismatch
      expect(result.repairSQL).toHaveLength(3)
    })
  })

  describe('repair()', () => {
    it('should not repair when no drift detected', async () => {
      const drift: DriftResult = {
        hasDrift: false,
        differences: [],
        repairSQL: [],
      }

      await driftDetection.repair(drift)

      expect(mockQueryRunner.command).not.toHaveBeenCalled()
    })

    it('should repair drift by executing repair SQL', async () => {
      const drift: DriftResult = {
        hasDrift: true,
        differences: [
          {
            type: 'table_missing',
            table: 'users',
            expected: { name: 'users', columns: [] },
          },
        ],
        repairSQL: ['CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id'],
      }

      mockQueryRunner.command.mockResolvedValue(undefined)

      await driftDetection.repair(drift)

      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
      })
    })

    it('should handle repair errors', async () => {
      const drift: DriftResult = {
        hasDrift: true,
        differences: [
          {
            type: 'table_missing',
            table: 'users',
            expected: { name: 'users', columns: [] },
          },
        ],
        repairSQL: ['CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id'],
      }

      const error = new Error('Repair failed')
      mockQueryRunner.command.mockRejectedValue(error)

      await expect(driftDetection.repair(drift)).rejects.toThrow('Repair failed')
    })

    it('should repair multiple drift issues', async () => {
      const drift: DriftResult = {
        hasDrift: true,
        differences: [
          {
            type: 'table_missing',
            table: 'users',
            expected: { name: 'users', columns: [] },
          },
          {
            type: 'column_missing',
            table: 'posts',
            column: 'title',
            expected: { name: 'title', type: 'String' },
          },
        ],
        repairSQL: [
          'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
          'ALTER TABLE posts ADD COLUMN title String',
        ],
      }

      mockQueryRunner.command.mockResolvedValue(undefined)

      await driftDetection.repair(drift)

      expect(mockQueryRunner.command).toHaveBeenCalledTimes(2)
      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
      })
      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: 'ALTER TABLE posts ADD COLUMN title String',
      })
    })
  })

  describe('getCurrentSchema()', () => {
    it('should get current schema from system tables', async () => {
      const mockColumns = [
        { table: 'users', name: 'id', type: 'UInt32' },
        { table: 'users', name: 'name', type: 'String' },
        { table: 'posts', name: 'id', type: 'UInt32' },
        { table: 'posts', name: 'title', type: 'String' },
      ]

      mockQueryRunner.execute.mockResolvedValue(mockColumns)

      const schema = await (driftDetection as any).getCurrentSchema()

      expect(schema.tables).toHaveLength(2)
      expect(schema.tables[0]).toEqual({
        name: 'users',
        columns: [
          { name: 'id', type: 'UInt32' },
          { name: 'name', type: 'String' },
        ],
      })
      expect(schema.tables[1]).toEqual({
        name: 'posts',
        columns: [
          { name: 'id', type: 'UInt32' },
          { name: 'title', type: 'String' },
        ],
      })

      expect(mockQueryRunner.execute).toHaveBeenCalledWith({
        sql: expect.stringContaining('FROM system.columns'),
      })
    })

    it('should handle empty schema', async () => {
      mockQueryRunner.execute.mockResolvedValue([])

      const schema = await (driftDetection as any).getCurrentSchema()

      expect(schema.tables).toEqual([])
    })
  })

  describe('getExpectedSchema()', () => {
    it('should return empty schema by default', async () => {
      const schema = await (driftDetection as any).getExpectedSchema()

      expect(schema.tables).toEqual([])
    })
  })

  describe('Error handling', () => {
    it('should handle errors in detect()', async () => {
      const error = new Error('Schema detection failed')
      jest.spyOn(driftDetection as any, 'getCurrentSchema').mockRejectedValue(error)

      await expect(driftDetection.detect()).rejects.toThrow('Schema detection failed')
    })

    it('should handle errors in repair()', async () => {
      const drift: DriftResult = {
        hasDrift: true,
        differences: [],
        repairSQL: ['INVALID SQL'],
      }

      const error = new Error('SQL execution failed')
      mockQueryRunner.command.mockRejectedValue(error)

      await expect(driftDetection.repair(drift)).rejects.toThrow('SQL execution failed')
    })
  })

  describe('Edge cases', () => {
    it('should handle tables with no columns', async () => {
      const currentSchema = {
        tables: [
          {
            name: 'empty_table',
            columns: [],
          },
        ],
      }

      const expectedSchema = {
        tables: [
          {
            name: 'empty_table',
            columns: [],
          },
        ],
      }

      jest.spyOn(driftDetection as any, 'getCurrentSchema').mockResolvedValue(currentSchema)
      jest.spyOn(driftDetection as any, 'getExpectedSchema').mockResolvedValue(expectedSchema)

      const result = await driftDetection.detect()

      expect(result.hasDrift).toBe(false)
    })

    it('should handle case sensitivity in column names', async () => {
      const currentSchema = {
        tables: [
          {
            name: 'users',
            columns: [
              { name: 'ID', type: 'UInt32' },
              { name: 'Name', type: 'String' },
            ],
          },
        ],
      }

      const expectedSchema = {
        tables: [
          {
            name: 'users',
            columns: [
              { name: 'id', type: 'UInt32' },
              { name: 'name', type: 'String' },
            ],
          },
        ],
      }

      jest.spyOn(driftDetection as any, 'getCurrentSchema').mockResolvedValue(currentSchema)
      jest.spyOn(driftDetection as any, 'getExpectedSchema').mockResolvedValue(expectedSchema)

      const result = await driftDetection.detect()

      expect(result.hasDrift).toBe(true)
      expect(result.differences).toHaveLength(4) // Both columns are considered missing
    })
  })
})
