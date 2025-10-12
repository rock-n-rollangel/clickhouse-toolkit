/**
 * Migration Tests
 * Comprehensive tests for the Migration base class and SimpleMigration
 */

import { describe, it, expect, beforeEach, jest } from '@jest/globals'
import { Migration, SimpleMigration, MigrationStep } from '../../../src/migrate/migration'
import { QueryRunner } from '../../../src/runner/query-runner'
import { MigratorOptions } from '../../../src/migrate/migrator'

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

// Test migration class
class TestMigration extends Migration {
  readonly id = 'test_migration'
  readonly description = 'Test migration'

  async up(runner: QueryRunner, options: MigratorOptions): Promise<void> {
    await this.executeSQL(runner, 'CREATE TABLE test (id UInt32) ENGINE = MergeTree() ORDER BY id', options)
  }

  async down(runner: QueryRunner, options: MigratorOptions): Promise<void> {
    await this.executeSQL(runner, 'DROP TABLE test', options)
  }

  protected containsMutations(): boolean {
    return true
  }

  protected containsTTLChanges(): boolean {
    return true
  }
}

describe('Migration', () => {
  let migration: TestMigration
  let options: MigratorOptions

  beforeEach(() => {
    jest.clearAllMocks()

    migration = new TestMigration()
    options = {
      migrationsTableName: 'test_migrations',
      cluster: null,
      allowMutations: false,
      dryRun: false,
      migrationsPath: './test-migrations',
      migrationsPattern: '*.sql',
      migrationClasses: [],
      tsMigrationsPath: './test-migrations',
      tsMigrationsPattern: '*.ts',
    }
  })

  describe('Basic properties', () => {
    it('should have correct id and description', () => {
      expect(migration.id).toBe('test_migration')
      expect(migration.description).toBe('Test migration')
    })
  })

  describe('up()', () => {
    it('should execute up migration', async () => {
      mockQueryRunner.command.mockResolvedValue(undefined)

      await migration.up(mockQueryRunner, options)

      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: 'CREATE TABLE test (id UInt32) ENGINE = MergeTree() ORDER BY id',
      })
    })

    it('should handle cluster option in SQL', async () => {
      const clusterOptions = { ...options, cluster: 'test_cluster' }
      mockQueryRunner.command.mockResolvedValue(undefined)

      await migration.up(mockQueryRunner, clusterOptions)

      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: 'CREATE TABLE ON CLUSTER test_cluster test (id UInt32) ENGINE = MergeTree() ORDER BY id',
      })
    })
  })

  describe('down()', () => {
    it('should execute down migration', async () => {
      mockQueryRunner.command.mockResolvedValue(undefined)

      await migration.down(mockQueryRunner, options)

      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: 'DROP TABLE test',
      })
    })

    it('should handle cluster option in SQL', async () => {
      const clusterOptions = { ...options, cluster: 'test_cluster' }
      mockQueryRunner.command.mockResolvedValue(undefined)

      await migration.down(mockQueryRunner, clusterOptions)

      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: 'DROP TABLE ON CLUSTER test_cluster test',
      })
    })
  })

  describe('plan()', () => {
    it('should return empty array by default', async () => {
      const steps = await migration.plan(mockQueryRunner, options)

      expect(steps).toEqual([])
    })
  })

  describe('calculateChecksum()', () => {
    it('should calculate consistent checksum', async () => {
      const checksum1 = await migration.calculateChecksum()
      const checksum2 = await migration.calculateChecksum()

      expect(checksum1).toBe(checksum2)
      expect(checksum1).toMatch(/^[a-f0-9]{64}$/) // SHA256 hex
    })

    it('should calculate different checksums for different migrations', async () => {
      class DifferentMigration extends Migration {
        readonly id = 'different_migration'
        readonly description = 'Different migration'

        async up(runner: QueryRunner, options: MigratorOptions): Promise<void> {
          await this.executeSQL(runner, 'CREATE TABLE different (id UInt32) ENGINE = MergeTree() ORDER BY id', options)
        }

        async down(runner: QueryRunner, options: MigratorOptions): Promise<void> {
          await this.executeSQL(runner, 'DROP TABLE different', options)
        }
      }
      const migration2 = new DifferentMigration()

      const checksum1 = await migration.calculateChecksum()
      const checksum2 = await migration2.calculateChecksum()

      expect(checksum1).not.toBe(checksum2)
    })
  })

  describe('isSafeToApply()', () => {
    it('should return safe when no mutations or TTL changes', async () => {
      class SafeMigration extends TestMigration {
        protected containsMutations(): boolean {
          return false
        }
        protected containsTTLChanges(): boolean {
          return false
        }
      }
      const safeMigration = new SafeMigration()

      const result = await safeMigration.isSafeToApply(mockQueryRunner, options)

      expect(result.safe).toBe(true)
      expect(result.warnings).toEqual([])
    })

    it('should return unsafe when mutations are detected and not allowed', async () => {
      const result = await migration.isSafeToApply(mockQueryRunner, options)

      expect(result.safe).toBe(false)
      expect(result.warnings).toContain('Migration contains mutations - use --allow-mutations flag')
      expect(result.warnings).toContain('Migration contains TTL changes - may cause rebuilds')
    })

    it('should return safe when mutations are allowed', async () => {
      const allowMutationsOptions = { ...options, allowMutations: true }

      const result = await migration.isSafeToApply(mockQueryRunner, allowMutationsOptions)

      expect(result.safe).toBe(false) // Still unsafe due to TTL changes
      expect(result.warnings).toContain('Migration contains TTL changes - may cause rebuilds')
      expect(result.warnings).not.toContain('Migration contains mutations - use --allow-mutations flag')
    })
  })

  describe('Error handling', () => {
    it('should propagate errors from executeSQL', async () => {
      const error = new Error('SQL execution failed')
      mockQueryRunner.command.mockRejectedValue(error)

      await expect(migration.up(mockQueryRunner, options)).rejects.toThrow('SQL execution failed')
    })
  })
})

describe('SimpleMigration', () => {
  let migration: SimpleMigration
  let options: MigratorOptions

  beforeEach(() => {
    jest.clearAllMocks()

    migration = new SimpleMigration(
      '001_create_users',
      'Create users table',
      'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
      'DROP TABLE users',
    )

    options = {
      migrationsTableName: 'test_migrations',
      cluster: null,
      allowMutations: false,
      dryRun: false,
      migrationsPath: './test-migrations',
      migrationsPattern: '*.sql',
      migrationClasses: [],
      tsMigrationsPath: './test-migrations',
      tsMigrationsPattern: '*.ts',
    }
  })

  describe('Constructor', () => {
    it('should initialize with provided values', () => {
      expect(migration.id).toBe('001_create_users')
      expect(migration.description).toBe('Create users table')
    })
  })

  describe('up()', () => {
    it('should execute up SQL', async () => {
      mockQueryRunner.command.mockResolvedValue(undefined)

      await migration.up(mockQueryRunner, options)

      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
      })
    })

    it('should handle cluster option', async () => {
      const clusterOptions = { ...options, cluster: 'test_cluster' }
      mockQueryRunner.command.mockResolvedValue(undefined)

      await migration.up(mockQueryRunner, clusterOptions)

      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: 'CREATE TABLE ON CLUSTER test_cluster users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
      })
    })
  })

  describe('down()', () => {
    it('should execute down SQL', async () => {
      mockQueryRunner.command.mockResolvedValue(undefined)

      await migration.down(mockQueryRunner, options)

      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: 'DROP TABLE users',
      })
    })

    it('should handle cluster option', async () => {
      const clusterOptions = { ...options, cluster: 'test_cluster' }
      mockQueryRunner.command.mockResolvedValue(undefined)

      await migration.down(mockQueryRunner, clusterOptions)

      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: 'DROP TABLE ON CLUSTER test_cluster users',
      })
    })
  })

  describe('calculateChecksum()', () => {
    it('should calculate checksum based on migration content', async () => {
      const checksum = await migration.calculateChecksum()

      expect(checksum).toMatch(/^[a-f0-9]{64}$/) // SHA256 hex
    })

    it('should produce different checksums for different content', async () => {
      const migration2 = new SimpleMigration(
        '002_create_posts',
        'Create posts table',
        'CREATE TABLE posts (id UInt32, title String) ENGINE = MergeTree() ORDER BY id',
        'DROP TABLE posts',
      )

      const checksum1 = await migration.calculateChecksum()
      const checksum2 = await migration2.calculateChecksum()

      expect(checksum1).not.toBe(checksum2)
    })
  })

  describe('getMigrationContent()', () => {
    it('should return content for checksum calculation', () => {
      // Access protected method through a test subclass
      class TestSimpleMigration extends SimpleMigration {
        public getMigrationContentPublic(): string {
          return this.getMigrationContent()
        }
      }

      const testMigration = new TestSimpleMigration(
        '001_create_users',
        'Create users table',
        'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
        'DROP TABLE users',
      )

      const content = testMigration.getMigrationContentPublic()

      expect(content).toContain('001_create_users')
      expect(content).toContain('Create users table')
      expect(content).toContain('CREATE TABLE users')
      expect(content).toContain('DROP TABLE users')
    })
  })

  describe('Error handling', () => {
    it('should propagate errors from up migration', async () => {
      const error = new Error('Up migration failed')
      mockQueryRunner.command.mockRejectedValue(error)

      await expect(migration.up(mockQueryRunner, options)).rejects.toThrow('Up migration failed')
    })

    it('should propagate errors from down migration', async () => {
      const error = new Error('Down migration failed')
      mockQueryRunner.command.mockRejectedValue(error)

      await expect(migration.down(mockQueryRunner, options)).rejects.toThrow('Down migration failed')
    })
  })
})

describe('MigrationStep', () => {
  it('should define correct step types', () => {
    const step: MigrationStep = {
      type: 'create_table',
      description: 'Create users table',
      sql: 'CREATE TABLE users (id UInt32) ENGINE = MergeTree() ORDER BY id',
      isReversible: true,
      reverseSql: 'DROP TABLE users',
    }

    expect(step.type).toBe('create_table')
    expect(step.description).toBe('Create users table')
    expect(step.sql).toBe('CREATE TABLE users (id UInt32) ENGINE = MergeTree() ORDER BY id')
    expect(step.isReversible).toBe(true)
    expect(step.reverseSql).toBe('DROP TABLE users')
  })

  it('should support all step types', () => {
    const stepTypes: MigrationStep['type'][] = [
      'create_table',
      'drop_table',
      'alter_table',
      'create_view',
      'drop_view',
      'insert_data',
      'custom',
    ]

    stepTypes.forEach((type) => {
      const step: MigrationStep = {
        type,
        description: 'Test step',
        sql: 'SELECT 1',
        isReversible: false,
      }

      expect(step.type).toBe(type)
    })
  })
})
