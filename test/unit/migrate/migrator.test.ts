/**
 * Migrator Tests
 * Comprehensive tests for the Migrator class
 */

import { describe, it, expect, beforeEach, jest, afterEach } from '@jest/globals'
import { Migrator, MigratorOptions } from '../../../src/migrate/migrator'
import { SimpleMigration } from '../../../src/migrate/migration'
import * as fs from 'fs'

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

// Mock fs module
jest.mock('fs')
const mockFs = fs as jest.Mocked<typeof fs>

describe('Migrator', () => {
  let migrator: Migrator
  let options: MigratorOptions

  beforeEach(() => {
    jest.clearAllMocks()

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

    migrator = new Migrator(mockQueryRunner, options)
  })

  afterEach(() => {
    jest.restoreAllMocks()
  })

  describe('Constructor', () => {
    it('should initialize with default options', () => {
      const defaultMigrator = new Migrator(mockQueryRunner)

      expect(defaultMigrator).toBeInstanceOf(Migrator)
    })

    it('should initialize with custom options', () => {
      const customOptions: MigratorOptions = {
        migrationsTableName: 'custom_migrations',
        cluster: 'test_cluster',
        allowMutations: true,
        dryRun: true,
        migrationsPath: './custom-migrations',
        migrationsPattern: '*.sql',
        migrationClasses: [],
        tsMigrationsPath: './custom-migrations',
        tsMigrationsPattern: '*.ts',
      }

      const customMigrator = new Migrator(mockQueryRunner, customOptions)
      expect(customMigrator).toBeInstanceOf(Migrator)
    })
  })

  describe('init()', () => {
    it('should create migrations table and load migrations', async () => {
      mockQueryRunner.command.mockResolvedValue(undefined)
      mockFs.existsSync.mockReturnValue(true)
      mockFs.readdirSync.mockReturnValue(['001_create_users.sql', '002_create_posts.sql'] as any)
      mockFs.readFileSync.mockReturnValue(
        '-- Create users table\nCREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
      )

      await migrator.init()

      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: expect.stringContaining('CREATE TABLE IF NOT EXISTS test_migrations'),
      })
    })

    it('should handle missing migrations directory', async () => {
      mockQueryRunner.command.mockResolvedValue(undefined)
      mockFs.existsSync.mockReturnValue(false)

      await migrator.init()

      expect(mockQueryRunner.command).toHaveBeenCalledWith({
        sql: expect.stringContaining('CREATE TABLE IF NOT EXISTS test_migrations'),
      })
    })
  })

  describe('status()', () => {
    it('should return migration status', async () => {
      const mockStatus = [
        {
          id: '001_create_users',
          applied_at: '2024-01-01T00:00:00Z',
          checksum: 'abc123',
          cluster: null,
          status: 'applied',
        },
        {
          id: '002_create_posts',
          applied_at: null,
          checksum: 'def456',
          cluster: null,
          status: 'pending',
        },
      ]

      mockQueryRunner.execute.mockResolvedValue(mockStatus)

      const status = await migrator.status()

      expect(status).toHaveLength(2)
      expect(status[0]).toEqual({
        id: '001_create_users',
        appliedAt: new Date('2024-01-01T00:00:00Z'),
        checksum: 'abc123',
        cluster: null,
        status: 'applied',
      })
      expect(status[1]).toEqual({
        id: '002_create_posts',
        appliedAt: null,
        checksum: 'def456',
        cluster: null,
        status: 'pending',
      })

      expect(mockQueryRunner.execute).toHaveBeenCalledWith({
        sql: 'SELECT * FROM test_migrations ORDER BY id',
      })
    })
  })

  describe('plan()', () => {
    beforeEach(() => {
      // Mock the loadMigrations method
      jest.spyOn(migrator as any, 'loadMigrations').mockResolvedValue(undefined)
      jest.spyOn(migrator as any, 'getPendingMigrations').mockResolvedValue([])
    })

    it('should create migration plan for pending migrations', async () => {
      const mockMigration = {
        id: '001_create_users',
        description: 'Create users table',
        content: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
        checksum: 'abc123',
        up: [
          { type: 'up' as const, sql: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id' },
        ],
        down: [{ type: 'down' as const, sql: 'DROP TABLE users' }],
      }

      jest.spyOn(migrator as any, 'getPendingMigrations').mockResolvedValue([mockMigration])
      jest.spyOn(migrator as any, 'convertFileMigrationToSteps').mockReturnValue([
        {
          type: 'custom' as const,
          description: 'Execute 001_create_users',
          sql: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
          isReversible: true,
          reverseSql: 'DROP TABLE users',
        },
      ])

      const plan = await migrator.plan()

      expect(plan).toBeDefined()
      expect(plan.getMigrations()).toHaveLength(1)
    })

    it('should detect mutations and add warnings', async () => {
      const mockMigration = {
        id: '001_alter_users',
        description: 'Alter users table',
        content: 'ALTER TABLE users ADD COLUMN email String',
        checksum: 'abc123',
        up: [{ type: 'up' as const, sql: 'ALTER TABLE users ADD COLUMN email String' }],
        down: [{ type: 'down' as const, sql: 'ALTER TABLE users DROP COLUMN email' }],
      }

      jest.spyOn(migrator as any, 'getPendingMigrations').mockResolvedValue([mockMigration])
      jest.spyOn(migrator as any, 'convertFileMigrationToSteps').mockReturnValue([
        {
          type: 'custom' as const,
          description: 'Execute 001_alter_users',
          sql: 'ALTER TABLE users ADD COLUMN email String',
          isReversible: true,
          reverseSql: 'ALTER TABLE users DROP COLUMN email',
        },
      ])
      jest.spyOn(migrator as any, 'containsMutations').mockReturnValue(true)

      const plan = await migrator.plan()

      expect(plan.getWarnings()).toContain('Migration contains mutations - use --allow-mutations flag')
    })

    it('should detect TTL changes and add warnings', async () => {
      const mockMigration = {
        id: '001_add_ttl',
        description: 'Add TTL to users table',
        content: 'ALTER TABLE users MODIFY TTL created_at + INTERVAL 1 YEAR',
        checksum: 'abc123',
        up: [{ type: 'up' as const, sql: 'ALTER TABLE users MODIFY TTL created_at + INTERVAL 1 YEAR' }],
        down: [{ type: 'down' as const, sql: 'ALTER TABLE users REMOVE TTL' }],
      }

      jest.spyOn(migrator as any, 'getPendingMigrations').mockResolvedValue([mockMigration])
      jest.spyOn(migrator as any, 'convertFileMigrationToSteps').mockReturnValue([
        {
          type: 'custom' as const,
          description: 'Execute 001_add_ttl',
          sql: 'ALTER TABLE users MODIFY TTL created_at + INTERVAL 1 YEAR',
          isReversible: true,
          reverseSql: 'ALTER TABLE users REMOVE TTL',
        },
      ])
      jest.spyOn(migrator as any, 'containsTTLChanges').mockReturnValue(true)

      const plan = await migrator.plan()

      expect(plan.getWarnings()).toContain('Migration contains TTL changes - may cause rebuilds')
    })
  })

  describe('up()', () => {
    beforeEach(() => {
      jest.spyOn(migrator as any, 'getPendingMigrations').mockResolvedValue([])
    })

    it('should apply pending migrations', async () => {
      const mockMigration = {
        id: '001_create_users',
        description: 'Create users table',
        content: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
        checksum: 'abc123',
        up: [
          { type: 'up' as const, sql: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id' },
        ],
        down: [{ type: 'down' as const, sql: 'DROP TABLE users' }],
      }

      jest.spyOn(migrator as any, 'getPendingMigrations').mockResolvedValue([mockMigration])
      const applyMigrationSpy = jest.spyOn(migrator as any, 'applyMigration').mockResolvedValue(undefined)

      await migrator.up()

      expect(applyMigrationSpy).toHaveBeenCalledWith(mockMigration)
    })

    it('should handle empty pending migrations', async () => {
      jest.spyOn(migrator as any, 'getPendingMigrations').mockResolvedValue([])
      const applyMigrationSpy = jest.spyOn(migrator as any, 'applyMigration').mockResolvedValue(undefined)

      await migrator.up()

      expect(applyMigrationSpy).not.toHaveBeenCalled()
    })
  })

  describe('down()', () => {
    beforeEach(() => {
      jest.spyOn(migrator as any, 'getAppliedMigrations').mockResolvedValue([])
    })

    it('should rollback specified number of migrations', async () => {
      const mockMigrations = [
        { id: '001_create_users', description: 'Create users table' },
        { id: '002_create_posts', description: 'Create posts table' },
      ]

      jest.spyOn(migrator as any, 'getAppliedMigrations').mockResolvedValue(mockMigrations)
      const rollbackMigrationSpy = jest.spyOn(migrator as any, 'rollbackMigration').mockResolvedValue(undefined)

      await migrator.down(2)

      expect(rollbackMigrationSpy).toHaveBeenCalledTimes(2)
    })

    it('should rollback single migration by default', async () => {
      const mockMigration = { id: '001_create_users', description: 'Create users table' }

      jest.spyOn(migrator as any, 'getAppliedMigrations').mockResolvedValue([mockMigration])
      const rollbackMigrationSpy = jest.spyOn(migrator as any, 'rollbackMigration').mockResolvedValue(undefined)

      await migrator.down()

      expect(rollbackMigrationSpy).toHaveBeenCalledTimes(1)
    })
  })

  describe('detectDrift()', () => {
    it('should detect schema drift', async () => {
      const mockDriftResult = {
        hasDrift: true,
        differences: [
          {
            type: 'table_missing' as const,
            table: 'users',
            expected: { name: 'users', columns: [] },
          },
        ],
        repairSQL: ['CREATE TABLE users (...)'],
      }

      const mockDriftDetection = {
        detect: jest.fn(() => Promise.resolve(mockDriftResult)),
      }
      Object.defineProperty(migrator, 'driftDetection', {
        value: mockDriftDetection,
        writable: true,
      })

      const drift = await migrator.detectDrift()

      expect(drift).toEqual(mockDriftResult)
    })
  })

  describe('repairDrift()', () => {
    it('should repair schema drift when drift is detected', async () => {
      const mockDriftResult = {
        hasDrift: true,
        differences: [
          {
            type: 'table_missing' as const,
            table: 'users',
            expected: { name: 'users', columns: [] },
          },
        ],
        repairSQL: ['CREATE TABLE users (...)'],
      }

      const mockDriftDetection = {
        detect: jest.fn(() => Promise.resolve(mockDriftResult)),
        repair: jest.fn(() => Promise.resolve(undefined)),
      }
      Object.defineProperty(migrator, 'driftDetection', {
        value: mockDriftDetection,
        writable: true,
      })

      await migrator.repairDrift()

      expect(mockDriftDetection.repair).toHaveBeenCalledWith(mockDriftResult)
    })

    it('should not repair when no drift is detected', async () => {
      const mockDriftResult = {
        hasDrift: false,
        differences: [],
        repairSQL: [],
      }

      const mockDriftDetection = {
        detect: jest.fn(() => Promise.resolve(mockDriftResult)),
        repair: jest.fn(() => Promise.resolve(undefined)),
      }
      Object.defineProperty(migrator, 'driftDetection', {
        value: mockDriftDetection,
        writable: true,
      })

      await migrator.repairDrift()

      expect(mockDriftDetection.repair).not.toHaveBeenCalled()
    })
  })

  describe('seed()', () => {
    it('should execute seeder function', async () => {
      const mockSeeder = jest.fn(() => Promise.resolve())

      await migrator.seed(mockSeeder)

      expect(mockSeeder).toHaveBeenCalledTimes(1)
    })

    it('should handle seeder errors', async () => {
      const mockSeeder = jest.fn(() => Promise.reject(new Error('Seeder failed')))

      await expect(migrator.seed(mockSeeder)).rejects.toThrow('Seeder failed')
    })
  })

  describe('Class-based migrations', () => {
    it('should handle class migrations in plan', async () => {
      const mockMigration = new SimpleMigration(
        '001_create_users',
        'Create users table',
        'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
        'DROP TABLE users',
      )

      jest.spyOn(migrator as any, 'loadMigrations').mockResolvedValue(undefined)
      jest.spyOn(migrator as any, 'getPendingMigrations').mockResolvedValue([mockMigration])
      jest.spyOn(mockMigration, 'plan').mockResolvedValue([
        {
          type: 'create_table' as const,
          description: 'Create users table',
          sql: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
          isReversible: true,
          reverseSql: 'DROP TABLE users',
        },
      ])
      jest.spyOn(mockMigration, 'isSafeToApply').mockResolvedValue({ safe: true, warnings: [] })

      const plan = await migrator.plan()

      expect(plan.getMigrations()).toHaveLength(1)
      expect(mockMigration.plan).toHaveBeenCalledWith(mockQueryRunner, options)
    })

    it('should handle class migration safety checks', async () => {
      const mockMigration = new SimpleMigration(
        '001_alter_users',
        'Alter users table',
        'ALTER TABLE users ADD COLUMN email String',
        'ALTER TABLE users DROP COLUMN email',
      )

      jest.spyOn(migrator as any, 'loadMigrations').mockResolvedValue(undefined)
      jest.spyOn(migrator as any, 'getPendingMigrations').mockResolvedValue([mockMigration])
      jest.spyOn(mockMigration, 'plan').mockResolvedValue([])
      jest.spyOn(mockMigration, 'isSafeToApply').mockResolvedValue({
        safe: false,
        warnings: ['Migration contains mutations - use --allow-mutations flag'],
      })

      const plan = await migrator.plan()

      expect(plan.getWarnings()).toContain('Migration contains mutations - use --allow-mutations flag')
    })
  })

  describe('Error handling', () => {
    it('should handle migration application errors', async () => {
      const mockMigration = {
        id: '001_create_users',
        description: 'Create users table',
        content: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
        checksum: 'abc123',
        up: [
          { type: 'up' as const, sql: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id' },
        ],
        down: [{ type: 'down' as const, sql: 'DROP TABLE users' }],
      }

      jest.spyOn(migrator as any, 'getPendingMigrations').mockResolvedValue([mockMigration])
      jest.spyOn(migrator as any, 'applyMigration').mockRejectedValue(new Error('Migration failed'))

      await expect(migrator.up()).rejects.toThrow('Migration failed')
    })

    it('should handle rollback errors', async () => {
      const mockMigration = { id: '001_create_users', description: 'Create users table' }

      jest.spyOn(migrator as any, 'getAppliedMigrations').mockResolvedValue([mockMigration])
      jest.spyOn(migrator as any, 'rollbackMigration').mockRejectedValue(new Error('Rollback failed'))

      await expect(migrator.down()).rejects.toThrow('Rollback failed')
    })
  })
})
