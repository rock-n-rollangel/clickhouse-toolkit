/**
 * MigrationPlan Tests
 * Comprehensive tests for the MigrationPlan class
 */

import { describe, it, expect, beforeEach, jest } from '@jest/globals'
import { MigrationPlan } from '../../../src/migrate/migration-plan'
import { Migration, MigrationStep } from '../../../src/migrate/migration'
import { SimpleMigration } from '../../../src/migrate/migration'
import { FileMigration } from '../../../src/migrate/migration'

// Mock migration classes
class TestMigration extends Migration {
  readonly id = 'test_migration'
  readonly description = 'Test migration'

  async up(): Promise<void> {}
  async down(): Promise<void> {}
}

describe('MigrationPlan', () => {
  let plan: MigrationPlan
  let migration: TestMigration
  let fileMigration: FileMigration
  let steps: MigrationStep[]

  beforeEach(() => {
    plan = new MigrationPlan()

    migration = new TestMigration()

    fileMigration = {
      id: '001_create_users',
      filename: '001_create_users.sql',
      content: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
      checksum: 'abc123',
      steps: [
        { type: 'up', sql: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id' },
        { type: 'down', sql: 'DROP TABLE users' },
      ],
      up: [{ type: 'up', sql: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id' }],
      down: [{ type: 'down', sql: 'DROP TABLE users' }],
      createdAt: new Date(),
      description: 'Create users table',
    }

    steps = [
      {
        type: 'create_table',
        description: 'Create users table',
        sql: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
        isReversible: true,
        reverseSql: 'DROP TABLE users',
      },
      {
        type: 'insert_data',
        description: 'Insert initial data',
        sql: "INSERT INTO users VALUES (1, 'John')",
        isReversible: false,
      },
    ]
  })

  describe('addMigration()', () => {
    it('should add migration with steps and warnings', () => {
      const warnings = ['Migration contains mutations']

      plan.addMigration(migration, steps, warnings)

      const migrations = plan.getMigrations()
      expect(migrations).toHaveLength(1)
      expect(migrations[0].migration).toBe(migration)
      expect(migrations[0].steps).toBe(steps)
      expect(migrations[0].warnings).toBe(warnings)
    })

    it('should add migration without warnings', () => {
      plan.addMigration(migration, steps)

      const migrations = plan.getMigrations()
      expect(migrations).toHaveLength(1)
      expect(migrations[0].warnings).toEqual([])
    })

    it('should add multiple migrations', () => {
      class TestMigration2 extends Migration {
        readonly id = 'test_migration_2'
        readonly description = 'Test migration 2'

        async up(): Promise<void> {}
        async down(): Promise<void> {}
      }
      const migration2 = new TestMigration2()
      const steps2: MigrationStep[] = [
        {
          type: 'drop_table',
          description: 'Drop test table',
          sql: 'DROP TABLE test',
          isReversible: false,
        },
      ]

      plan.addMigration(migration, steps, ['Warning 1'])
      plan.addMigration(migration2, steps2, ['Warning 2'])

      const migrations = plan.getMigrations()
      expect(migrations).toHaveLength(2)
    })
  })

  describe('getMigrations()', () => {
    it('should return empty array initially', () => {
      const migrations = plan.getMigrations()
      expect(migrations).toEqual([])
    })

    it('should return added migrations', () => {
      plan.addMigration(migration, steps, ['Test warning'])

      const migrations = plan.getMigrations()
      expect(migrations).toHaveLength(1)
      expect(migrations[0].migration).toBe(migration)
    })
  })

  describe('getTotalSteps()', () => {
    it('should return zero for empty plan', () => {
      expect(plan.getTotalSteps()).toBe(0)
    })

    it('should return total number of steps', () => {
      plan.addMigration(migration, steps)
      plan.addMigration(fileMigration, [steps[0]])

      expect(plan.getTotalSteps()).toBe(3) // 2 + 1
    })

    it('should handle migrations with no steps', () => {
      plan.addMigration(migration, [])
      plan.addMigration(fileMigration, steps)

      expect(plan.getTotalSteps()).toBe(2)
    })
  })

  describe('getWarnings()', () => {
    it('should return empty array when no warnings', () => {
      plan.addMigration(migration, steps)
      plan.addMigration(fileMigration, [steps[0]])

      expect(plan.getWarnings()).toEqual([])
    })

    it('should return all warnings from all migrations', () => {
      plan.addMigration(migration, steps, ['Warning 1', 'Warning 2'])
      plan.addMigration(fileMigration, [steps[0]], ['Warning 3'])

      const warnings = plan.getWarnings()
      expect(warnings).toHaveLength(3)
      expect(warnings).toContain('Warning 1')
      expect(warnings).toContain('Warning 2')
      expect(warnings).toContain('Warning 3')
    })
  })

  describe('hasWarnings()', () => {
    it('should return false when no warnings', () => {
      plan.addMigration(migration, steps)
      expect(plan.hasWarnings()).toBe(false)
    })

    it('should return true when warnings exist', () => {
      plan.addMigration(migration, steps, ['Warning 1'])
      expect(plan.hasWarnings()).toBe(true)
    })
  })

  describe('getSummary()', () => {
    it('should return empty summary for empty plan', () => {
      const summary = plan.getSummary()

      expect(summary.totalMigrations).toBe(0)
      expect(summary.totalSteps).toBe(0)
      expect(summary.warnings).toEqual([])
      expect(summary.migrations).toEqual([])
    })

    it('should return correct summary for single migration', () => {
      plan.addMigration(migration, steps, ['Warning 1'])

      const summary = plan.getSummary()

      expect(summary.totalMigrations).toBe(1)
      expect(summary.totalSteps).toBe(2)
      expect(summary.warnings).toEqual(['Warning 1'])
      expect(summary.migrations).toHaveLength(1)
      expect(summary.migrations[0]).toEqual({
        id: 'test_migration',
        description: 'Test migration',
        steps: 2,
        warnings: ['Warning 1'],
      })
    })

    it('should return correct summary for multiple migrations', () => {
      class TestMigration2 extends Migration {
        readonly id = 'test_migration_2'
        readonly description = 'Test migration 2'

        async up(): Promise<void> {}
        async down(): Promise<void> {}
      }
      const migration2 = new TestMigration2()

      plan.addMigration(migration, steps, ['Warning 1'])
      plan.addMigration(migration2, [steps[0]], ['Warning 2', 'Warning 3'])

      const summary = plan.getSummary()

      expect(summary.totalMigrations).toBe(2)
      expect(summary.totalSteps).toBe(3)
      expect(summary.warnings).toEqual(['Warning 1', 'Warning 2', 'Warning 3'])
      expect(summary.migrations).toHaveLength(2)
    })
  })

  describe('print()', () => {
    let consoleInfoSpy: jest.MockedFunction<any>
    let consoleWarnSpy: jest.MockedFunction<any>

    beforeEach(() => {
      consoleInfoSpy = jest.spyOn(console, 'info').mockImplementation(() => {})
      consoleWarnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {})
    })

    afterEach(() => {
      consoleInfoSpy.mockRestore()
      consoleWarnSpy.mockRestore()
    })

    it('should print empty plan', () => {
      plan.print()

      expect(consoleInfoSpy).toHaveBeenCalledWith(expect.stringContaining('Migration Plan'))
      expect(consoleInfoSpy).toHaveBeenCalledWith(expect.stringContaining('Total migrations: 0'))
      expect(consoleInfoSpy).toHaveBeenCalledWith(expect.stringContaining('Total steps: 0'))
    })

    it('should print plan with migrations', () => {
      plan.addMigration(migration, steps, ['Warning 1'])

      plan.print()

      expect(consoleInfoSpy).toHaveBeenCalledWith(expect.stringContaining('Total migrations: 1'))
      expect(consoleInfoSpy).toHaveBeenCalledWith(expect.stringContaining('Total steps: 2'))
      expect(consoleWarnSpy).toHaveBeenCalledWith(expect.stringContaining('Warnings:'))
      expect(consoleWarnSpy).toHaveBeenCalledWith(expect.stringContaining('Warning 1'))
      expect(consoleInfoSpy).toHaveBeenCalledWith(expect.stringContaining('test_migration: Test migration (2 steps)'))
    })

    it('should print plan without warnings section when no warnings', () => {
      plan.addMigration(migration, steps)

      plan.print()

      expect(consoleInfoSpy).toHaveBeenCalledWith(expect.stringContaining('Total migrations: 1'))
      expect(consoleWarnSpy).not.toHaveBeenCalledWith(expect.stringContaining('Warnings:'))
    })

    it('should print warnings for individual migrations', () => {
      class TestMigration2 extends Migration {
        readonly id = 'test_migration_2'
        readonly description = 'Test migration 2'

        async up(): Promise<void> {}
        async down(): Promise<void> {}
      }
      const migration2 = new TestMigration2()

      plan.addMigration(migration, steps, ['Warning 1'])
      plan.addMigration(migration2, [steps[0]], ['Warning 2'])

      plan.print()

      expect(consoleInfoSpy).toHaveBeenCalledWith(expect.stringContaining('test_migration: Test migration (2 steps)'))
      expect(consoleWarnSpy).toHaveBeenCalledWith(expect.stringContaining('Warning 1'))
      expect(consoleInfoSpy).toHaveBeenCalledWith(
        expect.stringContaining('test_migration_2: Test migration 2 (1 steps)'),
      )
      expect(consoleWarnSpy).toHaveBeenCalledWith(expect.stringContaining('Warning 2'))
    })
  })

  describe('Edge cases', () => {
    it('should handle migrations with undefined steps', () => {
      plan.addMigration(migration, undefined as any)

      expect(plan.getTotalSteps()).toBe(0)
      expect(plan.getSummary().migrations[0].steps).toBe(0)
    })

    it('should handle migrations with null steps', () => {
      plan.addMigration(migration, null as any)

      expect(plan.getTotalSteps()).toBe(0)
      expect(plan.getSummary().migrations[0].steps).toBe(0)
    })

    it('should handle empty steps array', () => {
      plan.addMigration(migration, [])

      expect(plan.getTotalSteps()).toBe(0)
      expect(plan.getSummary().migrations[0].steps).toBe(0)
    })
  })

  describe('Real migration examples', () => {
    it('should handle SimpleMigration', () => {
      const simpleMigration = new SimpleMigration(
        '001_create_users',
        'Create users table',
        'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
        'DROP TABLE users',
      )

      const steps: MigrationStep[] = [
        {
          type: 'create_table',
          description: 'Create users table',
          sql: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
          isReversible: true,
          reverseSql: 'DROP TABLE users',
        },
      ]

      plan.addMigration(simpleMigration, steps, ['Contains mutations'])

      const summary = plan.getSummary()
      expect(summary.totalMigrations).toBe(1)
      expect(summary.totalSteps).toBe(1)
      expect(summary.warnings).toEqual(['Contains mutations'])
      expect(summary.migrations[0].id).toBe('001_create_users')
      expect(summary.migrations[0].description).toBe('Create users table')
    })

    it('should handle FileMigration', () => {
      const fileMigration: FileMigration = {
        id: '002_create_posts',
        filename: '002_create_posts.sql',
        content: 'CREATE TABLE posts (id UInt32, title String) ENGINE = MergeTree() ORDER BY id',
        checksum: 'def456',
        steps: [
          { type: 'up', sql: 'CREATE TABLE posts (id UInt32, title String) ENGINE = MergeTree() ORDER BY id' },
          { type: 'down', sql: 'DROP TABLE posts' },
        ],
        up: [{ type: 'up', sql: 'CREATE TABLE posts (id UInt32, title String) ENGINE = MergeTree() ORDER BY id' }],
        down: [{ type: 'down', sql: 'DROP TABLE posts' }],
        createdAt: new Date(),
        description: 'Create posts table',
      }

      const steps: MigrationStep[] = [
        {
          type: 'create_table',
          description: 'Create posts table',
          sql: 'CREATE TABLE posts (id UInt32, title String) ENGINE = MergeTree() ORDER BY id',
          isReversible: true,
          reverseSql: 'DROP TABLE posts',
        },
      ]

      plan.addMigration(fileMigration, steps)

      const summary = plan.getSummary()
      expect(summary.totalMigrations).toBe(1)
      expect(summary.totalSteps).toBe(1)
      expect(summary.migrations[0].id).toBe('002_create_posts')
      expect(summary.migrations[0].description).toBe('Create posts table')
    })
  })
})
