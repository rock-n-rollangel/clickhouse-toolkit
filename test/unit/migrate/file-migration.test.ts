/**
 * File Migration Tests
 * Comprehensive tests for file migration loading and parsing
 */

import { describe, it, expect, beforeEach, jest, afterEach } from '@jest/globals'
import { Migrator, MigratorOptions } from '../../../src/migrate/migrator'
import * as fs from 'fs'
import * as crypto from 'crypto'

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

// Mock crypto module
jest.mock('crypto')
const mockCrypto = crypto as jest.Mocked<typeof crypto>

describe('File Migration Loading', () => {
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

  describe('loadFileMigrations()', () => {
    it('should load SQL migration files', async () => {
      const mockFiles = ['001_create_users.sql', '002_create_posts.sql']
      const mockContent =
        '-- Create users table\nCREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id'

      mockFs.existsSync.mockReturnValue(true)
      mockFs.readdirSync.mockReturnValue(mockFiles as any)
      mockFs.readFileSync.mockReturnValue(mockContent)

      // Mock crypto.createHash
      const mockHash = {
        update: jest.fn().mockReturnThis(),
        digest: jest.fn().mockReturnValue('abc123'),
      }
      mockCrypto.createHash.mockReturnValue(mockHash as any)

      await (migrator as any).loadFileMigrations()

      expect(mockFs.existsSync).toHaveBeenCalledWith('./test-migrations')
      expect(mockFs.readdirSync).toHaveBeenCalledWith('./test-migrations')
      expect(mockFs.readFileSync).toHaveBeenCalledWith('test-migrations/001_create_users.sql', 'utf8')
      expect(mockFs.readFileSync).toHaveBeenCalledWith('test-migrations/002_create_posts.sql', 'utf8')
    })

    it('should handle missing migrations directory', async () => {
      mockFs.existsSync.mockReturnValue(false)

      await (migrator as any).loadFileMigrations()

      expect(mockFs.readdirSync).not.toHaveBeenCalled()
    })

    it('should filter files by pattern', async () => {
      const mockFiles = ['001_create_users.sql', '002_create_posts.sql', 'README.md', 'config.json']

      mockFs.existsSync.mockReturnValue(true)
      mockFs.readdirSync.mockReturnValue(mockFiles as any)
      mockFs.readFileSync.mockReturnValue('CREATE TABLE test (id UInt32) ENGINE = MergeTree() ORDER BY id')

      const mockHash = {
        update: jest.fn().mockReturnThis(),
        digest: jest.fn().mockReturnValue('abc123'),
      }
      mockCrypto.createHash.mockReturnValue(mockHash as any)

      await (migrator as any).loadFileMigrations()

      // Should only process .sql files
      expect(mockFs.readFileSync).toHaveBeenCalledTimes(2)
      expect(mockFs.readFileSync).toHaveBeenCalledWith('test-migrations/001_create_users.sql', 'utf8')
      expect(mockFs.readFileSync).toHaveBeenCalledWith('test-migrations/002_create_posts.sql', 'utf8')
    })

    it('should handle file loading errors', async () => {
      const mockFiles = ['001_create_users.sql', '002_invalid.sql']

      mockFs.existsSync.mockReturnValue(true)
      mockFs.readdirSync.mockReturnValue(mockFiles as any)
      mockFs.readFileSync
        .mockReturnValueOnce('CREATE TABLE users (id UInt32) ENGINE = MergeTree() ORDER BY id')
        .mockImplementationOnce(() => {
          throw new Error('File read error')
        })

      await (migrator as any).loadFileMigrations()

      // Should continue processing other files even if one fails
      expect(mockFs.readFileSync).toHaveBeenCalledTimes(2)
    })
  })

  describe('parseMigrationContent()', () => {
    it('should parse migration with up and down sections', () => {
      const content = `
-- Create users table
-- up
CREATE TABLE users (
  id UInt32,
  name String
) ENGINE = MergeTree()
ORDER BY id

-- down
DROP TABLE users
`

      const steps = (migrator as any).parseMigrationContent(content)

      expect(steps).toHaveLength(2)
      expect(steps[0]).toEqual({
        type: 'up',
        sql: 'CREATE TABLE users (\n  id UInt32,\n  name String\n) ENGINE = MergeTree()\nORDER BY id',
        description: 'Migration step',
      })
      expect(steps[1]).toEqual({
        type: 'down',
        sql: 'DROP TABLE users',
        description: 'Migration step',
      })
    })

    it('should parse migration with only up section', () => {
      const content = `
-- Create users table
CREATE TABLE users (
  id UInt32,
  name String
) ENGINE = MergeTree()
ORDER BY id
`

      const steps = (migrator as any).parseMigrationContent(content)

      expect(steps).toHaveLength(1)
      expect(steps[0]).toEqual({
        type: 'up',
        sql: '-- Create users table\nCREATE TABLE users (\n  id UInt32,\n  name String\n) ENGINE = MergeTree()\nORDER BY id',
        description: 'Create users table',
      })
    })

    it('should handle migration with multiple up/down sections', () => {
      const content = `
-- up
CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id

-- down
DROP TABLE users

-- up
INSERT INTO users VALUES (1, 'John')

-- down
DELETE FROM users WHERE id = 1
`

      const steps = (migrator as any).parseMigrationContent(content)

      expect(steps).toHaveLength(4)
      expect(steps[0].type).toBe('up')
      expect(steps[1].type).toBe('down')
      expect(steps[2].type).toBe('up')
      expect(steps[3].type).toBe('down')
    })

    it('should handle empty content', () => {
      const content = ''

      const steps = (migrator as any).parseMigrationContent(content)

      expect(steps).toHaveLength(0)
    })

    it('should handle content with only comments', () => {
      const content = `
-- This is a comment
-- Another comment
`

      const steps = (migrator as any).parseMigrationContent(content)

      expect(steps).toHaveLength(1)
    })
  })

  describe('extractDescription()', () => {
    it('should extract description from comments', () => {
      const content = `
-- Create users table
-- This migration creates the users table
-- up
CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id
`

      const description = (migrator as any).extractDescription(content)

      expect(description).toBe('Create users table This migration creates the users table')
    })

    it('should return default description when no comments', () => {
      const content = `
CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id
`

      const description = (migrator as any).extractDescription(content)

      expect(description).toBe('No description')
    })

    it('should ignore up/down markers in description', () => {
      const content = `
-- Create users table
-- up
CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id
`

      const description = (migrator as any).extractDescription(content)

      expect(description).toBe('Create users table')
    })
  })

  describe('extractStepDescription()', () => {
    it('should extract description from first comment in SQL', () => {
      const sql = `
-- Create users table
CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id
`

      const description = (migrator as any).extractStepDescription(sql)

      expect(description).toBe('Create users table')
    })

    it('should return default description when no comments', () => {
      const sql = `CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id`

      const description = (migrator as any).extractStepDescription(sql)

      expect(description).toBe('Migration step')
    })

    it('should ignore up/down markers in step description', () => {
      const sql = `
-- Create users table
-- up
CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id
`

      const description = (migrator as any).extractStepDescription(sql)

      expect(description).toBe('Create users table')
    })
  })

  describe('matchesPattern()', () => {
    it('should match SQL files with *.sql pattern', () => {
      const matches = (migrator as any).matchesPattern('001_create_users.sql', '*.sql')
      expect(matches).toBe(true)
    })

    it('should not match non-SQL files with *.sql pattern', () => {
      const matches = (migrator as any).matchesPattern('README.md', '*.sql')
      expect(matches).toBe(false)
    })

    it('should match files with custom pattern', () => {
      const matches = (migrator as any).matchesPattern('migration_001.sql', 'migration_*.sql')
      expect(matches).toBe(true)
    })

    it('should match exact filename', () => {
      const matches = (migrator as any).matchesPattern('specific_file.sql', 'specific_file.sql')
      expect(matches).toBe(true)
    })

    it('should not match different filename', () => {
      const matches = (migrator as any).matchesPattern('other_file.sql', 'specific_file.sql')
      expect(matches).toBe(false)
    })
  })

  describe('loadMigrationFile()', () => {
    it('should load and parse migration file', async () => {
      const filename = '001_create_users.sql'
      const content = `
-- Create users table
-- up
CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id

-- down
DROP TABLE users
`

      mockFs.readFileSync.mockReturnValue(content)

      const mockHash = {
        update: jest.fn().mockReturnThis(),
        digest: jest.fn().mockReturnValue('abc123'),
      }
      mockCrypto.createHash.mockReturnValue(mockHash as any)

      const migration = await (migrator as any).loadMigrationFile('./test-migrations', filename)

      expect(migration).toBeDefined()
      expect(migration.id).toBe('001_create_users')
      expect(migration.filename).toBe(filename)
      expect(migration.content).toBe(content)
      expect(migration.checksum).toBe('abc123')
      expect(migration.steps).toHaveLength(2)
      expect(migration.up).toHaveLength(1)
      expect(migration.down).toHaveLength(1)
      expect(migration.description).toBe('Create users table')
    })

    it('should handle file loading errors', async () => {
      const filename = 'invalid_file.sql'

      mockFs.readFileSync.mockImplementation(() => {
        throw new Error('File read error')
      })

      const migration = await (migrator as any).loadMigrationFile('./test-migrations', filename)

      expect(migration).toBeNull()
    })

    it('should calculate correct checksum', async () => {
      const filename = '001_create_users.sql'
      const content = 'CREATE TABLE users (id UInt32) ENGINE = MergeTree() ORDER BY id'

      mockFs.readFileSync.mockReturnValue(content)

      const mockHash = {
        update: jest.fn().mockReturnThis(),
        digest: jest.fn().mockReturnValue('calculated_checksum'),
      }
      mockCrypto.createHash.mockReturnValue(mockHash as any)

      const migration = await (migrator as any).loadMigrationFile('./test-migrations', filename)

      expect(mockHash.update).toHaveBeenCalledWith(content)
      expect(mockHash.digest).toHaveBeenCalledWith('hex')
      expect(migration.checksum).toBe('calculated_checksum')
    })
  })

  describe('convertFileMigrationToSteps()', () => {
    it('should convert file migration to steps', () => {
      const fileMigration = {
        id: '001_create_users',
        up: [
          {
            type: 'up' as const,
            sql: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
            description: 'Create users table',
          },
        ],
        down: [
          {
            type: 'down' as const,
            sql: 'DROP TABLE users',
            description: 'Drop users table',
          },
        ],
      }

      const steps = (migrator as any).convertFileMigrationToSteps(fileMigration)

      expect(steps).toHaveLength(1)
      expect(steps[0]).toEqual({
        type: 'custom',
        description: 'Create users table',
        sql: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
        isReversible: true,
        reverseSql: 'DROP TABLE users',
      })
    })

    it('should handle migration without down steps', () => {
      const fileMigration = {
        id: '001_create_users',
        up: [
          {
            type: 'up' as const,
            sql: 'CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
            description: 'Create users table',
          },
        ],
        down: [],
      }

      const steps = (migrator as any).convertFileMigrationToSteps(fileMigration)

      expect(steps).toHaveLength(1)
      expect(steps[0].isReversible).toBe(false)
      expect(steps[0].reverseSql).toBeUndefined()
    })
  })

  describe('containsMutations()', () => {
    it('should detect ALTER TABLE mutations', () => {
      const content = 'ALTER TABLE users ADD COLUMN email String'
      const contains = (migrator as any).containsMutations(content)
      expect(contains).toBe(true)
    })

    it('should detect DROP COLUMN mutations', () => {
      const content = 'DROP COLUMN email FROM users'
      const contains = (migrator as any).containsMutations(content)
      expect(contains).toBe(true)
    })

    it('should detect ADD COLUMN mutations', () => {
      const content = 'ADD COLUMN email String TO users'
      const contains = (migrator as any).containsMutations(content)
      expect(contains).toBe(true)
    })

    it('should detect MODIFY COLUMN mutations', () => {
      const content = 'MODIFY COLUMN name String'
      const contains = (migrator as any).containsMutations(content)
      expect(contains).toBe(true)
    })

    it('should detect RENAME COLUMN mutations', () => {
      const content = 'RENAME COLUMN old_name TO new_name'
      const contains = (migrator as any).containsMutations(content)
      expect(contains).toBe(true)
    })

    it('should detect DROP INDEX mutations', () => {
      const content = 'DROP INDEX idx_name'
      const contains = (migrator as any).containsMutations(content)
      expect(contains).toBe(true)
    })

    it('should detect ADD INDEX mutations', () => {
      const content = 'ADD INDEX idx_name (name)'
      const contains = (migrator as any).containsMutations(content)
      expect(contains).toBe(true)
    })

    it('should not detect non-mutation SQL', () => {
      const content = 'CREATE TABLE users (id UInt32) ENGINE = MergeTree() ORDER BY id'
      const contains = (migrator as any).containsMutations(content)
      expect(contains).toBe(false)
    })

    it('should be case insensitive', () => {
      const content = 'alter table users add column email string'
      const contains = (migrator as any).containsMutations(content)
      expect(contains).toBe(true)
    })
  })

  describe('containsTTLChanges()', () => {
    it('should detect TTL changes', () => {
      const content = 'ALTER TABLE users MODIFY TTL created_at + INTERVAL 1 YEAR'
      const contains = (migrator as any).containsTTLChanges(content)
      expect(contains).toBe(true)
    })

    it('should detect TTL in CREATE TABLE', () => {
      const content =
        'CREATE TABLE users (id UInt32, created_at DateTime) ENGINE = MergeTree() TTL created_at + INTERVAL 1 YEAR ORDER BY id'
      const contains = (migrator as any).containsTTLChanges(content)
      expect(contains).toBe(true)
    })

    it('should not detect non-TTL SQL', () => {
      const content = 'CREATE TABLE users (id UInt32) ENGINE = MergeTree() ORDER BY id'
      const contains = (migrator as any).containsTTLChanges(content)
      expect(contains).toBe(false)
    })

    it('should be case insensitive', () => {
      const content = 'alter table users modify ttl created_at + interval 1 year'
      const contains = (migrator as any).containsTTLChanges(content)
      expect(contains).toBe(true)
    })
  })
})
