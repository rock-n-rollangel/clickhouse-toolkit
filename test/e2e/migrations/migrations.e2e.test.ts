/**
 * Migration System E2E Tests
 * Tests complete migration workflow with real ClickHouse instance
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals'
import { Migrator, SimpleMigration, select, insertInto, Eq } from '../../../src/index'
import { testSetup, DEFAULT_TEST_CONFIG } from '../setup/test-setup'
import * as fs from 'fs'
import * as path from 'path'
import { promisify } from 'util'

const writeFile = promisify(fs.writeFile)
const unlink = promisify(fs.unlink)
const mkdir = promisify(fs.mkdir)

describe('Migration System E2E Tests', () => {
  let queryRunner: any
  let migrator: Migrator
  let migrationsPath: string
  let tsMigrationsPath: string

  beforeAll(async () => {
    await testSetup.setup()
    queryRunner = testSetup.getQueryRunner()

    // Create temporary migration directories
    migrationsPath = path.join(__dirname, `temp_migrations_${Date.now()}`)
    tsMigrationsPath = path.join(__dirname, `temp_ts_migrations_${Date.now()}`)

    await mkdir(migrationsPath, { recursive: true })
    await mkdir(tsMigrationsPath, { recursive: true })
  }, 60000)

  afterAll(async () => {
    // Clean up migration files
    try {
      if (fs.existsSync(migrationsPath)) {
        const files = await fs.promises.readdir(migrationsPath)
        for (const file of files) {
          await unlink(path.join(migrationsPath, file))
        }
        await fs.promises.rmdir(migrationsPath)
      }
    } catch (error) {
      // Directory might not exist
    }

    try {
      if (fs.existsSync(tsMigrationsPath)) {
        const files = await fs.promises.readdir(tsMigrationsPath)
        for (const file of files) {
          await unlink(path.join(tsMigrationsPath, file))
        }
        await fs.promises.rmdir(tsMigrationsPath)
      }
    } catch (error) {
      // Directory might not exist
    }

    await testSetup.teardown()
  }, 30000)

  beforeEach(async () => {
    if (!queryRunner) {
      queryRunner = testSetup.getQueryRunner()
    }

    // Clean up any existing migration files to ensure test isolation
    try {
      if (fs.existsSync(migrationsPath)) {
        const files = await fs.promises.readdir(migrationsPath)
        for (const file of files) {
          await unlink(path.join(migrationsPath, file))
        }
      }
    } catch (error) {
      // Directory might not exist
    }
  })

  afterEach(async () => {
    // Clean up migration files after each test
    try {
      if (fs.existsSync(migrationsPath)) {
        const files = await fs.promises.readdir(migrationsPath)
        for (const file of files) {
          await unlink(path.join(migrationsPath, file))
        }
      }
    } catch (error) {
      // Directory might not exist
    }

    // Clean up migration records to ensure test isolation
    if (queryRunner) {
      try {
        await queryRunner.command({ sql: 'DELETE FROM test_migrations WHERE 1=1' })
      } catch (error) {
        // Table might not exist
      }
    }
  })

  describe('Basic Migration Workflow', () => {
    it('should initialize migration table', async () => {
      migrator = new Migrator(queryRunner, {
        migrationsTableName: 'test_migrations',
        migrationsPath,
        tsMigrationsPath,
      })

      await migrator.init()

      // Verify migration table was created
      const results = await select(['name'])
        .from('system.tables')
        .where({ database: Eq(DEFAULT_TEST_CONFIG.database), name: Eq('test_migrations') })
        .run<any>(queryRunner)

      expect(results.length).toBe(1)
      expect(results[0].name).toBe('test_migrations')
    })

    it('should execute simple class migration', async () => {
      const testTableName = `test_migration_table_${Date.now()}`

      migrator = new Migrator(queryRunner, {
        migrationsTableName: 'test_migrations',
        migrationsPath,
        tsMigrationsPath,
        migrationClasses: [
          new SimpleMigration(
            '001_create_test_table',
            'Create test table for migration',
            `CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
              id UInt32,
              name String,
              created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree() ORDER BY id`,
            `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
          ),
        ],
      })

      await migrator.init()

      // Check initial status
      const initialStatus = await migrator.status()
      expect(initialStatus).toEqual([])

      // Plan migration
      const plan = await migrator.plan()
      expect(plan.getMigrations()).toHaveLength(1)
      expect(plan.getMigrations()[0].migration.id).toBe('001_create_test_table')

      // Apply migration
      await migrator.up()

      // Verify table was created
      const tableResults = await select(['name'])
        .from('system.tables')
        .where({ database: Eq(DEFAULT_TEST_CONFIG.database), name: Eq(testTableName) })
        .run<any>(queryRunner)

      expect(tableResults.length).toBe(1)

      // Check migration status
      const statusAfter = await migrator.status()
      expect(statusAfter).toHaveLength(1)
      expect(statusAfter[0].id).toBe('001_create_test_table')
      expect(statusAfter[0].status).toBe('applied')

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE IF EXISTS ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should rollback migration', async () => {
      const testTableName = `test_rollback_table_${Date.now()}`

      migrator = new Migrator(queryRunner, {
        migrationsTableName: 'test_migrations',
        migrationsPath,
        tsMigrationsPath,
        migrationClasses: [
          new SimpleMigration(
            '002_create_rollback_table',
            'Create table for rollback test',
            `CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
              id UInt32,
              data String
            ) ENGINE = MergeTree() ORDER BY id`,
            `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
          ),
        ],
      })

      await migrator.init()
      await migrator.up()

      // Verify table exists
      const tableResults = await select(['name'])
        .from('system.tables')
        .where({ database: Eq(DEFAULT_TEST_CONFIG.database), name: Eq(testTableName) })
        .run(queryRunner)

      expect(tableResults.length).toBe(1)

      // Rollback migration
      await migrator.down(1)

      // Verify table was dropped
      const tableResultsAfter = await select(['name'])
        .from('system.tables')
        .where({ database: Eq(DEFAULT_TEST_CONFIG.database), name: Eq(testTableName) })
        .run(queryRunner)

      expect(tableResultsAfter.length).toBe(0)

      // Check migration status
      const statusAfter = await migrator.status()
      expect(statusAfter).toEqual([])
    })
  })

  describe('File-based Migrations', () => {
    it('should execute SQL file migrations', async () => {
      const testTableName = `test_file_migration_${Date.now()}`

      // Create SQL migration file
      const sqlContent = `-- Create users table migration
-- up
CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
  id UInt32,
  name String,
  email String,
  created_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY id;

-- down
DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName};
`

      const migrationFile = path.join(migrationsPath, '003_create_file_table.sql')
      await writeFile(migrationFile, sqlContent)

      migrator = new Migrator(queryRunner, {
        migrationsTableName: 'test_migrations',
        migrationsPath,
        tsMigrationsPath,
        migrationsPattern: '*.sql',
      })

      await migrator.init()

      // Plan migration
      const plan = await migrator.plan()
      expect(plan.getMigrations()).toHaveLength(1)
      expect(plan.getMigrations()[0].migration.id).toBe('003_create_file_table')

      // Apply migration
      await migrator.up()

      // Verify table was created
      const tableResults = await select(['name'])
        .from('system.tables')
        .where({ database: Eq(DEFAULT_TEST_CONFIG.database), name: Eq(testTableName) })
        .run<any>(queryRunner)

      expect(tableResults.length).toBe(1)

      // Clean up
      await unlink(migrationFile)
      await queryRunner.command({
        sql: `DROP TABLE IF EXISTS ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    it('should handle multiple file migrations in order', async () => {
      const testTable1Name = `test_multi_file_1_${Date.now()}`
      const testTable2Name = `test_multi_file_2_${Date.now()}`

      // Create multiple migration files
      const migration1Content = `-- First migration
-- up
CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTable1Name} (
  id UInt32,
  name String
) ENGINE = MergeTree() ORDER BY id;

-- down
DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTable1Name};
`

      const migration2Content = `-- Second migration
-- up
CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTable2Name} (
  id UInt32,
  description String,
  table1_id UInt32
) ENGINE = MergeTree() ORDER BY id;

-- down
DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTable2Name};
`

      const migration1File = path.join(migrationsPath, '001_create_first_table.sql')
      const migration2File = path.join(migrationsPath, '002_create_second_table.sql')

      await writeFile(migration1File, migration1Content)
      await writeFile(migration2File, migration2Content)

      migrator = new Migrator(queryRunner, {
        migrationsTableName: 'test_migrations',
        migrationsPath,
        tsMigrationsPath,
        migrationsPattern: '*.sql',
      })

      await migrator.init()

      // Plan migrations
      const plan = await migrator.plan()
      expect(plan.getMigrations()).toHaveLength(2)
      expect(plan.getMigrations()[0].migration.id).toBe('001_create_first_table')
      expect(plan.getMigrations()[1].migration.id).toBe('002_create_second_table')

      // Apply migrations
      await migrator.up()

      // Verify both tables were created
      const table1Results = await select(['name'])
        .from('system.tables')
        .where({ database: Eq(DEFAULT_TEST_CONFIG.database), name: Eq(testTable1Name) })
        .run<any>(queryRunner)

      const table2Results = await select(['name'])
        .from('system.tables')
        .where({ database: Eq(DEFAULT_TEST_CONFIG.database), name: Eq(testTable2Name) })
        .run<any>(queryRunner)

      expect(table1Results.length).toBe(1)
      expect(table2Results.length).toBe(1)

      // Clean up
      await unlink(migration1File)
      await unlink(migration2File)
      await queryRunner.command({
        sql: `DROP TABLE IF EXISTS ${DEFAULT_TEST_CONFIG.database}.${testTable1Name}`,
      })
      await queryRunner.command({
        sql: `DROP TABLE IF EXISTS ${DEFAULT_TEST_CONFIG.database}.${testTable2Name}`,
      })
    })
  })

  describe('Mixed Migration Types', () => {
    it('should handle both class and file migrations together', async () => {
      const testTable1Name = `test_mixed_class_${Date.now()}`
      const testTable2Name = `test_mixed_file_${Date.now()}`

      // Create file migration
      const fileMigrationContent = `-- File migration
-- up
CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTable2Name} (
  id UInt32,
  file_data String
) ENGINE = MergeTree() ORDER BY id;

-- down
DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTable2Name};
`

      const migrationFile = path.join(migrationsPath, '001_create_file_table.sql')
      await writeFile(migrationFile, fileMigrationContent)

      // Create class migration
      const classMigration = new SimpleMigration(
        '002_create_class_table',
        'Create class-based table',
        `CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTable1Name} (
          id UInt32,
          class_data String
        ) ENGINE = MergeTree() ORDER BY id`,
        `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTable1Name}`,
      )

      migrator = new Migrator(queryRunner, {
        migrationsTableName: 'test_migrations',
        migrationsPath,
        tsMigrationsPath,
        migrationsPattern: '*.sql',
        migrationClasses: [classMigration],
      })

      await migrator.init()

      // Plan migrations
      const plan = await migrator.plan()
      expect(plan.getMigrations()).toHaveLength(2)

      // File migration should come first (alphabetically)
      expect(plan.getMigrations()[0].migration.id).toBe('001_create_file_table')
      expect(plan.getMigrations()[1].migration.id).toBe('002_create_class_table')

      // Apply migrations
      await migrator.up()

      // Verify both tables were created
      const fileTableResults = await select(['name'])
        .from('system.tables')
        .where({ database: Eq(DEFAULT_TEST_CONFIG.database), name: Eq(testTable2Name) })
        .run<any>(queryRunner)

      const classTableResults = await select(['name'])
        .from('system.tables')
        .where({ database: Eq(DEFAULT_TEST_CONFIG.database), name: Eq(testTable1Name) })
        .run<any>(queryRunner)

      expect(fileTableResults.length).toBe(1)
      expect(classTableResults.length).toBe(1)

      // Clean up
      await unlink(migrationFile)
      await queryRunner.command({
        sql: `DROP TABLE IF EXISTS ${DEFAULT_TEST_CONFIG.database}.${testTable1Name}`,
      })
      await queryRunner.command({
        sql: `DROP TABLE IF EXISTS ${DEFAULT_TEST_CONFIG.database}.${testTable2Name}`,
      })
    })
  })

  describe('Migration Status and Planning', () => {
    it('should track migration status correctly', async () => {
      const testTableName = `test_status_table_${Date.now()}`

      migrator = new Migrator(queryRunner, {
        migrationsTableName: 'test_migrations',
        migrationsPath,
        tsMigrationsPath,
        migrationClasses: [
          new SimpleMigration(
            '001_status_test',
            'Status test migration',
            `CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
              id UInt32,
              status String
            ) ENGINE = MergeTree() ORDER BY id`,
            `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
          ),
        ],
      })

      await migrator.init()

      // Check initial status
      let status = await migrator.status()
      expect(status).toEqual([])

      // Apply migration
      await migrator.up()

      // Check status after application
      status = await migrator.status()
      expect(status).toHaveLength(1)
      expect(status[0].id).toBe('001_status_test')
      expect(status[0].status).toBe('applied')
      expect(status[0].appliedAt).toBeDefined()
      expect(status[0].checksum).toBeDefined()

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE IF EXISTS ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })

    /**
     * Drift detection is not fully implemented yet.
     * The getExpectedSchema() method in DriftDetection is a placeholder that returns
     * an empty schema, so it cannot detect missing tables from migrations.
     * TODO: Implement schema extraction from applied migrations for proper drift detection.
     */
    it.skip('should detect drift and provide repair options', async () => {
      const testTableName = `test_drift_table_${Date.now()}`

      migrator = new Migrator(queryRunner, {
        migrationsTableName: 'test_migrations',
        migrationsPath,
        tsMigrationsPath,
        migrationClasses: [
          new SimpleMigration(
            '001_drift_test',
            'Drift test migration',
            `CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
              id UInt32,
              name String
            ) ENGINE = MergeTree() ORDER BY id`,
            `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
          ),
        ],
      })

      await migrator.init()
      await migrator.up()

      // Manually drop the table to simulate drift
      await queryRunner.command({
        sql: `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })

      // Detect drift
      const drift = await migrator.detectDrift()
      expect(drift.hasDrift).toBe(true)
      expect(drift.differences.length).toBeGreaterThan(0)

      // Repair drift
      await migrator.repairDrift()

      // Verify table was recreated
      const tableResults = await select(['name'])
        .from('system.tables')
        .where({ database: Eq(DEFAULT_TEST_CONFIG.database), name: Eq(testTableName) })
        .run(queryRunner)

      expect(tableResults.length).toBe(1)

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE IF EXISTS ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })
  })

  describe('Error Handling', () => {
    it('should handle migration failures gracefully', async () => {
      migrator = new Migrator(queryRunner, {
        migrationsTableName: 'test_migrations',
        migrationsPath,
        tsMigrationsPath,
        migrationClasses: [
          new SimpleMigration('001_failing_migration', 'Failing migration', 'INVALID SQL STATEMENT', 'DROP TABLE test'),
        ],
      })

      await migrator.init()

      // Attempt to apply failing migration
      await expect(migrator.up()).rejects.toThrow()

      // Check that migration was recorded as failed
      const status = await migrator.status()
      expect(status).toHaveLength(1)
      expect(status[0].status).toBe('failed')
    })

    it('should handle partial migration failures', async () => {
      const testTableName = `test_partial_failure_${Date.now()}`

      migrator = new Migrator(queryRunner, {
        migrationsTableName: 'test_migrations',
        migrationsPath,
        tsMigrationsPath,
        migrationClasses: [
          new SimpleMigration(
            '001_success_migration',
            'Success migration',
            `CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
              id UInt32,
              name String
            ) ENGINE = MergeTree() ORDER BY id`,
            `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
          ),
          new SimpleMigration('002_failing_migration', 'Failing migration', 'INVALID SQL STATEMENT', 'DROP TABLE test'),
        ],
      })

      await migrator.init()

      // Attempt to apply migrations (first should succeed, second should fail)
      await expect(migrator.up()).rejects.toThrow()

      // Verify first migration was applied
      const tableResults = await select(['name'])
        .from('system.tables')
        .where({ database: Eq(DEFAULT_TEST_CONFIG.database), name: Eq(testTableName) })
        .run(queryRunner)

      expect(tableResults.length).toBe(1)

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE IF EXISTS ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })
  })

  describe('Dry Run Mode', () => {
    it('should not execute SQL in dry run mode', async () => {
      const testTableName = `test_dry_run_${Date.now()}`

      migrator = new Migrator(queryRunner, {
        migrationsTableName: 'test_migrations',
        migrationsPath,
        tsMigrationsPath,
        dryRun: true,
        migrationClasses: [
          new SimpleMigration(
            '001_dry_run_test',
            'Dry run test migration',
            `CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
              id UInt32,
              name String
            ) ENGINE = MergeTree() ORDER BY id`,
            `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
          ),
        ],
      })

      await migrator.init()

      // Plan migration
      const plan = await migrator.plan()
      expect(plan.getMigrations()).toHaveLength(1)

      // Apply migration in dry run mode
      await migrator.up()

      // Verify table was NOT created
      const tableResults = await select(['name'])
        .from('system.tables')
        .where({ database: Eq(DEFAULT_TEST_CONFIG.database), name: Eq(testTableName) })
        .run(queryRunner)

      expect(tableResults.length).toBe(0)
    })
  })

  describe('Seeding', () => {
    it('should execute seeder function', async () => {
      const testTableName = `test_seed_table_${Date.now()}`

      migrator = new Migrator(queryRunner, {
        migrationsTableName: 'test_migrations',
        migrationsPath,
        tsMigrationsPath,
        migrationClasses: [
          new SimpleMigration(
            '001_create_seed_table',
            'Create table for seeding',
            `CREATE TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName} (
              id UInt32,
              name String
            ) ENGINE = MergeTree() ORDER BY id`,
            `DROP TABLE ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
          ),
        ],
      })

      await migrator.init()
      await migrator.up()

      // Create seeder function
      const seeder = async () => {
        await insertInto(testTableName)
          .columns(['id', 'name'])
          .values([
            [1, 'Seeded User 1'],
            [2, 'Seeded User 2'],
            [3, 'Seeded User 3'],
          ])
          .run(queryRunner)
      }

      // Execute seeder
      await migrator.seed(seeder)

      // Verify seeding worked
      const results = await select(['count()']).from(testTableName).run(queryRunner)

      expect(parseInt(results[0]['count()'] as string)).toBe(3)

      // Clean up
      await queryRunner.command({
        sql: `DROP TABLE IF EXISTS ${DEFAULT_TEST_CONFIG.database}.${testTableName}`,
      })
    })
  })
})
