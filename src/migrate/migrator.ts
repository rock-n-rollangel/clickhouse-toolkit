/**
 * Advanced migrator with versioning, drift detection, ON CLUSTER support
 */

import { QueryRunner } from '../runner/query-runner'
import { valueFormatter } from '../core/value-formatter'
import { DriftDetection } from './drift-detection'
import { Migration, FileMigration, MigrationStep } from './migration'
import { MigrationPlan } from './migration-plan'
import { createMigrationError } from '../core/errors'
import { createLoggerContext } from '../core/logger'
import * as fs from 'fs'
import * as path from 'path'
import * as crypto from 'crypto'

export interface MigratorOptions {
  migrationsTableName?: string
  cluster?: string
  allowMutations?: boolean
  dryRun?: boolean
  migrationsPath?: string
  migrationsPattern?: string
  migrationClasses?: Migration[]
  tsMigrationsPath?: string
  tsMigrationsPattern?: string
}

export interface MigrationStatus {
  id: string
  appliedAt: Date | null
  checksum: string
  cluster: string | null
  status: 'pending' | 'applied' | 'failed'
}

export class Migrator {
  private runner: QueryRunner
  private options: MigratorOptions
  private fileMigrations: FileMigration[] = []
  private classMigrations: Migration[] = []
  private driftDetection: DriftDetection
  private logger = createLoggerContext({ component: 'Migrator' })

  constructor(runner: QueryRunner, options: MigratorOptions = {}) {
    this.runner = runner
    this.options = {
      migrationsTableName: 'migrations',
      cluster: null,
      allowMutations: false,
      dryRun: false,
      migrationsPath: './migrations',
      migrationsPattern: '*.sql',
      migrationClasses: [],
      tsMigrationsPath: './migrations',
      tsMigrationsPattern: '*.ts',
      ...options,
    }
    this.driftDetection = new DriftDetection(runner)
  }

  /**
   * Initialize the migrator and create migrations table if needed
   */
  async init(): Promise<void> {
    await this.createMigrationsTable()
    await this.loadMigrations()
  }

  /**
   * Get migration status
   */
  async status(): Promise<MigrationStatus[]> {
    const results = await this.runner.execute<{
      id: string
      applied_at: string | null
      checksum: string
      cluster: string | null
      status: string
    }>({
      sql: `SELECT * FROM ${this.options.migrationsTableName} ORDER BY id`,
    })

    return results.map((row) => ({
      id: row.id,
      appliedAt: row.applied_at ? new Date(row.applied_at) : null,
      checksum: row.checksum,
      cluster: row.cluster,
      status: row.status as 'pending' | 'applied' | 'failed',
    }))
  }

  /**
   * Plan migrations (dry-run)
   */
  async plan(): Promise<MigrationPlan> {
    await this.loadMigrations()
    const pending = await this.getPendingMigrations()
    const plan = new MigrationPlan()

    for (const migration of pending) {
      let steps: MigrationStep[]
      const warnings: string[] = []

      if ('content' in migration) {
        // FileMigration
        steps = this.convertFileMigrationToSteps(migration)

        // Check for mutations in the SQL
        if (!this.options.allowMutations && this.containsMutations(migration.content)) {
          warnings.push('Migration contains mutations - use --allow-mutations flag')
        }

        // Check for TTL changes
        if (this.containsTTLChanges(migration.content)) {
          warnings.push('Migration contains TTL changes - may cause rebuilds')
        }
      } else {
        // Class-based Migration
        steps = await migration.plan(this.runner, this.options)

        // Check for mutations in class migration SQL
        const migrationContent = migration.getMigrationContent()
        if (!this.options.allowMutations && this.containsMutations(migrationContent)) {
          warnings.push('Migration contains mutations - use --allow-mutations flag')
        }

        // Check for TTL changes in class migration SQL
        if (this.containsTTLChanges(migrationContent)) {
          warnings.push('Migration contains TTL changes - may cause rebuilds')
        }

        // Check safety for class migrations
        const safety = await migration.isSafeToApply(this.runner, this.options)
        if (!safety.safe) {
          warnings.push(...safety.warnings)
        }
      }

      plan.addMigration(migration, steps, warnings)
    }

    return plan
  }

  /**
   * Apply pending migrations
   */
  async up(): Promise<void> {
    const pending = await this.getPendingMigrations()

    for (const migration of pending) {
      await this.applyMigration(migration)
    }
  }

  /**
   * Rollback migrations
   */
  async down(count: number = 1): Promise<void> {
    const applied = await this.getAppliedMigrations()
    const toRollback = applied.slice(-count)

    for (const migration of toRollback.reverse()) {
      await this.rollbackMigration(migration)
    }
  }

  /**
   * Detect schema drift
   */
  async detectDrift(): Promise<any> {
    return await this.driftDetection.detect()
  }

  /**
   * Repair schema drift
   */
  async repairDrift(): Promise<void> {
    const drift = await this.detectDrift()
    if (drift.hasDrift) {
      await this.driftDetection.repair(drift)
    }
  }

  /**
   * Seed data
   */
  async seed(seeder: () => Promise<void>): Promise<void> {
    const logger = createLoggerContext({ component: 'migrator', operation: 'seed' })
    try {
      await seeder()
      logger.info('✅ Seeding completed successfully')
    } catch (error) {
      logger.error('❌ Seeding failed', { error })
      throw error
    }
  }

  // Private methods

  private async createMigrationsTable(): Promise<void> {
    const sql = `
      CREATE TABLE IF NOT EXISTS ${this.options.migrationsTableName} (
        id String,
        applied_at DateTime,
        checksum String,
        cluster Nullable(String),
        status String DEFAULT 'applied'
      ) ENGINE = MergeTree()
      ORDER BY id
    `

    await this.runner.command({ sql })
  }

  private async loadMigrations(): Promise<void> {
    this.logger.debug('Loading migrations', {
      sqlPath: this.options.migrationsPath,
      sqlPattern: this.options.migrationsPattern,
      tsPath: this.options.tsMigrationsPath,
      tsPattern: this.options.tsMigrationsPattern,
      classMigrations: this.options.migrationClasses?.length || 0,
    })

    try {
      // Clear existing migrations to avoid duplicates
      this.fileMigrations = []
      this.classMigrations = []

      // Load SQL file migrations
      await this.loadFileMigrations()

      // Load TypeScript class migrations
      await this.loadClassMigrations()

      // Load TypeScript file migrations
      await this.loadTsFileMigrations()

      this.logger.info('Loaded migrations successfully', {
        fileMigrations: this.fileMigrations.length,
        classMigrations: this.classMigrations.length,
        totalMigrations: this.fileMigrations.length + this.classMigrations.length,
      })
    } catch (error) {
      this.logger.error('Failed to load migrations', {
        error: error instanceof Error ? error.message : String(error),
      })
      throw createMigrationError(
        `Failed to load migrations: ${error instanceof Error ? error.message : String(error)}`,
        undefined,
        'loadMigrations',
      )
    }
  }

  private async loadFileMigrations(): Promise<void> {
    const migrationsPath = this.options.migrationsPath!

    if (!fs.existsSync(migrationsPath)) {
      this.logger.warn('SQL migrations directory does not exist', { path: migrationsPath })
      return
    }

    const files = fs
      .readdirSync(migrationsPath)
      .filter((file) => this.matchesPattern(file, this.options.migrationsPattern!))
      .sort()

    this.logger.info('Found SQL migration files', { count: files.length, files })

    for (const file of files) {
      const migration = await this.loadMigrationFile(migrationsPath, file)
      if (migration) {
        this.fileMigrations.push(migration)
      }
    }
  }

  private async loadClassMigrations(): Promise<void> {
    const classMigrations = this.options.migrationClasses || []

    if (classMigrations.length > 0) {
      this.logger.info('Loading class migrations', { count: classMigrations.length })

      for (const migration of classMigrations) {
        this.classMigrations.push(migration)
      }
    }
  }

  private async loadTsFileMigrations(): Promise<void> {
    const tsMigrationsPath = this.options.tsMigrationsPath!

    if (!fs.existsSync(tsMigrationsPath)) {
      this.logger.warn('TypeScript migrations directory does not exist', { path: tsMigrationsPath })
      return
    }

    const files = fs
      .readdirSync(tsMigrationsPath)
      .filter((file) => this.matchesPattern(file, this.options.tsMigrationsPattern!))
      .sort()

    this.logger.info('Found TypeScript migration files', { count: files.length, files })

    this.logger.warn('TypeScript file migrations require manual registration', {
      files,
      note: 'Use migrationClasses option to register TypeScript migration classes',
    })
  }

  private matchesPattern(filename: string, pattern: string): boolean {
    if (pattern === '*.sql') {
      return filename.endsWith('.sql')
    }

    if (pattern.includes('*')) {
      const regex = new RegExp(pattern.replace(/\*/g, '.*'))
      return regex.test(filename)
    }

    return filename === pattern
  }

  private async loadMigrationFile(migrationsPath: string, filename: string): Promise<FileMigration | null> {
    try {
      const filePath = path.join(migrationsPath, filename)
      const content = fs.readFileSync(filePath, 'utf8')

      const id = path.basename(filename, path.extname(filename))
      const checksum = crypto.createHash('sha256').update(content).digest('hex')
      const steps = this.parseMigrationContent(content)

      const migration: FileMigration = {
        id,
        filename,
        content,
        checksum,
        steps,
        up: steps.filter((step) => step.type === 'up'),
        down: steps.filter((step) => step.type === 'down'),
        createdAt: new Date(),
        description: this.extractDescription(content),
      }

      this.logger.debug('Loaded migration file', {
        id: migration.id,
        filename: migration.filename,
        steps: migration.steps.length,
        checksum: migration.checksum,
      })

      return migration
    } catch (error) {
      this.logger.error('Failed to load migration file', {
        filename,
        error: error instanceof Error ? error.message : String(error),
      })
      return null
    }
  }

  private parseMigrationContent(content: string): Array<{ type: 'up' | 'down'; sql: string; description?: string }> {
    const steps: Array<{ type: 'up' | 'down'; sql: string; description?: string }> = []

    // Split content by migration markers
    const sections = content.split(/^--\s*(up|down)\s*$/im)

    let currentType: 'up' | 'down' | null = null
    let currentSql = ''

    for (const section of sections) {
      const trimmed = section.trim()

      if (trimmed === 'up' || trimmed === 'down') {
        if (currentType && currentSql.trim()) {
          steps.push({
            type: currentType,
            sql: currentSql.trim(),
            description: this.extractStepDescription(currentSql),
          })
        }
        currentType = trimmed as 'up' | 'down'
        currentSql = ''
      } else if (currentType && trimmed) {
        currentSql += (currentSql ? '\n' : '') + trimmed
      }
    }

    // Add the last section if it exists
    if (currentType && currentSql.trim()) {
      steps.push({
        type: currentType,
        sql: currentSql.trim(),
        description: this.extractStepDescription(currentSql),
      })
    }

    // If no explicit up/down sections, treat entire content as 'up'
    if (steps.length === 0 && content.trim()) {
      steps.push({
        type: 'up',
        sql: content.trim(),
        description: this.extractStepDescription(content),
      })
    }

    return steps
  }

  private extractDescription(content: string): string {
    // Extract description from comments at the top of the file
    const lines = content.split('\n')
    const descriptionLines: string[] = []

    for (const line of lines) {
      const trimmed = line.trim()
      if (trimmed.startsWith('--') && !trimmed.startsWith('-- up') && !trimmed.startsWith('-- down')) {
        descriptionLines.push(trimmed.substring(2).trim())
      } else if (trimmed && !trimmed.startsWith('--')) {
        break
      }
    }

    return descriptionLines.join(' ').trim() || 'No description'
  }

  private extractStepDescription(sql: string): string {
    // Extract description from the first comment in the SQL
    const lines = sql.split('\n')
    for (const line of lines) {
      const trimmed = line.trim()
      if (trimmed.startsWith('--') && !trimmed.startsWith('-- up') && !trimmed.startsWith('-- down')) {
        return trimmed.substring(2).trim()
      }
    }
    return 'Migration step'
  }

  private async getPendingMigrations(): Promise<(FileMigration | Migration)[]> {
    const applied = await this.getAppliedMigrationIds()
    const allMigrations = [...this.fileMigrations, ...this.classMigrations]
    return allMigrations.filter((m) => !applied.includes(m.id))
  }

  private async getAppliedMigrations(): Promise<(FileMigration | Migration)[]> {
    const applied = await this.getAppliedMigrationIds()
    const allMigrations = [...this.fileMigrations, ...this.classMigrations]
    return allMigrations.filter((m) => applied.includes(m.id))
  }

  private convertFileMigrationToSteps(migration: FileMigration): MigrationStep[] {
    return migration.up.map((step) => ({
      type: 'custom' as const,
      description: step.description || `Execute ${migration.id}`,
      sql: step.sql,
      isReversible: migration.down.length > 0,
      reverseSql: migration.down.length > 0 ? migration.down[0].sql : undefined,
    }))
  }

  private containsMutations(content: string): boolean {
    const mutationKeywords = [
      'ALTER TABLE',
      'DROP COLUMN',
      'ADD COLUMN',
      'MODIFY COLUMN',
      'RENAME COLUMN',
      'DROP INDEX',
      'ADD INDEX',
    ]
    return mutationKeywords.some((keyword) => content.toUpperCase().includes(keyword.toUpperCase()))
  }

  private containsTTLChanges(content: string): boolean {
    return content.toUpperCase().includes('TTL')
  }

  private async getAppliedMigrationIds(): Promise<string[]> {
    const results = await this.runner.execute<{ id: string }>({
      sql: `SELECT id FROM ${this.options.migrationsTableName} WHERE status = 'applied'`,
    })

    return results.map((row) => row.id)
  }

  private async applyMigration(migration: FileMigration | Migration): Promise<void> {
    let checksum: string

    try {
      if (!this.options.dryRun) {
        if ('content' in migration) {
          // FileMigration
          checksum = migration.checksum
          for (const step of migration.up) {
            await this.runner.command({ sql: step.sql })
          }
        } else {
          // Class-based Migration
          checksum = await migration.calculateChecksum()
          await migration.up(this.runner, this.options)
        }
      } else {
        checksum = 'content' in migration ? migration.checksum : await migration.calculateChecksum()
      }

      await this.recordMigration(migration.id, checksum, 'applied')
      const logger = createLoggerContext({ component: 'migrator', operation: 'apply' })
      logger.info(`Applied migration: ${migration.id}`)
    } catch (error) {
      await this.recordMigration(migration.id, checksum!, 'failed')
      const logger = createLoggerContext({ component: 'migrator', operation: 'apply' })
      logger.error(`Failed migration: ${migration.id}`, { error })
      throw error
    }
  }

  private async rollbackMigration(migration: FileMigration | Migration): Promise<void> {
    try {
      if (!this.options.dryRun) {
        if ('content' in migration) {
          // FileMigration
          for (const step of migration.down) {
            await this.runner.command({ sql: step.sql })
          }
        } else {
          // Class-based Migration
          await migration.down(this.runner, this.options)
        }
      }

      await this.removeMigrationRecord(migration.id)
      const logger = createLoggerContext({ component: 'migrator', operation: 'rollback' })
      logger.info(`Rolled back migration: ${migration.id}`)
    } catch (error) {
      const logger = createLoggerContext({ component: 'migrator', operation: 'rollback' })
      logger.error(`Failed to rollback migration: ${migration.id}`, { error })
      throw error
    }
  }

  private async recordMigration(migrationId: string, checksum: string, status: string): Promise<void> {
    const sql = `
      INSERT INTO ${this.options.migrationsTableName} 
      (id, applied_at, checksum, cluster, status) 
      VALUES (?, ?, ?, ?, ?)
    `

    const sqlWithValues = valueFormatter.injectValues(sql, [
      migrationId,
      new Date()
        .toISOString()
        .replace('T', ' ')
        .replace(/\.\d{3}Z$/, ''),
      checksum,
      this.options.cluster,
      status,
    ])
    await this.runner.command({ sql: sqlWithValues })
  }

  private async removeMigrationRecord(id: string): Promise<void> {
    const sql = `DELETE FROM ${this.options.migrationsTableName} WHERE id = ?`
    const sqlWithValues = valueFormatter.injectValues(sql, [id])
    await this.runner.command({ sql: sqlWithValues })
  }
}
