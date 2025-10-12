/**
 * Migration base class and utilities
 */

import { QueryRunner } from '../runner/query-runner'
import { MigratorOptions } from './migrator'
import { createHash } from 'crypto'

export interface MigrationStep {
  type: 'create_table' | 'drop_table' | 'alter_table' | 'create_view' | 'drop_view' | 'insert_data' | 'custom'
  description: string
  sql: string
  isReversible: boolean
  reverseSql?: string
}

export abstract class Migration {
  abstract readonly id: string
  abstract readonly description: string

  /**
   * Apply the migration
   */
  abstract up(runner: QueryRunner, options: MigratorOptions): Promise<void>

  /**
   * Rollback the migration
   */
  abstract down(runner: QueryRunner, options: MigratorOptions): Promise<void>

  /**
   * Plan the migration steps
   */
  async plan(runner: QueryRunner, options: MigratorOptions): Promise<MigrationStep[]> {
    // Default implementation - subclasses can override
    return []
  }

  /**
   * Calculate checksum for the migration
   */
  async calculateChecksum(): Promise<string> {
    const content = this.getMigrationContent()
    return createHash('sha256').update(content).digest('hex')
  }

  /**
   * Get migration content for checksum calculation
   */
  public getMigrationContent(): string {
    return `${this.id}:${this.description}:${this.constructor.name}`
  }

  /**
   * Execute SQL with cluster support
   */
  protected async executeSQL(runner: QueryRunner, sql: string, options: MigratorOptions): Promise<void> {
    if (options.cluster) {
      sql = sql.replace(/CREATE TABLE/g, 'CREATE TABLE ON CLUSTER ' + options.cluster)
      sql = sql.replace(/DROP TABLE/g, 'DROP TABLE ON CLUSTER ' + options.cluster)
      sql = sql.replace(/ALTER TABLE/g, 'ALTER TABLE ON CLUSTER ' + options.cluster)
    }

    await runner.command({ sql })
  }

  /**
   * Check if migration is safe to apply
   */
  async isSafeToApply(runner: QueryRunner, options: MigratorOptions): Promise<{ safe: boolean; warnings: string[] }> {
    const warnings: string[] = []

    // Check for mutations
    if (!options.allowMutations && this.containsMutations()) {
      warnings.push('Migration contains mutations - use --allow-mutations flag')
    }

    // Check for TTL changes
    if (this.containsTTLChanges()) {
      warnings.push('Migration contains TTL changes - may cause rebuilds')
    }

    return {
      safe: warnings.length === 0,
      warnings,
    }
  }

  /**
   * Check if migration contains mutations
   */
  protected containsMutations(): boolean {
    // Override in subclasses to detect mutations
    return false
  }

  /**
   * Check if migration contains TTL changes
   */
  protected containsTTLChanges(): boolean {
    // Override in subclasses to detect TTL changes
    return false
  }
}

/**
 * Simple migration implementation
 */
export class SimpleMigration extends Migration {
  constructor(
    public readonly id: string,
    public readonly description: string,
    private upSQL: string,
    private downSQL: string,
  ) {
    super()
  }

  async up(runner: QueryRunner, options: MigratorOptions): Promise<void> {
    await this.executeSQL(runner, this.upSQL, options)
  }

  async down(runner: QueryRunner, options: MigratorOptions): Promise<void> {
    await this.executeSQL(runner, this.downSQL, options)
  }

  async plan(runner: QueryRunner, options: MigratorOptions): Promise<MigrationStep[]> {
    return [
      {
        type: 'custom',
        description: this.description,
        sql: this.upSQL,
        isReversible: true,
        reverseSql: this.downSQL,
      },
    ]
  }

  public getMigrationContent(): string {
    return `${this.id}:${this.description}:${this.upSQL}:${this.downSQL}`
  }
}

/**
 * File-based migration interface
 */
export interface FileMigration {
  id: string
  filename: string
  content: string
  checksum: string
  steps: Array<{ type: 'up' | 'down'; sql: string; description?: string }>
  up: Array<{ type: 'up' | 'down'; sql: string; description?: string }>
  down: Array<{ type: 'up' | 'down'; sql: string; description?: string }>
  createdAt: Date
  description: string
}
