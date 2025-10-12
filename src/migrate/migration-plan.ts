/**
 * Migration planning and dry-run support
 */

import { Migration, FileMigration } from './migration'
import { MigrationStep } from './migration'
import { createLoggerContext } from '../core/logger'

export class MigrationPlan {
  private migrations: Array<{
    migration: Migration | FileMigration
    steps: MigrationStep[]
    warnings: string[]
  }> = []

  /**
   * Add a migration to the plan
   */
  addMigration(migration: Migration | FileMigration, steps: MigrationStep[], warnings: string[] = []): void {
    this.migrations.push({ migration, steps, warnings })
  }

  /**
   * Get all migrations in the plan
   */
  getMigrations(): Array<{
    migration: Migration | FileMigration
    steps: MigrationStep[]
    warnings: string[]
  }> {
    return this.migrations
  }

  /**
   * Get total number of steps
   */
  getTotalSteps(): number {
    return this.migrations.reduce((total, m) => total + (m.steps?.length || 0), 0)
  }

  /**
   * Get all warnings
   */
  getWarnings(): string[] {
    return this.migrations.flatMap((m) => m.warnings)
  }

  /**
   * Check if plan has any warnings
   */
  hasWarnings(): boolean {
    return this.getWarnings().length > 0
  }

  /**
   * Get plan summary
   */
  getSummary(): {
    totalMigrations: number
    totalSteps: number
    warnings: string[]
    migrations: Array<{
      id: string
      description: string
      steps: number
      warnings: string[]
    }>
  } {
    return {
      totalMigrations: this.migrations.length,
      totalSteps: this.getTotalSteps(),
      warnings: this.getWarnings(),
      migrations: this.migrations.map((m) => ({
        id: m.migration.id,
        description: m.migration.description,
        steps: m.steps?.length || 0,
        warnings: m.warnings,
      })),
    }
  }

  /**
   * Print plan to console
   */
  print(): void {
    const logger = createLoggerContext({ component: 'migration-plan', operation: 'print' })
    const summary = this.getSummary()

    logger.info('\nMigration Plan')
    logger.info('================')
    logger.info(`Total migrations: ${summary.totalMigrations}`)
    logger.info(`Total steps: ${summary.totalSteps}`)

    if (summary.warnings.length > 0) {
      logger.warn('\nWarnings:')
      summary.warnings.forEach((warning) => logger.warn(`  - ${warning}`))
    }

    logger.info('\nMigrations:')
    summary.migrations.forEach((m) => {
      logger.info(`  ${m.id}: ${m.description} (${m.steps} steps)`)
      if (m.warnings.length > 0) {
        m.warnings.forEach((warning) => logger.warn(`    ${warning}`))
      }
    })

    logger.info('')
  }
}
