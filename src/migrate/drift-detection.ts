/**
 * Schema drift detection and repair
 */

import { QueryRunner } from '../runner/query-runner'
import { createLoggerContext } from '../core/logger'

export interface DriftResult {
  hasDrift: boolean
  differences: Array<{
    type: 'table_missing' | 'table_extra' | 'column_missing' | 'column_extra' | 'column_type_mismatch'
    table: string
    column?: string
    expected?: any
    actual?: any
  }>
  repairSQL: string[]
}

export class DriftDetection {
  constructor(private runner: QueryRunner) {}

  /**
   * Detect schema drift
   */
  async detect(): Promise<DriftResult> {
    const differences: DriftResult['differences'] = []
    const repairSQL: string[] = []

    // Get current schema from system tables
    const currentSchema = await this.getCurrentSchema()

    // Get expected schema (this would come from your schema definitions)
    const expectedSchema = await this.getExpectedSchema()

    // Compare tables
    const currentTables = new Set(currentSchema.tables.map((t) => t.name))
    const expectedTables = new Set(expectedSchema.tables.map((t) => t.name))

    // Find missing tables
    for (const table of expectedTables) {
      if (!currentTables.has(table)) {
        differences.push({
          type: 'table_missing',
          table,
          expected: expectedSchema.tables.find((t) => t.name === table),
        })
        repairSQL.push(`CREATE TABLE ${table} (...)`)
      }
    }

    // Find extra tables
    for (const table of currentTables) {
      if (!expectedTables.has(table)) {
        differences.push({
          type: 'table_extra',
          table,
          actual: currentSchema.tables.find((t) => t.name === table),
        })
      }
    }

    // Compare columns for existing tables
    for (const expectedTable of expectedSchema.tables) {
      const currentTable = currentSchema.tables.find((t) => t.name === expectedTable.name)
      if (!currentTable) continue

      const currentColumns = new Map(currentTable.columns.map((c) => [c.name, c]))
      const expectedColumns = new Map(expectedTable.columns.map((c) => [c.name, c]))

      // Find missing columns
      for (const [name, expectedCol] of expectedColumns) {
        if (!currentColumns.has(name)) {
          differences.push({
            type: 'column_missing',
            table: expectedTable.name,
            column: name,
            expected: expectedCol,
          })
          repairSQL.push(`ALTER TABLE ${expectedTable.name} ADD COLUMN ${name} ${expectedCol.type}`)
        }
      }

      // Find extra columns
      for (const [name, currentCol] of currentColumns) {
        if (!expectedColumns.has(name)) {
          differences.push({
            type: 'column_extra',
            table: expectedTable.name,
            column: name,
            actual: currentCol,
          })
        }
      }

      // Find type mismatches
      for (const [name, expectedCol] of expectedColumns) {
        const currentCol = currentColumns.get(name)
        if (currentCol && currentCol.type !== expectedCol.type) {
          differences.push({
            type: 'column_type_mismatch',
            table: expectedTable.name,
            column: name,
            expected: expectedCol.type,
            actual: currentCol.type,
          })
          repairSQL.push(`ALTER TABLE ${expectedTable.name} MODIFY COLUMN ${name} ${expectedCol.type}`)
        }
      }
    }

    return {
      hasDrift: differences.length > 0,
      differences,
      repairSQL,
    }
  }

  /**
   * Repair schema drift
   */
  async repair(drift: DriftResult): Promise<void> {
    const logger = createLoggerContext({ component: 'drift-detection', operation: 'repair' })

    if (!drift.hasDrift) {
      logger.info('✅ No drift detected')
      return
    }

    logger.info(`🔧 Repairing ${drift.differences.length} drift issues...`)

    for (const sql of drift.repairSQL) {
      try {
        await this.runner.command({ sql })
        logger.info(`✅ Executed: ${sql}`)
      } catch (error) {
        logger.error(`❌ Failed to execute: ${sql}`, { error })
        throw error
      }
    }

    logger.info('✅ Schema drift repair completed')
  }

  /**
   * Get current schema from system tables
   */
  private async getCurrentSchema(): Promise<{
    tables: Array<{
      name: string
      columns: Array<{ name: string; type: string }>
    }>
  }> {
    const tables = await this.runner.execute<{
      table: string
      name: string
      type: string
    }>({
      sql: `
        SELECT 
          table,
          name,
          type
        FROM system.columns 
        WHERE database = currentDatabase()
        ORDER BY table, position
      `,
    })

    const tableMap = new Map<string, Array<{ name: string; type: string }>>()

    for (const row of tables) {
      if (!tableMap.has(row.table)) {
        tableMap.set(row.table, [])
      }
      tableMap.get(row.table)!.push({
        name: row.name,
        type: row.type,
      })
    }

    return {
      tables: Array.from(tableMap.entries()).map(([name, columns]) => ({
        name,
        columns,
      })),
    }
  }

  /**
   * Get expected schema (this would be loaded from your schema definitions)
   */
  private async getExpectedSchema(): Promise<{
    tables: Array<{
      name: string
      columns: Array<{ name: string; type: string }>
    }>
  }> {
    return {
      tables: [],
    }
  }
}
