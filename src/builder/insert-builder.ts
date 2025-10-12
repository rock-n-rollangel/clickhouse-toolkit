/**
 * Fluent DSL for INSERT queries
 */

import { InsertNode } from '../core/ast'
import { QueryNormalizer } from '../core/normalizer'
import { ClickHouseRenderer } from '../dialect-ch/renderer'
import { createValidationError } from '../core/errors'
import { QueryRunner } from '../runner/query-runner'

export class InsertBuilder {
  private query: InsertNode

  constructor(table: string) {
    this.query = {
      type: 'insert',
      table,
      columns: [],
      values: [],
    }
  }

  // Column specification
  columns(cols: string[]): this {
    this.query.columns = cols
    return this
  }

  // Values specification
  values(vals: any[][]): this {
    this.query.values = vals
    return this
  }

  // Single row values
  value(row: any[]): this {
    this.query.values.push(row)
    return this
  }

  // Format specification
  format(fmt: string): this {
    this.query.format = fmt
    return this
  }

  // Output methods
  toSQL(): { sql: string; params: any[] } {
    // Normalize AST to IR
    const { normalized, validation } = QueryNormalizer.normalize(this.query)

    if (!validation.valid) {
      throw createValidationError(`Query validation failed: ${validation.errors.join(', ')}`, undefined, 'query')
    }

    // Render IR to SQL using ClickHouse dialect
    return ClickHouseRenderer.render(normalized)
  }

  async run(runner?: QueryRunner): Promise<void> {
    const { sql } = this.toSQL()
    if (!runner) {
      throw createValidationError('QueryRunner is required to execute queries', undefined, 'runner')
    }
    await runner.command({ sql })
  }
}

// Factory function
export function insertInto(table: string): InsertBuilder {
  return new InsertBuilder(table)
}
