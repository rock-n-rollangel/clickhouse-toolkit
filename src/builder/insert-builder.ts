/**
 * Fluent DSL for INSERT queries
 */

import { InsertNode } from '../core/ast'
import { QueryNormalizer } from '../core/normalizer'
import { ClickHouseRenderer } from '../dialect-ch/renderer'
import { createValidationError } from '../core/errors'
import { QueryRunner } from '../runner/query-runner'
import { DataFormat } from '@clickhouse/client'

export class InsertBuilder {
  private query: InsertNode
  private insertStrategy: 'values' | 'objects' | 'stream' = 'values'
  private objectData?: Array<Record<string, any>>
  private streamData?: NodeJS.ReadableStream

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

  values(vals: any[][]): this {
    this.insertStrategy = 'values'
    this.query.values = vals
    return this
  }

  value(row: any[]): this {
    this.insertStrategy = 'values'
    this.query.values.push(row)
    return this
  }

  objects(data: Array<Record<string, any>>): this {
    this.insertStrategy = 'objects'
    this.objectData = data
    return this
  }

  fromStream(stream: NodeJS.ReadableStream): this {
    this.insertStrategy = 'stream'
    this.streamData = stream
    return this
  }

  // Format specification
  format(fmt: DataFormat): this {
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
    if (!runner) {
      throw createValidationError('QueryRunner is required to execute queries', undefined, 'runner')
    }

    // Determine insert strategy and execute accordingly
    if (this.insertStrategy === 'values') {
      // Use existing SQL generation approach
      const { sql } = this.toSQL()
      await runner.command({ sql, format: this.query.format as DataFormat })
    } else if (this.insertStrategy === 'objects') {
      // Use ClickHouse client's insert method for objects
      if (!this.objectData) {
        throw createValidationError('Object data is required for object-based inserts', undefined, 'data')
      }
      await runner.insert({
        table: this.query.table,
        values: this.objectData,
        format: (this.query.format as DataFormat) || 'JSONEachRow',
        columns: this.query.columns.length > 0 ? this.query.columns : undefined,
      })
    } else if (this.insertStrategy === 'stream') {
      // Use ClickHouse client's insert method for streams
      if (!this.streamData) {
        throw createValidationError('Stream data is required for stream-based inserts', undefined, 'data')
      }
      await runner.insert({
        table: this.query.table,
        values: this.streamData,
        format: (this.query.format as DataFormat) || 'JSONCompactEachRow',
        columns: this.query.columns.length > 0 ? this.query.columns : undefined,
      })
    }
  }
}

// Factory function
export function insertInto(table: string): InsertBuilder {
  return new InsertBuilder(table)
}
