/**
 * Fluent DSL for INSERT queries
 */

import { InsertNode } from '../core/ast'
import { ClickHouseRenderer } from '../dialect-ch/renderer'
import { createValidationError } from '../core/errors'
import { QueryRunner } from '../runner/query-runner'
import { Logger } from '../core/logger'
import { DataFormat } from '@clickhouse/client'
import { QueryBuilder } from './base-builder'
import type { SelectBuilder } from './select-builder'

export class InsertBuilder extends QueryBuilder<InsertNode> {
  private query: InsertNode
  private insertStrategy: 'values' | 'objects' | 'stream' | 'select' = 'values'
  private objectData?: Array<Record<string, any>>
  private streamData?: NodeJS.ReadableStream
  private renderer: ClickHouseRenderer

  constructor(table: string, logger?: Logger) {
    super(logger, 'InsertBuilder')
    this.renderer = new ClickHouseRenderer(this.logger)
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
    this.query.select = undefined
    return this
  }

  value(row: any[]): this {
    this.insertStrategy = 'values'
    this.query.values.push(row)
    this.query.select = undefined
    return this
  }

  objects(data: Array<Record<string, any>>): this {
    this.insertStrategy = 'objects'
    this.objectData = data
    this.query.select = undefined
    return this
  }

  fromStream(stream: NodeJS.ReadableStream): this {
    this.insertStrategy = 'stream'
    this.streamData = stream
    this.query.select = undefined
    return this
  }

  fromSelect(selectQuery: SelectBuilder): this {
    if (!selectQuery || typeof selectQuery.getQuery !== 'function') {
      throw createValidationError('A SelectBuilder instance is required for INSERT ... SELECT', undefined, 'insert')
    }

    this.insertStrategy = 'select'
    this.query.values = []
    this.objectData = undefined
    this.streamData = undefined
    this.query.select = selectQuery.getQuery()
    return this
  }

  // Format specification
  format(fmt: DataFormat): this {
    this.query.format = fmt
    return this
  }

  /**
   * Get the underlying query node for the base class
   */
  protected getQueryNode(): InsertNode {
    return this.query
  }

  // Output methods
  toSQL(): { sql: string; params: any[] } {
    // Normalize AST to IR
    const { normalized, validation } = this.normalizer.normalize(this.query)

    // Use base class validation method
    this.validateAndThrow(validation, 'toSQL')

    // Render IR to SQL using ClickHouse dialect
    return this.renderer.render(normalized)
  }

  async run(runner?: QueryRunner): Promise<void> {
    if (!runner) {
      throw createValidationError('QueryRunner is required to execute queries', undefined, 'runner')
    }

    // Determine insert strategy and execute accordingly
    if (this.insertStrategy === 'values' || this.insertStrategy === 'select') {
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
