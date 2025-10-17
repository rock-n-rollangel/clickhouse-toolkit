/**
 * Fluent DSL for SELECT queries
 */

import { SelectNode, PredicateNode } from '../core/ast'
import { Operator, PredicateCombinator, WhereInput } from '../core/operators'
import { QueryNormalizer } from '../core/normalizer'
import { ClickHouseRenderer } from '../dialect-ch/renderer'
import { QueryRunner } from '../runner/query-runner'
import { createValidationError } from '../core/errors'
import { Logger, createLoggerContext } from '../core/logger'
import { operatorToPredicate, parseColumnRef } from '../core/predicate-builder'
import { DataFormat, StreamableDataFormat } from '@clickhouse/client'

export class SelectBuilder {
  private query: SelectNode
  private logger: Logger
  private subqueryAlias?: string

  constructor(columns?: string[] | Record<string, string | any>, logger?: Logger) {
    this.logger = logger || createLoggerContext({ component: 'SelectBuilder' })

    this.logger.debug('Creating SELECT query', { columns })

    this.query = {
      type: 'select',
      columns: this.parseColumns(columns),
    }
  }

  /**
   * Set an alias for this query when used as a subquery
   */
  as(alias: string): this {
    this.subqueryAlias = alias
    return this
  }

  /**
   * Get the alias for this subquery
   */
  getAlias(): string | undefined {
    return this.subqueryAlias
  }

  private parseColumns(columns?: string[] | Record<string, string | any>): any[] {
    if (!columns) {
      return []
    }

    // Array syntax: ['id', 'name', 'age']
    if (Array.isArray(columns)) {
      return columns.map((col) => ({ type: 'column', name: col }))
    }

    // Object syntax: { userId: 'id', userName: 'name', userAge: 'age' }
    // Alias: column expression, Value: alias
    return Object.entries(columns).map(([alias, columnExpr]) => {
      // Check if it's a SelectBuilder (subquery)
      if (columnExpr && typeof columnExpr === 'object' && columnExpr.constructor?.name === 'SelectBuilder') {
        return {
          type: 'subquery',
          query: columnExpr,
          alias: alias,
        }
      }
      return {
        type: 'column',
        name: columnExpr,
        alias: alias,
      }
    })
  }

  // WITH clause
  with(alias: string, expr: any): this {
    if (!this.query.with) {
      this.query.with = []
    }
    this.query.with.push({ alias, query: expr })
    return this
  }

  // FROM clause
  from(table: string | SelectBuilder, alias?: string): this {
    this.logger.debug('Adding FROM clause', { table: typeof table === 'string' ? table : 'subquery', alias })
    this.query.from = {
      table: typeof table === 'string' ? table : (table as any),
      alias,
    }
    return this
  }

  // JOIN clauses
  join(type: 'inner' | 'left' | 'right' | 'full', table: string | SelectBuilder, on: WhereInput, alias?: string): this {
    if (!this.query.joins) {
      this.query.joins = []
    }
    this.query.joins.push({
      type,
      table: typeof table === 'string' ? table : (table as any),
      alias,
      on: this.buildPredicate(on),
    })
    return this
  }

  innerJoin(table: string | SelectBuilder, on: WhereInput, alias?: string): this {
    return this.join('inner', table, on, alias)
  }

  leftJoin(table: string | SelectBuilder, on: WhereInput, alias?: string): this {
    return this.join('left', table, on, alias)
  }

  rightJoin(table: string | SelectBuilder, on: WhereInput, alias?: string): this {
    return this.join('right', table, on, alias)
  }

  fullJoin(table: string | SelectBuilder, on: WhereInput, alias?: string): this {
    return this.join('full', table, on, alias)
  }

  // WHERE clauses
  prewhere(input: WhereInput): this {
    this.query.prewhere = this.buildPredicate(input)
    return this
  }

  where(input: WhereInput): this {
    this.logger.debug('Adding WHERE clause', { input })
    const newPredicate = this.buildPredicate(input)

    if (this.query.where) {
      // Combine existing where with new one using AND
      this.query.where = {
        type: 'and',
        predicates: [this.query.where, newPredicate],
      }
    } else {
      this.query.where = newPredicate
    }

    return this
  }

  // GROUP BY clause
  groupBy(columns: string[]): this {
    this.query.groupBy = columns.map((col) => ({ type: 'column', name: col }))
    return this
  }

  // HAVING clause
  having(input: WhereInput): this {
    this.query.having = this.buildPredicate(input)
    return this
  }

  // ORDER BY clause
  orderBy(specs: Array<{ column: string; direction: 'ASC' | 'DESC' }>): this {
    this.query.orderBy = specs.map((spec) => ({
      column: { type: 'column', name: spec.column },
      direction: spec.direction,
    }))
    return this
  }

  // LIMIT clause
  limit(n: number, offset?: number): this {
    this.query.limit = n
    if (offset !== undefined) {
      this.query.offset = offset
    }
    return this
  }

  // ClickHouse-specific
  final(): this {
    this.query.final = true
    return this
  }

  // Settings
  settings(opts: Record<string, any>): this {
    this.query.settings = opts
    return this
  }

  // Format specification
  format(fmt: DataFormat): this {
    this.query.format = fmt
    return this
  }

  // Output methods
  toSQL(): { sql: string; params: any[] } {
    this.logger.debug('Converting query to SQL')

    // Normalize AST to IR
    const { normalized, validation } = QueryNormalizer.normalize(this.query)

    if (!validation.valid) {
      this.logger.error('Query validation failed', { errors: validation.errors })
      throw createValidationError(`Query validation failed: ${validation.errors.join(', ')}`, undefined, 'query')
    }

    // Render IR to SQL using ClickHouse dialect
    const result = ClickHouseRenderer.render(normalized)

    this.logger.debug('Query converted to SQL', {
      sql: result.sql,
      paramCount: result.params.length,
    })

    return result
  }

  async run<T = unknown>(runner?: QueryRunner): Promise<T[]> {
    const { sql } = this.toSQL()
    if (!runner) {
      throw createValidationError('QueryRunner is required to execute queries', undefined, 'runner')
    }
    return runner.execute<T>({
      sql,
      settings: this.query.settings,
      format: (this.query.format as DataFormat) || 'JSON',
    })
  }

  async stream<T = unknown>(runner?: QueryRunner): Promise<NodeJS.ReadableStream> {
    const { sql } = this.toSQL()
    if (!runner) {
      throw createValidationError('QueryRunner is required to stream queries', undefined, 'runner')
    }

    const format = this.query.format || 'JSONEachRow'

    // Validate that the format is streamable
    this.validateStreamableFormat(format as DataFormat)

    return await runner.stream<T>({
      sql,
      settings: this.query.settings,
      format: format as StreamableDataFormat,
    })
  }

  private validateStreamableFormat(format: DataFormat): void {
    const streamableFormats: StreamableDataFormat[] = [
      'JSONEachRow',
      'JSONStringsEachRow',
      'JSONCompactEachRow',
      'JSONCompactStringsEachRow',
      'JSONCompactEachRowWithNames',
      'JSONCompactEachRowWithNamesAndTypes',
      'JSONCompactStringsEachRowWithNames',
      'JSONCompactStringsEachRowWithNamesAndTypes',
      'CSV',
      'CSVWithNames',
      'CSVWithNamesAndTypes',
      'TabSeparated',
      'TabSeparatedRaw',
      'TabSeparatedWithNames',
      'TabSeparatedWithNamesAndTypes',
    ]

    if (!streamableFormats.includes(format as StreamableDataFormat)) {
      throw createValidationError(
        `Format '${format}' is not streamable. Streamable formats: ${streamableFormats.join(', ')}`,
        undefined,
        'format',
        format,
      )
    }
  }

  // Internal methods
  private buildPredicate(input: WhereInput): PredicateNode {
    if (typeof input === 'object' && !Array.isArray(input) && 'type' in input) {
      // It's already a PredicateCombinator
      return this.combinatorToPredicate(input as PredicateCombinator)
    }

    // It's a record of column -> operator/combinator mappings
    const record = input as Record<string, Operator | PredicateCombinator>
    const predicates = Object.entries(record).map(([column, operatorOrCombinator]) => {
      // Check if the value is a combinator
      if (
        typeof operatorOrCombinator === 'object' &&
        'type' in operatorOrCombinator &&
        'predicates' in operatorOrCombinator
      ) {
        // It's a combinator - apply it to the column
        return this.applyColumnToCombinator(column, operatorOrCombinator as PredicateCombinator)
      }
      // It's a regular operator
      return operatorToPredicate(column, operatorOrCombinator as Operator, parseColumnRef)
    })

    if (predicates.length === 1) {
      return predicates[0]
    }

    return {
      type: 'and',
      predicates,
    }
  }

  private applyColumnToCombinator(column: string, combinator: PredicateCombinator): PredicateNode {
    // Convert each predicate in the combinator to apply to the specific column
    const predicates = combinator.predicates.map((pred) => {
      // Check if it's a nested combinator
      if (typeof pred === 'object' && 'type' in pred && 'predicates' in pred) {
        // It's a nested combinator - recursively apply the column
        return this.applyColumnToCombinator(column, pred as PredicateCombinator)
      }
      // Check if it's a WhereInput (Record)
      if (typeof pred === 'object' && !('type' in pred)) {
        // It's a record - build it normally
        return this.buildPredicate(pred)
      }
      // It's an Operator - apply it to the column
      return operatorToPredicate(column, pred as Operator, parseColumnRef)
    })

    // Return the appropriate combinator type
    switch (combinator.type) {
      case 'and':
        return { type: 'and', predicates, fromCombinator: true }
      case 'or':
        return { type: 'or', predicates }
      case 'not':
        return { type: 'not', predicate: predicates[0] }
      default:
        throw createValidationError(
          `Unsupported combinator type: ${(combinator as any).type}`,
          undefined,
          'combinator',
          (combinator as any).type,
        )
    }
  }

  private buildPredicateFromOperatorOrWhereInput(input: Operator | WhereInput): PredicateNode {
    // Check if it's an Operator (has 'type' and 'value')
    if (typeof input === 'object' && 'type' in input && 'value' in input) {
      // This is a bare Operator without a column - this shouldn't happen in normal flow
      // but we handle it for type safety
      throw createValidationError(
        'Cannot use a bare Operator without a column. Use it within a column context like { column: Operator }',
        undefined,
        'operator',
        input.type,
      )
    }
    // It's a WhereInput
    return this.buildPredicate(input as WhereInput)
  }

  private combinatorToPredicate(combinator: PredicateCombinator): PredicateNode {
    switch (combinator.type) {
      case 'and':
        return {
          type: 'and',
          predicates: combinator.predicates.map((p) => this.buildPredicateFromOperatorOrWhereInput(p)),
        }
      case 'or':
        return {
          type: 'or',
          predicates: combinator.predicates.map((p) => this.buildPredicateFromOperatorOrWhereInput(p)),
        }
      case 'not':
        const firstPredicate = combinator.predicates[0]
        if (!firstPredicate) {
          return {
            type: 'predicate',
            left: { type: 'column', name: '' },
            operator: '=',
            right: { type: 'value', value: null },
          }
        }

        return {
          type: 'not',
          predicate: this.buildPredicateFromOperatorOrWhereInput(firstPredicate),
        }
      default:
        throw createValidationError(
          `Unsupported combinator type: ${(combinator as any).type}`,
          undefined,
          'combinator',
          (combinator as any).type,
        )
    }
  }
}

// WhereInput is imported from core/operators

// Factory function
export function select(columns?: string[] | Record<string, string | any>): SelectBuilder {
  return new SelectBuilder(columns)
}
