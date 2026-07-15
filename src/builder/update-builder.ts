/**
 * Fluent DSL for UPDATE queries
 */

import { UpdateNode, PredicateNode } from '../core/ast'
import { WhereInput, Operator, PredicateCombinator } from '../core/operators'
import { ClickHouseRenderer } from '../render/renderer'
import { createValidationError } from '../core/errors'
import { QueryRunner } from '../runner/query-runner'
import { Logger } from '../core/logger'
import { operatorToPredicate, parseColumnRef, isPredicateCombinator } from './predicate-utils'
import { WhereBuilder } from './base-builder'

export class UpdateBuilder extends WhereBuilder<UpdateNode> {
  private query: UpdateNode
  private renderer: ClickHouseRenderer

  constructor(table: string, logger?: Logger) {
    super(logger, 'UpdateBuilder')
    this.renderer = new ClickHouseRenderer(this.logger)
    this.query = {
      type: 'update',
      table,
      set: {},
    }
  }

  // SET clause
  set(values: Record<string, any>): this {
    this.query.set = { ...this.query.set, ...values }
    return this
  }

  // WHERE clause
  where(input: WhereInput): this {
    this.query.where = this.buildPredicate(input)
    return this
  }

  // Settings
  settings(opts: Record<string, any>): this {
    this.query.settings = opts
    return this
  }

  // Output methods
  toSQL(): { sql: string; params: any[] } {
    // Normalize AST to IR
    const { normalized, validation } = this.normalizer.normalize(this.query)

    if (!validation.valid) {
      throw createValidationError(`Query validation failed: ${validation.errors.join(', ')}`, undefined, 'query')
    }

    // Render IR to SQL using ClickHouse dialect
    return this.renderer.render(normalized)
  }

  async run(runner?: QueryRunner): Promise<void> {
    const { sql } = this.toSQL()
    if (!runner) {
      throw createValidationError('QueryRunner is required to execute queries', undefined, 'runner')
    }
    await runner.command({ sql, settings: this.query.settings })
  }

  protected getQueryNode(): UpdateNode {
    return this.query
  }

  // Internal methods
  protected buildPredicate(input: WhereInput): PredicateNode {
    // Check if it's a PredicateCombinator using the shared utility function
    // This properly handles cases where a database field is named 'type'
    if (isPredicateCombinator(input)) {
      return this.combinatorToPredicate(input)
    }

    // It's a record of column -> operator/combinator mappings
    const record = input as Record<string, Operator | PredicateCombinator>
    const predicates = Object.entries(record).map(([column, operatorOrCombinator]) => {
      // Check if the value is a combinator using the shared utility function
      // This properly handles cases where a database field is named 'type'
      if (isPredicateCombinator(operatorOrCombinator)) {
        // It's a combinator - apply it to the column
        return this.applyColumnToCombinator(column, operatorOrCombinator)
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
}

// Factory function
export function update(table: string): UpdateBuilder {
  return new UpdateBuilder(table)
}
