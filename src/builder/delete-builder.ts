/**
 * Fluent DSL for DELETE queries
 */

import { DeleteNode, PredicateNode } from '../core/ast'
import { WhereInput, Operator, PredicateCombinator } from '../core/operators'
import { QueryNormalizer } from '../core/normalizer'
import { ClickHouseRenderer } from '../dialect-ch/renderer'
import { createValidationError } from '../core/errors'
import { QueryRunner } from '../runner/query-runner'
import { Logger, LoggingComponent } from '../core/logger'
import { QueryValidator } from '../core/validator'
import { ValidationResult } from '../core/ir'
import { operatorToPredicate, parseColumnRef } from '../core/predicate-builder'

export class DeleteBuilder extends LoggingComponent {
  private query: DeleteNode
  private normalizer: QueryNormalizer
  private renderer: ClickHouseRenderer
  private validator: QueryValidator

  constructor(table: string, logger?: Logger) {
    super(logger, 'DeleteBuilder')
    this.normalizer = new QueryNormalizer(this.logger)
    this.renderer = new ClickHouseRenderer(this.logger)
    this.validator = new QueryValidator(this.logger)
    this.query = {
      type: 'delete',
      table,
    }
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

  /**
   * Validate the current query
   */
  validate(): ValidationResult {
    return this.validator.validateQuery(this.query)
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
          fromCombinator: true,
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

// Factory function
export function deleteFrom(table: string): DeleteBuilder {
  return new DeleteBuilder(table)
}
