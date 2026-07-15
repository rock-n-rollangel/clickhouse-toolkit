/**
 * Abstract base class for all query builders
 */

import { QueryNode, PredicateNode } from '../core/ast'
import { ValidationResult } from '../core/ir'
import { Logger, LoggingComponent } from '../core/logger'
import { QueryValidator } from '../core/validator'
import { QueryNormalizer } from '../core/normalizer'
import { createValidationError } from '../core/errors'
import { WhereInput, Operator, PredicateCombinator } from '../core/operators'
import { operatorToPredicate, parseColumnRef, isPredicateCombinator } from './predicate-utils'

export abstract class QueryBuilder<T extends QueryNode> extends LoggingComponent {
  protected validator: QueryValidator
  protected normalizer: QueryNormalizer

  constructor(logger?: Logger, componentName?: string) {
    super(logger, componentName)
    this.validator = new QueryValidator(this.logger)
    this.normalizer = new QueryNormalizer(this.logger)
  }

  /**
   * Validate the current query and throw if invalid
   */
  protected validateAndThrow(result: ValidationResult, context: string): void {
    if (!result.valid) {
      this.logger.error('Validation failed', { context, errors: result.errors })
      throw createValidationError(`Validation failed in ${context}: ${result.errors.join(', ')}`, undefined, context)
    }
  }

  /**
   * Abstract method to generate SQL from the query
   */
  abstract toSQL(): { sql: string; params: any[] }

  /**
   * Validate the current query
   */
  validate(): ValidationResult {
    return this.validator.validateQuery(this.getQueryNode())
  }

  /**
   * Abstract method to get the underlying query node
   */
  protected abstract getQueryNode(): T
}

/**
 * Base class for builders that support a WHERE clause (SELECT / UPDATE / DELETE).
 *
 * Owns the shared predicate-construction machinery so the three builders stay in
 * lockstep. Subclasses implement `buildPredicate` — the WHERE entrypoint —
 * (SELECT additionally accepts Raw expressions); the combinator/column handling
 * below is common to all three.
 */
export abstract class WhereBuilder<T extends QueryNode> extends QueryBuilder<T> {
  protected abstract buildPredicate(input: WhereInput): PredicateNode

  protected applyColumnToCombinator(column: string, combinator: PredicateCombinator): PredicateNode {
    // Convert each predicate in the combinator to apply to the specific column
    const predicates = combinator.predicates.map((pred) => {
      // Check if it's a nested combinator using the shared utility function
      if (isPredicateCombinator(pred)) {
        // It's a nested combinator - recursively apply the column
        return this.applyColumnToCombinator(column, pred)
      }
      // Check if it's a WhereInput (Record) - if it's not a combinator and not an Operator, it's a WhereInput
      if (typeof pred === 'object' && !isPredicateCombinator(pred) && !('type' in pred && 'value' in pred)) {
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

  protected buildPredicateFromOperatorOrWhereInput(input: Operator | WhereInput): PredicateNode {
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

  protected combinatorToPredicate(combinator: PredicateCombinator): PredicateNode {
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
