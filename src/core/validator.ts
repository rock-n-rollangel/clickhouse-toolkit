/**
 * Centralized validation service for queries, expressions, and predicates
 */

import { QueryNode, PredicateNode, Expr } from './ast'
import { ValidationResult } from './ir'
import { ValidationError, createValidationError } from './errors'
import { Logger, LoggingComponent } from './logger'

export class QueryValidator extends LoggingComponent {
  constructor(logger?: Logger) {
    super(logger, 'QueryValidator')
  }

  /**
   * Validate a complete query
   */
  validateQuery(query: QueryNode): ValidationResult {
    const result: ValidationResult = { valid: true, errors: [], warnings: [] }

    try {
      // Validate table name
      if (query.type === 'select' && query.from && typeof query.from.table === 'string') {
        this.validateIdentifier(query.from.table, 'table')
      }

      // Validate column names
      if (query.type === 'select' && query.columns) {
        query.columns.forEach((col) => {
          if (col.type === 'column') {
            this.validateIdentifier(col.name, 'column')
            if (col.table) {
              this.validateIdentifier(col.table, 'table')
            }
          }
        })
      }

      // Validate predicates (only for queries that support WHERE)
      if ('where' in query && query.where) {
        this.validatePredicate(query.where)
      }
    } catch (error) {
      result.valid = false
      if (error instanceof ValidationError) {
        result.errors.push(error.message)
      } else {
        result.errors.push(error instanceof Error ? error.message : 'Unknown validation error')
      }
      this.logger.error('Query validation failed', { error, query: query.type })
    }

    return result
  }

  /**
   * Validate a predicate expression
   */
  validatePredicate(predicate: PredicateNode): ValidationResult {
    const result: ValidationResult = { valid: true, errors: [], warnings: [] }

    try {
      if (predicate.type === 'predicate') {
        if (predicate.left.type === 'column') {
          // Skip validation for EXISTS/NOT EXISTS which don't have a left column
          if (predicate.left.name !== '') {
            this.validateIdentifier(predicate.left.name, 'column')
            if (predicate.left.table) {
              this.validateIdentifier(predicate.left.table, 'table')
            }
          }
        }
      } else if (predicate.type === 'and' || predicate.type === 'or') {
        predicate.predicates.forEach((p) => {
          const subResult = this.validatePredicate(p)
          if (!subResult.valid) {
            result.errors.push(...subResult.errors)
            result.warnings.push(...subResult.warnings)
          }
        })
      } else if (predicate.type === 'not') {
        const subResult = this.validatePredicate(predicate.predicate)
        if (!subResult.valid) {
          result.errors.push(...subResult.errors)
          result.warnings.push(...subResult.warnings)
        }
      }

      if (result.errors.length > 0) {
        result.valid = false
      }
    } catch (error) {
      result.valid = false
      if (error instanceof ValidationError) {
        result.errors.push(error.message)
      } else {
        result.errors.push(error instanceof Error ? error.message : 'Unknown predicate validation error')
      }
      this.logger.error('Predicate validation failed', { error, predicateType: predicate.type })
    }

    return result
  }

  /**
   * Validate an expression
   */
  validateExpression(expr: Expr): ValidationResult {
    const result: ValidationResult = { valid: true, errors: [], warnings: [] }

    try {
      switch (expr.type) {
        case 'column':
          this.validateIdentifier(expr.name, 'column')
          if (expr.table) {
            this.validateIdentifier(expr.table, 'table')
          }
          break

        case 'function':
          // Validate function arguments recursively
          expr.args.forEach((arg, index) => {
            const argResult = this.validateExpression(arg)
            if (!argResult.valid) {
              result.errors.push(`Function argument ${index}: ${argResult.errors.join(', ')}`)
            }
          })
          break

        case 'case':
          // Validate case conditions and expressions
          expr.cases.forEach((caseItem, index) => {
            const conditionResult = this.validatePredicate(caseItem.condition)
            if (!conditionResult.valid) {
              result.errors.push(`Case condition ${index}: ${conditionResult.errors.join(', ')}`)
            }

            const thenResult = this.validateExpression(caseItem.then)
            if (!thenResult.valid) {
              result.errors.push(`Case then ${index}: ${thenResult.errors.join(', ')}`)
            }
          })

          if (expr.else) {
            const elseResult = this.validateExpression(expr.else)
            if (!elseResult.valid) {
              result.errors.push(`Case else: ${elseResult.errors.join(', ')}`)
            }
          }
          break

        case 'subquery':
          // Validate subquery
          const subqueryResult = this.validateQuery(expr.query)
          if (!subqueryResult.valid) {
            result.errors.push(`Subquery validation failed: ${subqueryResult.errors.join(', ')}`)
          }
          break

        case 'raw':
          // Log raw SQL usage for security awareness
          this.logger.warn('Raw SQL expression validated', { sql: expr.sql })
          result.warnings.push('Raw SQL expression used - ensure it is safe from SQL injection')
          break

        case 'value':
        case 'array':
        case 'tuple':
          // These are generally safe
          break

        default:
          result.valid = false
          result.errors.push(`Unsupported expression type: ${(expr as any).type}`)
      }

      if (result.errors.length > 0) {
        result.valid = false
      }
    } catch (error) {
      result.valid = false
      if (error instanceof ValidationError) {
        result.errors.push(error.message)
      } else {
        result.errors.push(error instanceof Error ? error.message : 'Unknown expression validation error')
      }
      this.logger.error('Expression validation failed', { error, expressionType: expr.type })
    }

    return result
  }

  /**
   * Validate an identifier (column name, table name, etc.)
   */
  validateIdentifier(identifier: string, type: string): ValidationResult {
    const result: ValidationResult = { valid: true, errors: [], warnings: [] }

    try {
      if (!identifier || typeof identifier !== 'string') {
        throw createValidationError(
          `Invalid ${type} identifier: must be a non-empty string`,
          undefined,
          type,
          identifier,
        )
      }

      // Allow any identifier - we'll escape it with backticks in the renderer
      // This prevents SQL injection by properly escaping malicious identifiers
    } catch (error) {
      result.valid = false
      if (error instanceof ValidationError) {
        result.errors.push(error.message)
      } else {
        result.errors.push(error instanceof Error ? error.message : 'Unknown identifier validation error')
      }
      this.logger.error('Identifier validation failed', { error, identifier, type })
    }

    return result
  }
}
