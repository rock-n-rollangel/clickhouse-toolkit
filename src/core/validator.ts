/**
 * Centralized validation service for queries, expressions, and predicates
 */

import { QueryNode, PredicateNode, Expr } from './ast'
import { ValidationResult } from './ir'
import { ValidationError, createValidationError } from './errors'
import { Logger, LoggingComponent } from './logger'

/**
 * The library has TWO identifier rules, and the difference between them is deliberate.
 * They differ by exactly one character — a hyphen — so read this before "fixing" the
 * apparent inconsistency by unifying them.
 *
 * Which rule applies depends entirely on whether the identifier gets wrapped in
 * backticks when it is emitted:
 *
 *  - BACKTICKED_IDENTIFIER is for names the renderer wraps, as `user-profiles`. A hyphen
 *    cannot escape backticks, and `user-profiles` is a legitimate ClickHouse table name,
 *    so hyphens are allowed.
 *  - BARE_IDENTIFIER is for names emitted with no quoting at all: WINDOW names, SETTINGS
 *    keys, function names, the migrations table and cluster names, and the table and
 *    columns handed to @clickhouse/client's insert(). Unquoted, a hyphen parses as the
 *    subtraction operator, so `SETTINGS a-b = 1` does not name `a-b` at all.
 *
 * Merging these into one permissive rule would grant hyphens to every unquoted context
 * and reopen the surface this release closed. If they ever must converge, the safe
 * direction is the strict one.
 */
export const BARE_IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_]*$/
export const BACKTICKED_IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_-]*$/

/**
 * Assert that a value is shaped like an identifier that will be emitted **bare** — see
 * BARE_IDENTIFIER above for why that is the strict rule.
 *
 * This is the single definition for every unquoted context: WINDOW names, SETTINGS keys
 * and function names in the renderer; the migrations table and cluster names in the
 * migrator, where these reach CREATE / ALTER / DROP and run with whatever privileges the
 * migration runner holds; and QueryRunner.insert(), whose client interpolates them raw.
 * Several arrive from configuration or environment variables, so a type annotation
 * guarantees nothing about them at runtime.
 *
 * `qualified` additionally permits a single db.table prefix, each part validated.
 */
export function assertSqlIdentifier(value: unknown, field: string, qualified = false): string {
  const parts = typeof value === 'string' && qualified ? value.split('.') : [value]
  const valid =
    typeof value === 'string' &&
    parts.length <= 2 &&
    parts.every((part) => typeof part === 'string' && BARE_IDENTIFIER.test(part))

  if (!valid) {
    throw createValidationError(
      `Invalid ${field}: ${JSON.stringify(value)} must be a plain identifier` +
        `${qualified ? ' (optionally qualified as db.table)' : ''} - ` +
        `letters, digits and underscores, not starting with a digit.`,
      undefined,
      field,
      value as never,
    )
  }
  return value as string
}

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
