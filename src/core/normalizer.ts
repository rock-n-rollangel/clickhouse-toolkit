/**
 * AST to IR normalization
 * Simplifies boolean logic, validates identifiers, resolves types
 */

import { QueryNode, PredicateNode, AndPredicate, OrPredicate, NotPredicate, Predicate, Expr, RawPredicate } from './ast'
import {
  QueryIR,
  ExprIR,
  NormalizedPredicate,
  NormalizedPredicateNode,
  NormalizedAndPredicate,
  NormalizedOrPredicate,
  NormalizedNotPredicate,
  ValidationResult,
} from './ir'
import { ValidationError, createValidationError } from './errors'
import { Logger, LoggingComponent } from './logger'
import { QueryValidator } from './validator'

export class QueryNormalizer extends LoggingComponent {
  private validator: QueryValidator

  constructor(logger?: Logger) {
    super(logger, 'QueryNormalizer')
    this.validator = new QueryValidator(this.logger)
  }

  /**
   * Normalize a query AST to IR
   */
  normalize(query: QueryNode): { normalized: QueryIR; validation: ValidationResult } {
    // Use the centralized validator
    const validation = this.validator.validateQuery(query)

    if (!validation.valid) {
      return { normalized: {} as QueryIR, validation }
    }

    try {
      const normalized = this.normalizeQuery(query)
      return { normalized, validation }
    } catch (error) {
      validation.valid = false
      if (error instanceof ValidationError) {
        validation.errors.push(error.message)
      } else {
        validation.errors.push(error instanceof Error ? error.message : 'Unknown error')
      }
      return { normalized: {} as QueryIR, validation }
    }
  }

  private normalizeQuery(query: QueryNode): QueryIR {
    switch (query.type) {
      case 'select':
        return this.normalizeSelect(query)
      case 'insert':
        return this.normalizeInsert(query)
      case 'update':
        return this.normalizeUpdate(query)
      case 'delete':
        return this.normalizeDelete(query)
      default:
        throw createValidationError(
          `Unsupported query type: ${(query as any).type}`,
          undefined,
          'queryType',
          (query as any).type,
        )
    }
  }

  private normalizeSelect(query: any): QueryIR {
    const predicates = this.normalizePredicates(query.where)
    const prewherePredicates = this.normalizePredicates(query.prewhere).map((p) => ({ ...p, isPrewhere: true }))

    return {
      type: 'select',
      table: query.from?.table || '',
      tableAlias: query.from?.alias,
      columns: query.columns?.map((col: any) => this.normalizeExpression(col)),
      predicates: [...prewherePredicates, ...predicates],
      orderBy: query.orderBy?.map((o: any) => ({
        column: this.normalizeColumnRef(o.column),
        direction: o.direction,
      })),
      limit: query.limit,
      offset: query.offset,
      final: query.final,
      settings: query.settings,
      joins: query.joins?.map((j: any) => ({
        type: j.type,
        table: typeof j.table === 'string' ? j.table : 'subquery',
        alias: j.alias,
        on: this.normalizePredicate(j.on),
      })),
      groupBy: query.groupBy?.map((col: any) => this.normalizeColumnRef(col)),
      having: query.having ? this.normalizePredicates(query.having) : undefined,
    }
  }

  private normalizeInsert(query: any): QueryIR {
    return {
      type: 'insert',
      table: query.table,
      columns: query.columns?.map((col: string) => ({
        exprType: 'column' as const,
        columnName: col,
      })),
      values: query.values,
      predicates: [],
    }
  }

  private normalizeUpdate(query: any): QueryIR {
    return {
      type: 'update',
      table: query.table,
      set: query.set,
      predicates: this.normalizePredicates(query.where),
      settings: query.settings,
    }
  }

  private normalizeDelete(query: any): QueryIR {
    return {
      type: 'delete',
      table: query.table,
      predicates: this.normalizePredicates(query.where),
      settings: query.settings,
    }
  }

  private normalizePredicates(predicate?: PredicateNode): NormalizedPredicateNode[] {
    if (!predicate) return []

    const normalized = this.normalizePredicate(predicate)
    return [normalized]
  }

  private normalizePredicate(predicate: PredicateNode): NormalizedPredicateNode {
    switch (predicate.type) {
      case 'predicate':
        return this.normalizeSimplePredicate(predicate)
      case 'and':
        return this.normalizeAndPredicate(predicate)
      case 'or':
        return this.normalizeOrPredicate(predicate)
      case 'not':
        return this.normalizeNotPredicate(predicate)
      case 'raw_predicate':
        this.logger.warn('Raw SQL predicate used', { sql: (predicate as RawPredicate).sql })
        return {
          type: 'raw_predicate',
          sql: (predicate as RawPredicate).sql,
        }
      default:
        throw createValidationError(
          `Unsupported predicate type: ${(predicate as any).type}`,
          undefined,
          'predicateType',
          (predicate as any).type,
        )
    }
  }

  private normalizeSimplePredicate(predicate: Predicate): NormalizedPredicate {
    return {
      type: 'predicate',
      left: this.normalizeColumnRef(predicate.left),
      operator: predicate.operator,
      right: this.extractValue(predicate.right),
    }
  }

  private normalizeAndPredicate(predicate: AndPredicate): NormalizedAndPredicate {
    return {
      type: 'and',
      predicates: predicate.predicates.map((p) => this.normalizePredicate(p)),
      fromCombinator: predicate.fromCombinator,
    }
  }

  private normalizeOrPredicate(predicate: OrPredicate): NormalizedOrPredicate {
    return {
      type: 'or',
      predicates: predicate.predicates.map((p) => this.normalizePredicate(p)),
    }
  }

  private normalizeNotPredicate(predicate: NotPredicate): NormalizedNotPredicate {
    return {
      type: 'not',
      predicate: this.normalizePredicate(predicate.predicate),
    }
  }

  private normalizeColumnRef(expr: any): string {
    if (expr.type === 'column') {
      return expr.table ? `${expr.table}.${expr.name}` : expr.name
    }
    throw createValidationError('Expected column reference', undefined, 'expression', expr)
  }

  private normalizeExpression(expr: Expr): ExprIR {
    const alias = (expr as any).alias

    switch (expr.type) {
      case 'column':
        return {
          exprType: 'column',
          columnName: expr.name,
          tableName: expr.table,
          alias,
        }

      case 'raw':
        // Log usage for security awareness
        this.logger.warn('Raw SQL expression used', { sql: expr.sql })
        return {
          exprType: 'raw',
          rawSql: expr.sql,
          alias,
        }

      case 'function':
        // Keep structure - don't stringify yet!
        return {
          exprType: 'function',
          functionName: expr.name,
          functionArgs: expr.args.map((arg) => this.normalizeExpression(arg)),
          alias,
        }

      case 'case':
        // Keep structure - don't stringify yet!
        return {
          exprType: 'case',
          caseCases: expr.cases.map((c) => ({
            condition: this.normalizePredicate(c.condition),
            then: this.normalizeExpression(c.then),
          })),
          caseElse: expr.else ? this.normalizeExpression(expr.else) : undefined,
          alias,
        }

      case 'subquery':
        const subqueryIR = this.normalizeQuery(expr.query)
        return {
          exprType: 'subquery',
          subquery: subqueryIR,
          alias,
        }

      case 'value':
        return {
          exprType: 'value',
          value: expr.value,
          alias,
        }

      case 'array':
        return {
          exprType: 'array',
          values: expr.values,
          alias,
        }

      case 'tuple':
        return {
          exprType: 'tuple',
          values: expr.values,
          alias,
        }

      default:
        throw createValidationError(
          `Unsupported expression type: ${(expr as any).type}`,
          undefined,
          'expression',
          (expr as any).type,
        )
    }
  }

  private normalizeColumnWithAlias(expr: any): ExprIR {
    return this.normalizeExpression(expr)
  }

  private extractValue(expr: any): any {
    switch (expr.type) {
      case 'value':
        return expr.value
      case 'array':
        return expr.values
      case 'tuple':
        return expr.values
      case 'column':
        // For column references in JOIN ON clauses, return the column reference as-is
        return expr
      case 'subquery':
        // Return a marker for subqueries that will be handled in rendering
        return { __subquery: this.normalizeQuery(expr.query.query) }
      default:
        throw createValidationError(`Cannot extract value from ${expr.type}`, undefined, 'expression', expr)
    }
  }

  private flattenAnd(predicate: AndPredicate): PredicateNode[] {
    const result: PredicateNode[] = []
    for (const p of predicate.predicates) {
      if (p.type === 'and') {
        result.push(...this.flattenAnd(p))
      } else {
        result.push(p)
      }
    }
    return result
  }

  private flattenOr(predicate: OrPredicate): PredicateNode[] {
    const result: PredicateNode[] = []
    for (const p of predicate.predicates) {
      if (p.type === 'or') {
        result.push(...this.flattenOr(p))
      } else {
        result.push(p)
      }
    }
    return result
  }

  private invertOperator(operator: string): string {
    const inversions: Record<string, string> = {
      '=': '!=',
      '!=': '=',
      '>': '<=',
      '>=': '<',
      '<': '>=',
      '<=': '>',
      IN: 'NOT IN',
      'NOT IN': 'IN',
      LIKE: 'NOT LIKE',
      'NOT LIKE': 'LIKE',
    }
    return inversions[operator] || operator
  }
}
