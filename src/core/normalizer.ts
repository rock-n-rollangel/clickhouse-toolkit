/**
 * AST to IR normalization
 * Simplifies boolean logic, validates identifiers, resolves types
 */

import { QueryNode, PredicateNode, AndPredicate, OrPredicate, NotPredicate, Predicate } from './ast'
import {
  QueryIR,
  NormalizedPredicate,
  NormalizedPredicateNode,
  NormalizedAndPredicate,
  NormalizedOrPredicate,
  NormalizedNotPredicate,
  ValidationResult,
} from './ir'
import { ValidationError, createValidationError } from './errors'

export class QueryNormalizer {
  /**
   * Normalize a query AST to IR
   */
  static normalize(query: QueryNode): { normalized: QueryIR; validation: ValidationResult } {
    const validation: ValidationResult = { valid: true, errors: [], warnings: [] }

    try {
      // Validate query structure
      this.validateQuery(query)

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

  private static validateQuery(query: QueryNode): void {
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
  }

  private static validatePredicate(predicate: PredicateNode): void {
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
      predicate.predicates.forEach((p) => this.validatePredicate(p))
    } else if (predicate.type === 'not') {
      this.validatePredicate(predicate.predicate)
    }
  }

  private static validateIdentifier(identifier: string, type: string): void {
    if (!identifier || typeof identifier !== 'string') {
      throw createValidationError(`Invalid ${type} identifier: must be a non-empty string`, undefined, type, identifier)
    }

    // Allow any identifier - we'll escape it with backticks in the renderer
    // This prevents SQL injection by properly escaping malicious identifiers
    return
  }

  private static normalizeQuery(query: QueryNode): QueryIR {
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

  private static normalizeSelect(query: any): QueryIR {
    const predicates = this.normalizePredicates(query.where)
    const prewherePredicates = this.normalizePredicates(query.prewhere).map((p) => ({ ...p, isPrewhere: true }))

    return {
      type: 'select',
      table: query.from?.table || '',
      tableAlias: query.from?.alias,
      columns: query.columns?.map((col: any) => this.normalizeColumnWithAlias(col)),
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

  private static normalizeInsert(query: any): QueryIR {
    return {
      type: 'insert',
      table: query.table,
      columns: query.columns?.map((col: string) => ({ column: col })),
      values: query.values,
      predicates: [],
    }
  }

  private static normalizeUpdate(query: any): QueryIR {
    return {
      type: 'update',
      table: query.table,
      set: query.set,
      predicates: this.normalizePredicates(query.where),
      settings: query.settings,
    }
  }

  private static normalizeDelete(query: any): QueryIR {
    return {
      type: 'delete',
      table: query.table,
      predicates: this.normalizePredicates(query.where),
      settings: query.settings,
    }
  }

  private static normalizePredicates(predicate?: PredicateNode): NormalizedPredicateNode[] {
    if (!predicate) return []

    const normalized = this.normalizePredicate(predicate)
    return [normalized]
  }

  private static normalizePredicate(predicate: PredicateNode): NormalizedPredicateNode {
    switch (predicate.type) {
      case 'predicate':
        return this.normalizeSimplePredicate(predicate)
      case 'and':
        return this.normalizeAndPredicate(predicate)
      case 'or':
        return this.normalizeOrPredicate(predicate)
      case 'not':
        return this.normalizeNotPredicate(predicate)
      default:
        throw createValidationError(
          `Unsupported predicate type: ${(predicate as any).type}`,
          undefined,
          'predicateType',
          (predicate as any).type,
        )
    }
  }

  private static normalizeSimplePredicate(predicate: Predicate): NormalizedPredicate {
    return {
      type: 'predicate',
      left: this.normalizeColumnRef(predicate.left),
      operator: predicate.operator,
      right: this.extractValue(predicate.right),
    }
  }

  private static normalizeAndPredicate(predicate: AndPredicate): NormalizedAndPredicate {
    return {
      type: 'and',
      predicates: predicate.predicates.map((p) => this.normalizePredicate(p)),
      fromCombinator: predicate.fromCombinator,
    }
  }

  private static normalizeOrPredicate(predicate: OrPredicate): NormalizedOrPredicate {
    return {
      type: 'or',
      predicates: predicate.predicates.map((p) => this.normalizePredicate(p)),
    }
  }

  private static normalizeNotPredicate(predicate: NotPredicate): NormalizedNotPredicate {
    return {
      type: 'not',
      predicate: this.normalizePredicate(predicate.predicate),
    }
  }

  private static normalizeColumnRef(expr: any): string {
    if (expr.type === 'column') {
      return expr.table ? `${expr.table}.${expr.name}` : expr.name
    }
    throw createValidationError('Expected column reference', undefined, 'expression', expr)
  }

  private static normalizeColumnWithAlias(expr: any): { column: string; alias?: string } {
    if (expr.type === 'column') {
      const column = expr.table ? `${expr.table}.${expr.name}` : expr.name
      return expr.alias ? { column, alias: expr.alias } : { column }
    }
    if (expr.type === 'subquery') {
      // Subquery in SELECT - normalize it and return as a special marker
      const subqueryIR = this.normalizeQuery(expr.query.query)
      const alias = expr.query.getAlias?.() || expr.alias
      return { column: `__SUBQUERY_${JSON.stringify(subqueryIR)}__`, alias }
    }
    throw createValidationError('Expected column reference', undefined, 'expression', expr)
  }

  private static extractValue(expr: any): any {
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

  private static flattenAnd(predicate: AndPredicate): PredicateNode[] {
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

  private static flattenOr(predicate: OrPredicate): PredicateNode[] {
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

  private static invertOperator(operator: string): string {
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
