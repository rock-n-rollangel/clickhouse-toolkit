/**
 * Operator helpers for building predicates
 * Values-first approach for safety
 */

import { Primitive } from './ast'

export interface Operator {
  type: string
  value: Primitive | Primitive[] | any // 'any' to support SelectBuilder without circular dependency
}

// Comparison operators
export function Eq(value: Primitive | any): Operator {
  return { type: 'eq', value }
}

export function EqCol(column: string): Operator {
  return { type: 'eq_col', value: column }
}

export function Ne(value: Primitive | any): Operator {
  return { type: 'ne', value }
}

export function Gt(value: Primitive | any): Operator {
  return { type: 'gt', value }
}

export function Gte(value: Primitive | any): Operator {
  return { type: 'gte', value }
}

export function Lt(value: Primitive | any): Operator {
  return { type: 'lt', value }
}

export function Lte(value: Primitive | any): Operator {
  return { type: 'lte', value }
}

// Set operators
export function In(values: Primitive[] | any): Operator {
  return { type: 'in', value: values }
}

export function NotIn(values: Primitive[]): Operator {
  return { type: 'not_in', value: values }
}

export function Between(start: Primitive, end: Primitive): Operator {
  return { type: 'between', value: [start, end] }
}

// String operators
export function Like(pattern: string): Operator {
  return { type: 'like', value: pattern }
}

export function ILike(pattern: string): Operator {
  return { type: 'ilike', value: pattern }
}

export function RegExp(pattern: string, flags?: string): Operator {
  return { type: 'regexp', value: { pattern, flags } as any }
}

// Null operators
export function IsNull(): Operator {
  return { type: 'is_null', value: null }
}

export function IsNotNull(): Operator {
  return { type: 'is_not_null', value: null }
}

// Array operators
export function HasAny(values: Primitive[]): Operator {
  return { type: 'has_any', value: values }
}

export function HasAll(values: Primitive[]): Operator {
  return { type: 'has_all', value: values }
}

// Tuple operators
export function InTuple(tupleRows: Primitive[][]): Operator {
  return { type: 'in_tuple', value: tupleRows as any }
}

// Subquery operators
export function Exists(subquery: any): Operator {
  return { type: 'exists', value: subquery }
}

export function NotExists(subquery: any): Operator {
  return { type: 'not_exists', value: subquery }
}

// Boolean combinators
export interface PredicateCombinator {
  type: 'and' | 'or' | 'not'
  predicates: (WhereInput | Operator)[]
}

export function And(...predicates: (WhereInput | Operator)[]): PredicateCombinator {
  return { type: 'and', predicates }
}

export function Or(...predicates: (WhereInput | Operator)[]): PredicateCombinator {
  return { type: 'or', predicates }
}

export function Not(predicate: WhereInput | Operator): PredicateCombinator {
  return { type: 'not', predicates: [predicate] }
}

// Escape hatch for raw SQL (explicitly unsafe)
export function Raw(sql: string): Operator {
  return { type: 'raw', value: sql }
}

// Helper functions for common patterns
export function startsWith(prefix: string): Operator {
  return Like(`${escapeLikePattern(prefix)}%`)
}

export function endsWith(suffix: string): Operator {
  return Like(`%${escapeLikePattern(suffix)}`)
}

export function contains(substring: string): Operator {
  return Like(`%${escapeLikePattern(substring)}%`)
}

function escapeLikePattern(pattern: string): string {
  return pattern.replace(/[%_\\]/g, '\\$&')
}

// Type definitions
export type WhereInput = Record<string, Operator | PredicateCombinator> | PredicateCombinator
