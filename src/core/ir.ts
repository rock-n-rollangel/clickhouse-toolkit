/**
 * IR (Intermediate Representation) definitions
 * Normalized, minimal data structure for rendering
 */

import { Primitive } from './ast'

export interface ExprIR {
  exprType: 'column' | 'raw' | 'function' | 'case' | 'value' | 'subquery' | 'array' | 'tuple'
  alias?: string

  // For columns
  columnName?: string
  tableName?: string

  // For raw SQL
  rawSql?: string

  // For function calls (keep structured!)
  functionName?: string
  functionArgs?: ExprIR[]

  // For CASE expressions (keep structured!)
  caseCases?: Array<{ condition: NormalizedPredicateNode; then: ExprIR }>
  caseElse?: ExprIR

  // For values
  value?: Primitive

  // For arrays/tuples
  values?: Primitive[]

  // For subqueries - will be defined after QueryIR
  subquery?: QueryIR
}

// Keep ColumnIR for backward compatibility
export interface ColumnIR {
  column: string
  alias?: string
}

export interface QueryIR {
  type: 'select' | 'insert' | 'update' | 'delete'
  table: string
  tableAlias?: string
  columns?: ExprIR[]
  predicates: NormalizedPredicateNode[]
  orderBy?: Array<{ column: string; direction: 'ASC' | 'DESC' }>
  limit?: number
  offset?: number
  final?: boolean
  settings?: Record<string, any>
  joins?: Array<{
    type: 'inner' | 'left' | 'right' | 'full'
    table: string
    alias?: string
    on: NormalizedPredicateNode
  }>
  with?: Array<{
    alias: string
    query: QueryIR
  }>
  groupBy?: string[]
  having?: NormalizedPredicateNode[]
  values?: Array<Primitive[]>
  set?: Record<string, Primitive>
}

export interface NormalizedPredicate {
  type: 'predicate'
  left: string // normalized column reference
  operator: string
  right: Primitive | Primitive[]
  isPrewhere?: boolean
}

export interface NormalizedAndPredicate {
  type: 'and'
  predicates: NormalizedPredicateNode[]
  isPrewhere?: boolean
  fromCombinator?: boolean
}

export interface NormalizedOrPredicate {
  type: 'or'
  predicates: NormalizedPredicateNode[]
  isPrewhere?: boolean
}

export interface NormalizedNotPredicate {
  type: 'not'
  predicate: NormalizedPredicateNode
  isPrewhere?: boolean
}

export interface RawPredicateIR {
  type: 'raw_predicate'
  sql: string
}

export type NormalizedPredicateNode =
  | NormalizedPredicate
  | NormalizedAndPredicate
  | NormalizedOrPredicate
  | NormalizedNotPredicate
  | RawPredicateIR

export interface NormalizedQuery {
  type: 'select' | 'insert' | 'update' | 'delete'
  table: string
  columns?: string[]
  predicates: NormalizedPredicate[]
  orderBy?: Array<{ column: string; direction: 'ASC' | 'DESC' }>
  limit?: number
  offset?: number
  final?: boolean
  settings?: Record<string, any>
  joins?: Array<{
    type: 'inner' | 'left' | 'right' | 'full'
    table: string
    alias?: string
    on: NormalizedPredicate
  }>
}

export interface QueryContext {
  queryId: string
  timestamp: Date
  settings: Record<string, any>
}

export interface ValidationResult {
  valid: boolean
  errors: string[]
  warnings: string[]
}
