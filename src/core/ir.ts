/**
 * IR (Intermediate Representation) definitions
 * Normalized, minimal data structure for rendering
 */

import { Primitive } from './ast'

// Forward declarations to break circular dependencies
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

interface BaseExprIR {
  alias?: string
}

interface ColumnExprIR extends BaseExprIR {
  exprType: 'column'
  columnName: string
  tableName?: string
}

interface RawExprIR extends BaseExprIR {
  exprType: 'raw'
  rawSql: string
}

interface FunctionExprIR extends BaseExprIR {
  exprType: 'function'
  functionName: string
  functionArgs: ExprIR[]
}

interface CaseExprIR extends BaseExprIR {
  exprType: 'case'
  caseCases: Array<{ condition: NormalizedPredicateNode; then: ExprIR }>
  caseElse?: ExprIR
}

interface ValueExprIR extends BaseExprIR {
  exprType: 'value'
  value: Primitive
}

interface SubqueryExprIR extends BaseExprIR {
  exprType: 'subquery'
  subquery: QueryIR
}

interface ArrayExprIR extends BaseExprIR {
  exprType: 'array'
  values: Primitive[]
}

interface TupleExprIR extends BaseExprIR {
  exprType: 'tuple'
  values: Primitive[]
}

export type ExprIR =
  | ColumnExprIR
  | RawExprIR
  | FunctionExprIR
  | CaseExprIR
  | ValueExprIR
  | SubqueryExprIR
  | ArrayExprIR
  | TupleExprIR

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
