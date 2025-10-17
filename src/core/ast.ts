/**
 * Core AST (Abstract Syntax Tree) definitions
 * Represents the structure of queries independent of SQL text
 */

export type Primitive = string | number | boolean | Date | null

export interface ColumnRef {
  type: 'column'
  name: string
  table?: string
  alias?: string
}

export interface Value {
  type: 'value'
  value: Primitive
}

export interface ArrayValue {
  type: 'array'
  values: Primitive[]
}

export interface TupleValue {
  type: 'tuple'
  values: Primitive[]
}

export interface Subquery {
  type: 'subquery'
  query: SelectNode
}

export type Expr = ColumnRef | Value | ArrayValue | TupleValue | Subquery

export interface Predicate {
  type: 'predicate'
  left: Expr
  operator: string
  right: Expr
}

export interface AndPredicate {
  type: 'and'
  predicates: PredicateNode[]
  fromCombinator?: boolean
}

export interface OrPredicate {
  type: 'or'
  predicates: PredicateNode[]
}

export interface NotPredicate {
  type: 'not'
  predicate: PredicateNode
}

export type PredicateNode = Predicate | AndPredicate | OrPredicate | NotPredicate

export interface OrderSpec {
  column: ColumnRef
  direction: 'ASC' | 'DESC'
}

export interface JoinSpec {
  type: 'inner' | 'left' | 'right' | 'full'
  table: string | Subquery
  alias?: string
  on: PredicateNode
}

export interface WithClause {
  alias: string
  query: SelectNode
}

export interface SelectNode {
  type: 'select'
  columns: ColumnRef[]
  from?: {
    table: string | Subquery
    alias?: string
  }
  joins?: JoinSpec[]
  with?: WithClause[]
  prewhere?: PredicateNode
  where?: PredicateNode
  groupBy?: ColumnRef[]
  having?: PredicateNode
  orderBy?: OrderSpec[]
  limit?: number
  offset?: number
  final?: boolean
  settings?: Record<string, any>
  format?: string
}

export interface InsertNode {
  type: 'insert'
  table: string
  columns: string[]
  values: Primitive[][]
  format?: string
}

export interface UpdateNode {
  type: 'update'
  table: string
  set: Record<string, Primitive>
  where?: PredicateNode
  settings?: Record<string, any>
}

export interface DeleteNode {
  type: 'delete'
  table: string
  where?: PredicateNode
  settings?: Record<string, any>
}

export type QueryNode = SelectNode | InsertNode | UpdateNode | DeleteNode
