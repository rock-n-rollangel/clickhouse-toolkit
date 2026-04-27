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

export interface RawExpr {
  type: 'raw'
  sql: string
}

export interface FunctionCall {
  type: 'function'
  name: string
  args: Expr[]
}

export interface CaseExpr {
  type: 'case'
  cases: Array<{ condition: PredicateNode; then: Expr }>
  else?: Expr
}

export type Expr = ColumnRef | Value | ArrayValue | TupleValue | Subquery | RawExpr | FunctionCall | CaseExpr | WindowExpression

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

export interface RawPredicate {
  type: 'raw_predicate'
  sql: string
}

export type PredicateNode = Predicate | AndPredicate | OrPredicate | NotPredicate | RawPredicate

export interface OrderSpec {
  column: ColumnRef
  direction: 'ASC' | 'DESC'
}

export type FrameBound =
  | 'UNBOUNDED PRECEDING'
  | 'CURRENT ROW'
  | 'UNBOUNDED FOLLOWING'
  | { preceding: number }
  | { following: number }

export interface WindowSpec {
  partitionBy?: string[]
  orderBy?: OrderSpec[]
  frame?: {
    type: 'ROWS' | 'RANGE'
    start: FrameBound
    end?: FrameBound
  }
}

export interface WindowExpression {
  type: 'window'
  fn: FunctionCall
  ref: WindowSpec | { name: string }
  alias?: string
}

export type JoinKind = 'inner' | 'left' | 'right' | 'full' | 'cross'
export type JoinStrictness = 'any' | 'asof' | 'semi' | 'anti'

export interface JoinSpec {
  type: JoinKind
  strictness?: JoinStrictness
  global?: boolean
  table: string | Subquery
  alias?: string
  on?: PredicateNode
  using?: string[]
}

export interface ArrayJoinClause {
  kind: 'inner' | 'left'
  items: Array<{ expr: string; alias?: string }>
}

export interface WithClause {
  alias: string
  query: SelectNode
}

export interface SelectNode {
  type: 'select'
  columns: Expr[]
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
  setOperations?: Array<{
    type: 'union' | 'union_all'
    query: SelectNode
  }>
  windows?: Record<string, WindowSpec>
  arrayJoin?: ArrayJoinClause
}

export interface InsertNode {
  type: 'insert'
  table: string
  columns: string[]
  values: Primitive[][]
  format?: string
  select?: SelectNode
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
