/**
 * SQL function helpers for building queries
 * These functions return Expr objects for type-safe query building
 */

import { Expr, ColumnRef, Value, RawExpr, FunctionCall, Primitive, WindowSpec, WindowExpression } from './ast'

/**
 * Creates a raw SQL expression that works in SELECT, WHERE, and other contexts
 * WARNING: This bypasses all SQL escaping. Ensure input is safe to prevent SQL injection.
 *
 * @example
 * // In SELECT
 * select([Raw('COUNT(*) OVER (PARTITION BY user_id)')])
 *
 * // In WHERE (entire predicate)
 * where(Raw('age > 18 AND status = "active"'))
 */
export function Raw(sql: string): RawExpr {
  return { type: 'raw', sql }
}

/**
 * Creates a column reference expression
 */
export function Column(name: string, table?: string): ColumnRef {
  return { type: 'column', name, table }
}

/**
 * Creates a literal value expression
 */
export function Value(value: Primitive): Value {
  return { type: 'value', value }
}

/**
 * Aggregate Functions
 */

export function Count(column?: string | Expr): Overable<FunctionCall> {
  const arg = column
    ? typeof column === 'string'
      ? ({ type: 'column', name: column } as ColumnRef)
      : column
    : ({ type: 'raw', sql: '*' } as RawExpr)
  return withOver({ type: 'function', name: 'count', args: [arg] })
}

export function Sum(column: string | Expr): Overable<FunctionCall> {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return withOver({ type: 'function', name: 'sum', args: [arg] })
}

export function Avg(column: string | Expr): Overable<FunctionCall> {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return withOver({ type: 'function', name: 'avg', args: [arg] })
}

export function Min(column: string | Expr): Overable<FunctionCall> {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return withOver({ type: 'function', name: 'min', args: [arg] })
}

export function Max(column: string | Expr): Overable<FunctionCall> {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return withOver({ type: 'function', name: 'max', args: [arg] })
}

/**
 * String Functions
 */

export function Concat(...columns: (string | Expr)[]): FunctionCall {
  const args = columns.map((col) => (typeof col === 'string' ? ({ type: 'column', name: col } as ColumnRef) : col))
  return { type: 'function', name: 'concat', args }
}

export function Upper(column: string | Expr): FunctionCall {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return { type: 'function', name: 'upper', args: [arg] }
}

export function Lower(column: string | Expr): FunctionCall {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return { type: 'function', name: 'lower', args: [arg] }
}

export function Trim(column: string | Expr): FunctionCall {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return { type: 'function', name: 'trim', args: [arg] }
}

export function Substring(column: string | Expr, start: number, length?: number): FunctionCall {
  const args = [
    typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column,
    { type: 'value', value: start } as Value,
  ]
  if (length !== undefined) {
    args.push({ type: 'value', value: length } as Value)
  }
  return { type: 'function', name: 'substring', args }
}

/**
 * Math Functions
 */

export function Round(column: string | Expr, decimals: number = 0): FunctionCall {
  const args = [
    typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column,
    { type: 'value', value: decimals } as Value,
  ]
  return { type: 'function', name: 'round', args }
}

export function Floor(column: string | Expr): FunctionCall {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return { type: 'function', name: 'floor', args: [arg] }
}

export function Ceil(column: string | Expr): FunctionCall {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return { type: 'function', name: 'ceil', args: [arg] }
}

export function Abs(column: string | Expr): FunctionCall {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return { type: 'function', name: 'abs', args: [arg] }
}

/**
 * Conditional Functions
 */

export function If(condition: string | Expr, thenValue: string | Expr, elseValue: string | Expr): FunctionCall {
  const args = [
    typeof condition === 'string' ? Raw(condition) : condition,
    typeof thenValue === 'string' ? ({ type: 'column', name: thenValue } as ColumnRef) : thenValue,
    typeof elseValue === 'string' ? ({ type: 'column', name: elseValue } as ColumnRef) : elseValue,
  ]
  return { type: 'function', name: 'if', args }
}

export function Coalesce(...values: (string | Expr)[]): FunctionCall {
  const args = values.map((val) => (typeof val === 'string' ? ({ type: 'column', name: val } as ColumnRef) : val))
  return { type: 'function', name: 'coalesce', args }
}

/**
 * Date/Time Functions
 */

export function Now(): FunctionCall {
  return { type: 'function', name: 'now', args: [] }
}

export function Today(): FunctionCall {
  return { type: 'function', name: 'today', args: [] }
}

export function ToDate(column: string | Expr): FunctionCall {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return { type: 'function', name: 'toDate', args: [arg] }
}

export function ToDateTime(column: string | Expr): FunctionCall {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return { type: 'function', name: 'toDateTime', args: [arg] }
}

export function FormatDateTime(column: string | Expr, format: string): FunctionCall {
  const args = [
    typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column,
    { type: 'value', value: format } as Value,
  ]
  return { type: 'function', name: 'formatDateTime', args }
}

/**
 * Array Functions
 */

export function ArrayElement(array: string | Expr, index: number): FunctionCall {
  const args = [
    typeof array === 'string' ? ({ type: 'column', name: array } as ColumnRef) : array,
    { type: 'value', value: index } as Value,
  ]
  return { type: 'function', name: 'arrayElement', args }
}

export function ArrayLength(array: string | Expr): FunctionCall {
  const arg = typeof array === 'string' ? ({ type: 'column', name: array } as ColumnRef) : array
  return { type: 'function', name: 'length', args: [arg] }
}

export function ArrayJoin(array: string | Expr, separator: string = ','): FunctionCall {
  const args = [
    typeof array === 'string' ? ({ type: 'column', name: array } as ColumnRef) : array,
    { type: 'value', value: separator } as Value,
  ]
  return { type: 'function', name: 'arrayStringConcat', args }
}

/**
 * Type Conversion Functions
 */

export function Cast(value: string | Expr, toType: string): FunctionCall {
  const arg = typeof value === 'string' ? ({ type: 'column', name: value } as ColumnRef) : value
  return {
    type: 'function',
    name: 'cast',
    args: [arg, { type: 'raw', sql: toType } as RawExpr],
  }
}

export function ToString(column: string | Expr): FunctionCall {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return { type: 'function', name: 'toString', args: [arg] }
}

export function ToInt(column: string | Expr): FunctionCall {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return { type: 'function', name: 'toInt32', args: [arg] }
}

export function ToFloat(column: string | Expr): FunctionCall {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return { type: 'function', name: 'toFloat64', args: [arg] }
}

/**
 * ClickHouse-Specific Functions
 */

export function Distinct(column: string | Expr): FunctionCall {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return { type: 'function', name: 'distinct', args: [arg] }
}

export function CountDistinct(column: string | Expr): Overable<FunctionCall> {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  // This is a special case - count(distinct x) function
  return withOver({ type: 'function', name: 'count', args: [{ type: 'function', name: 'distinct', args: [arg] }] })
}

export function GroupArray(column: string | Expr): Overable<FunctionCall> {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return withOver({ type: 'function', name: 'groupArray', args: [arg] })
}

export function GroupUniqArray(column: string | Expr): Overable<FunctionCall> {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return withOver({ type: 'function', name: 'groupUniqArray', args: [arg] })
}

export function UniqExact(column: string | Expr): Overable<FunctionCall> {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return withOver({ type: 'function', name: 'uniqExact', args: [arg] })
}

export function Quantile(level: number, column: string | Expr): Overable<FunctionCall> {
  const args = [
    { type: 'value', value: level } as Value,
    typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column,
  ]
  return withOver({ type: 'function', name: 'quantile', args })
}

export function Median(column: string | Expr): Overable<FunctionCall> {
  const arg = typeof column === 'string' ? ({ type: 'column', name: column } as ColumnRef) : column
  return withOver({ type: 'function', name: 'median', args: [arg] })
}

/**
 * Window Function Support
 */

export type Overable<T extends FunctionCall> = T & {
  over(refOrName: WindowSpec | string): WindowExpression
}

function withOver<T extends FunctionCall>(fn: T): Overable<T> {
  const result = fn as Overable<T>
  ;(result as any).over = function (refOrName: WindowSpec | string): WindowExpression {
    return {
      type: 'window',
      fn: this as FunctionCall,
      ref: typeof refOrName === 'string' ? { name: refOrName } : refOrName,
    }
  }
  return result
}

export function RowNumber(): Overable<FunctionCall> {
  return withOver({ type: 'function', name: 'row_number', args: [] })
}

export function Rank(): Overable<FunctionCall> {
  return withOver({ type: 'function', name: 'rank', args: [] })
}

export function DenseRank(): Overable<FunctionCall> {
  return withOver({ type: 'function', name: 'dense_rank', args: [] })
}

export function PercentRank(): Overable<FunctionCall> {
  return withOver({ type: 'function', name: 'percent_rank', args: [] })
}

export function CumeDist(): Overable<FunctionCall> {
  return withOver({ type: 'function', name: 'cume_dist', args: [] })
}

export function Ntile(buckets: number): Overable<FunctionCall> {
  return withOver({
    type: 'function',
    name: 'ntile',
    args: [{ type: 'value', value: buckets } as Value],
  })
}

function colArg(c: string | Expr): Expr {
  return typeof c === 'string' ? ({ type: 'column', name: c } as ColumnRef) : c
}

export function Lag(column: string | Expr, offset?: number, defaultValue?: Primitive): Overable<FunctionCall> {
  const args: Expr[] = [colArg(column)]
  if (offset !== undefined) args.push({ type: 'value', value: offset } as Value)
  if (defaultValue !== undefined) args.push({ type: 'value', value: defaultValue } as Value)
  return withOver({ type: 'function', name: 'lagInFrame', args })
}

export function Lead(column: string | Expr, offset?: number, defaultValue?: Primitive): Overable<FunctionCall> {
  const args: Expr[] = [colArg(column)]
  if (offset !== undefined) args.push({ type: 'value', value: offset } as Value)
  if (defaultValue !== undefined) args.push({ type: 'value', value: defaultValue } as Value)
  return withOver({ type: 'function', name: 'leadInFrame', args })
}

export function FirstValue(column: string | Expr): Overable<FunctionCall> {
  return withOver({ type: 'function', name: 'first_value', args: [colArg(column)] })
}

export function LastValue(column: string | Expr): Overable<FunctionCall> {
  return withOver({ type: 'function', name: 'last_value', args: [colArg(column)] })
}

export function NthValue(column: string | Expr, n: number): Overable<FunctionCall> {
  return withOver({
    type: 'function',
    name: 'nth_value',
    args: [colArg(column), { type: 'value', value: n } as Value],
  })
}

// Re-export Case builder for convenience
export { Case } from './case-builder'
