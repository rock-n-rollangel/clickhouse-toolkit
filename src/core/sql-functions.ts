/**
 * SQL function helpers for building queries
 * These functions return SQL expressions as strings
 */

/**
 * Aggregate Functions
 */

export function Count(column: string = '*'): string {
  return `count(${column})`
}

export function Sum(column: string): string {
  return `sum(${column})`
}

export function Avg(column: string): string {
  return `avg(${column})`
}

export function Min(column: string): string {
  return `min(${column})`
}

export function Max(column: string): string {
  return `max(${column})`
}

/**
 * String Functions
 */

export function Concat(...columns: string[]): string {
  return `concat(${columns.join(', ')})`
}

export function Upper(column: string): string {
  return `upper(${column})`
}

export function Lower(column: string): string {
  return `lower(${column})`
}

export function Trim(column: string): string {
  return `trim(${column})`
}

export function Substring(column: string, start: number, length?: number): string {
  return length !== undefined ? `substring(${column}, ${start}, ${length})` : `substring(${column}, ${start})`
}

/**
 * Math Functions
 */

export function Round(column: string, decimals: number = 0): string {
  return `round(${column}, ${decimals})`
}

export function Floor(column: string): string {
  return `floor(${column})`
}

export function Ceil(column: string): string {
  return `ceil(${column})`
}

export function Abs(column: string): string {
  return `abs(${column})`
}

/**
 * Conditional Functions
 */

export function If(condition: string, thenValue: string, elseValue: string): string {
  return `if(${condition}, ${thenValue}, ${elseValue})`
}

export function Coalesce(...values: string[]): string {
  return `coalesce(${values.join(', ')})`
}

/**
 * Date/Time Functions
 */

export function Now(): string {
  return 'now()'
}

export function Today(): string {
  return 'today()'
}

export function ToDate(column: string): string {
  return `toDate(${column})`
}

export function ToDateTime(column: string): string {
  return `toDateTime(${column})`
}

export function FormatDateTime(column: string, format: string): string {
  return `formatDateTime(${column}, '${format}')`
}

/**
 * Array Functions
 */

export function ArrayElement(array: string, index: number): string {
  return `arrayElement(${array}, ${index})`
}

export function ArrayLength(array: string): string {
  return `length(${array})`
}

export function ArrayJoin(array: string, separator: string = ','): string {
  return `arrayStringConcat(${array}, '${separator}')`
}

/**
 * Type Conversion Functions
 */

export function Cast(column: string, type: string): string {
  return `cast(${column} as ${type})`
}

export function ToString(column: string): string {
  return `toString(${column})`
}

export function ToInt(column: string): string {
  return `toInt32(${column})`
}

export function ToFloat(column: string): string {
  return `toFloat64(${column})`
}

/**
 * ClickHouse-Specific Functions
 */

export function Distinct(column: string): string {
  return `distinct ${column}`
}

export function CountDistinct(column: string): string {
  return `count(distinct ${column})`
}

export function GroupArray(column: string): string {
  return `groupArray(${column})`
}

export function GroupUniqArray(column: string): string {
  return `groupUniqArray(${column})`
}

export function UniqExact(column: string): string {
  return `uniqExact(${column})`
}

export function Quantile(level: number, column: string): string {
  return `quantile(${level})(${column})`
}

export function Median(column: string): string {
  return `median(${column})`
}
