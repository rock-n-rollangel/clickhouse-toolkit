/**
 * Numeric coercion for JSON-format results
 *
 * ClickHouse's JSON family of formats quotes 64-bit-and-wider integers as
 * strings, because those values do not survive a JavaScript number. The same
 * response carries a `meta` block declaring each column's type, and that block
 * is the ONLY thing that distinguishes the number 15 from the string "15" once
 * the JSON has been parsed.
 *
 * `QueryRunner.execute` previously read `data` and dropped `meta`, so a
 * `count()` reached callers as "15" with nothing to say it had ever been a
 * number. Every consumer then had to know which of its own columns to coerce —
 * and the ones that did not silently mis-handled them (a chart plotting "15" as
 * a non-number renders nothing at all).
 */

export interface ClickHouseColumnMeta {
  name: string
  type: string
}

/**
 * Types whose values arrive quoted but fit a JS number.
 *
 * Decimal is deliberately absent. Its purpose is exactness and a JS float
 * cannot hold it, so coercing it would trade a correct string for a subtly
 * wrong number. Decimal stays a string.
 */
const COERCIBLE_TYPE = /^(?:U?Int(?:8|16|32|64|128|256)|Float(?:32|64))$/

/** Wrappers that decorate a type without changing how its value is serialised. */
const DECORATOR = /^(?:Nullable|LowCardinality)\((.*)\)$/s

const ARRAY_TYPE = /^Array\((.*)\)$/s

const INTEGER_LITERAL = /^-?\d+$/

type Coercer = (value: unknown) => unknown

function unwrapDecorators(type: string): string {
  let current = type.trim()
  for (;;) {
    const match = DECORATOR.exec(current)
    if (!match) return current
    current = match[1].trim()
  }
}

/**
 * Reacts to the value that actually arrived rather than assuming which types
 * ClickHouse chose to quote: a value already parsed as a number is left alone,
 * so this stays correct if the server's quoting settings differ.
 */
function coerceScalar(value: unknown): unknown {
  if (typeof value !== 'string' || value === '') return value

  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return value

  // The reason the value was quoted in the first place: past 2^53 the round trip
  // is lossy, and a silently corrupted total is worse than a string the caller
  // must handle deliberately.
  if (INTEGER_LITERAL.test(value) && !Number.isSafeInteger(parsed)) return value

  return parsed
}

function coercerFor(type: string): Coercer | null {
  const unwrapped = unwrapDecorators(type)

  const array = ARRAY_TYPE.exec(unwrapped)
  if (array) {
    const element = coercerFor(array[1])
    if (!element) return null
    return (value) => (Array.isArray(value) ? value.map(element) : value)
  }

  return COERCIBLE_TYPE.test(unwrapped) ? coerceScalar : null
}

/**
 * Turns quoted numerics back into numbers, guided by the response's own `meta`.
 *
 * Rows are amended in place: they were produced by parsing this response and are
 * not shared with anything else, so copying them would double the memory of a
 * large result for no benefit. A result whose columns need no coercion is
 * returned untouched without being walked at all.
 */
export function coerceNumericColumns<T>(rows: T[], meta: ClickHouseColumnMeta[] | undefined): T[] {
  if (!Array.isArray(rows) || rows.length === 0 || !Array.isArray(meta)) return rows

  const coercers: Array<[string, Coercer]> = []
  for (const column of meta) {
    if (!column || typeof column.type !== 'string' || typeof column.name !== 'string') continue
    const coercer = coercerFor(column.type)
    if (coercer) coercers.push([column.name, coercer])
  }

  if (coercers.length === 0) return rows

  for (const row of rows) {
    if (!row || typeof row !== 'object') continue
    const record = row as Record<string, unknown>
    for (const [name, coerce] of coercers) {
      if (name in record) record[name] = coerce(record[name])
    }
  }

  return rows
}
