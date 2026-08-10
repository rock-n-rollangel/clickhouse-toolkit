/**
 * Numeric coercion tests
 *
 * ClickHouse's JSON format quotes 64-bit-and-wider integers as strings because
 * they do not survive a JavaScript number, and ships a `meta` block declaring
 * each column's type. These tests pin the rules for turning those quoted values
 * back into numbers without ever corrupting one silently.
 */

import { describe, it, expect } from '@jest/globals'
import { coerceNumericColumns } from '../../../src/core/numeric-coercion'

describe('coerceNumericColumns', () => {
  it('coerces a quoted 64-bit integer to a number', () => {
    const rows = [{ bucket: '2026-07-11', readings: '15' }]
    const out = coerceNumericColumns(rows, [
      { name: 'bucket', type: 'String' },
      { name: 'readings', type: 'UInt64' },
    ])
    expect(out).toEqual([{ bucket: '2026-07-11', readings: 15 }])
  })

  it('leaves a string column alone even when it looks numeric', () => {
    const rows = [{ label: '15' }]
    const out = coerceNumericColumns(rows, [{ name: 'label', type: 'String' }])
    expect(out).toEqual([{ label: '15' }])
  })

  // The whole reason ClickHouse quotes these: Number() cannot hold them. Losing
  // precision quietly would be worse than handing back the string.
  it('keeps an integer beyond Number.MAX_SAFE_INTEGER as a string', () => {
    const rows = [{ big: '18446744073709551615', small: '9007199254740991' }]
    const out = coerceNumericColumns(rows, [
      { name: 'big', type: 'UInt64' },
      { name: 'small', type: 'UInt64' },
    ])
    expect(out).toEqual([{ big: '18446744073709551615', small: 9007199254740991 }])
  })

  it('unwraps Nullable and LowCardinality, and preserves null', () => {
    const rows = [{ a: '7', b: '8', c: null }]
    const out = coerceNumericColumns(rows, [
      { name: 'a', type: 'Nullable(UInt64)' },
      { name: 'b', type: 'LowCardinality(Nullable(Int64))' },
      { name: 'c', type: 'Nullable(UInt64)' },
    ])
    expect(out).toEqual([{ a: 7, b: 8, c: null }])
  })

  it('coerces the elements of a numeric array', () => {
    const rows = [{ counts: ['1', '2', '3'], names: ['a', 'b'] }]
    const out = coerceNumericColumns(rows, [
      { name: 'counts', type: 'Array(UInt64)' },
      { name: 'names', type: 'Array(String)' },
    ])
    expect(out).toEqual([{ counts: [1, 2, 3], names: ['a', 'b'] }])
  })

  // Decimal exists to be exact. A JS float cannot hold it, so it stays a string
  // rather than being quietly rounded.
  it('does not coerce Decimal', () => {
    const rows = [{ amount: '12345.6789' }]
    const out = coerceNumericColumns(rows, [{ name: 'amount', type: 'Decimal(18, 4)' }])
    expect(out).toEqual([{ amount: '12345.6789' }])
  })

  it('leaves a value that is already a number untouched', () => {
    const rows = [{ n: 42 }]
    const out = coerceNumericColumns(rows, [{ name: 'n', type: 'Int32' }])
    expect(out).toEqual([{ n: 42 }])
  })

  it('leaves a non-numeric string in a numeric column alone', () => {
    const rows = [{ n: 'nan-ish', m: '' }]
    const out = coerceNumericColumns(rows, [
      { name: 'n', type: 'Int64' },
      { name: 'm', type: 'Int64' },
    ])
    expect(out).toEqual([{ n: 'nan-ish', m: '' }])
  })

  it('is a no-op without meta, so a format that carries none is unaffected', () => {
    const rows = [{ readings: '15' }]
    expect(coerceNumericColumns(rows, undefined)).toEqual([{ readings: '15' }])
  })

  it('tolerates an empty result and a missing column', () => {
    expect(coerceNumericColumns([], [{ name: 'n', type: 'UInt64' }])).toEqual([])
    expect(coerceNumericColumns([{ other: '1' }], [{ name: 'n', type: 'UInt64' }])).toEqual([{ other: '1' }])
  })

  it('coerces a negative signed integer and a quoted float', () => {
    const rows = [{ delta: '-42', ratio: '1.5' }]
    const out = coerceNumericColumns(rows, [
      { name: 'delta', type: 'Int64' },
      { name: 'ratio', type: 'Float64' },
    ])
    expect(out).toEqual([{ delta: -42, ratio: 1.5 }])
  })
})
