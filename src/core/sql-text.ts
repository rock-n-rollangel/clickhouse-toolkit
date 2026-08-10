/**
 * Lexical rules for reading SQL text.
 *
 * Several places need to know "is this position ordinary SQL, or is it inside a string
 * literal, a quoted identifier, or a comment?" — the statement splitter, the ON CLUSTER
 * rewriter, and placeholder substitution. Each grew its own copy of these rules, and a
 * blind `String.replace` over SQL is exactly how `ON CLUSTER` and `injectValues` both
 * broke. This module is the single definition; callers should not re-derive it.
 */

export type NonCodeKind = 'literal' | 'comment'

export interface NonCodeRegion {
  /** Index just past the region. */
  end: number
  kind: NonCodeKind
}

/**
 * If a string literal, quoted identifier, or comment begins at `index`, return where it
 * ends and what it was; otherwise return null.
 *
 * Handles `'…'`, `"…"` and backtick-quoted regions — including `''` doubling and, for
 * single-quoted strings, `\'` escapes — plus `-- …`, `# …` line comments and
 * `/* … *\/` block comments. An unterminated region runs to the end of the input, which
 * keeps callers from looping forever on malformed SQL.
 */
export function readNonCode(sql: string, index: number): NonCodeRegion | null {
  const ch = sql[index]
  const next = sql[index + 1]
  const n = sql.length

  // Line comment.
  if ((ch === '-' && next === '-') || ch === '#') {
    const nl = sql.indexOf('\n', index)
    return { end: nl === -1 ? n : nl, kind: 'comment' }
  }

  // Block comment.
  if (ch === '/' && next === '*') {
    const close = sql.indexOf('*/', index + 2)
    return { end: close === -1 ? n : close + 2, kind: 'comment' }
  }

  // String literal or quoted identifier.
  if (ch === "'" || ch === '"' || ch === '`') {
    const quote = ch
    let i = index + 1
    while (i < n) {
      const c = sql[i]
      // ClickHouse supports backslash escapes inside single-quoted strings.
      if (c === '\\' && quote === "'" && i + 1 < n) {
        i += 2
        continue
      }
      if (c === quote) {
        // A doubled quote is an escaped quote, not a terminator.
        if (sql[i + 1] === quote) {
          i += 2
          continue
        }
        return { end: i + 1, kind: 'literal' }
      }
      i++
    }
    return { end: n, kind: 'literal' }
  }

  return null
}
