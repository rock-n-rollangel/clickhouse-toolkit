/**
 * Split a SQL script into individual statements.
 *
 * ClickHouse over HTTP rejects multiple statements in one request
 * (`Multi-statements are not allowed`), so a migration section containing
 * several `;`-separated statements must be executed one statement at a time.
 *
 * Splitting is done on top-level `;` only: semicolons inside string literals
 * ('...'), quoted identifiers ("..." / `...`), line comments (`-- ...`, `# ...`)
 * and block comments (`/* ... *​/`) are ignored. Comments are preserved within
 * their statement (ClickHouse ignores them); fragments that contain no actual
 * SQL (blank or comment-only) are dropped so we never send an empty query.
 */
export function splitSqlStatements(sql: string): string[] {
  const statements: string[] = []
  let current = ''
  let hasContent = false
  let i = 0
  const n = sql.length

  const flush = (): void => {
    if (hasContent) statements.push(current.trim())
    current = ''
    hasContent = false
  }

  while (i < n) {
    const ch = sql[i]
    const next = sql[i + 1]

    // Line comment: `-- ...` or `# ...` up to end of line.
    if ((ch === '-' && next === '-') || ch === '#') {
      const nl = sql.indexOf('\n', i)
      const end = nl === -1 ? n : nl
      current += sql.slice(i, end)
      i = end
      continue
    }

    // Block comment: `/* ... */`.
    if (ch === '/' && next === '*') {
      const close = sql.indexOf('*/', i + 2)
      const end = close === -1 ? n : close + 2
      current += sql.slice(i, end)
      i = end
      continue
    }

    // String literal or quoted identifier.
    if (ch === "'" || ch === '"' || ch === '`') {
      const quote = ch
      current += ch
      hasContent = true
      i++
      while (i < n) {
        const c = sql[i]
        // Backslash escape (single-quoted strings support \' in ClickHouse).
        if (c === '\\' && quote === "'" && i + 1 < n) {
          current += sql.slice(i, i + 2)
          i += 2
          continue
        }
        if (c === quote) {
          // Doubled quote is an escaped quote, not a terminator.
          if (sql[i + 1] === quote) {
            current += quote + quote
            i += 2
            continue
          }
          current += c
          i++
          break
        }
        current += c
        i++
      }
      continue
    }

    // Statement terminator.
    if (ch === ';') {
      flush()
      i++
      continue
    }

    current += ch
    if (!/\s/.test(ch)) hasContent = true
    i++
  }

  flush()
  return statements
}
