import { readNonCode } from '../core/sql-text'

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

  const flush = (): void => {
    if (hasContent) statements.push(current.trim())
    current = ''
    hasContent = false
  }

  while (i < sql.length) {
    // Literals and comments are copied through whole; only a literal counts as content,
    // so a comment-only fragment is still dropped.
    const region = readNonCode(sql, i)
    if (region) {
      current += sql.slice(i, region.end)
      if (region.kind === 'literal') hasContent = true
      i = region.end
      continue
    }

    const ch = sql[i]
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

/**
 * Insert `ON CLUSTER <name>` into the DDL statements of a migration script.
 *
 * ClickHouse puts the clause *after* the table name:
 *
 *   CREATE TABLE [IF NOT EXISTS] [db.]name ON CLUSTER c (...)
 *   DROP TABLE   [IF EXISTS]     [db.]name ON CLUSTER c
 *   ALTER TABLE                  [db.]name ON CLUSTER c ...
 *
 * The previous implementation ran three `sql.replace(/CREATE TABLE/g, ...)` passes and
 * emitted `CREATE TABLE ON CLUSTER c t (...)`, which ClickHouse rejects outright with
 * SYNTAX_ERROR (code 62) at the `CLUSTER` token — so `ON CLUSTER` never worked at all.
 * That approach also rewrote matches inside string literals and comments, and cascaded,
 * because text inserted by one pass was rewritten again by the next.
 *
 * This is a single left-to-right scan instead. It shares the literal/comment rules of
 * splitSqlStatements above, and only rewrites at a *statement head* — the start of the
 * script or of a top-level `;`-separated statement — so a `CREATE TABLE` appearing
 * inside a string, a comment or mid-statement is left alone, and inserted text is never
 * re-examined.
 *
 * Statements that already carry an `ON CLUSTER` clause are left untouched.
 *
 * Only `TABLE` statements are handled, matching the behaviour this replaces.
 * `CREATE MATERIALIZED VIEW` and `CREATE DICTIONARY` also accept `ON CLUSTER` but are
 * deliberately NOT rewritten here: extending the set changes what existing migrations
 * emit, so it should be an explicit decision rather than a side effect of this fix.
 */
export function applyOnCluster(sql: string, cluster: string): string {
  // A DDL head: the keywords, an optional existence guard, then a possibly-qualified,
  // possibly-quoted table name. Sticky so it can be anchored at the current index.
  const NAME = '(?:`[^`]*`|"[^"]*"|[A-Za-z_][A-Za-z0-9_$]*)'
  const HEAD = new RegExp(
    `(?:create\\s+table\\s+(?:if\\s+not\\s+exists\\s+)?` +
      `|drop\\s+table\\s+(?:if\\s+exists\\s+)?` +
      `|alter\\s+table\\s+)` +
      `${NAME}(?:\\s*\\.\\s*${NAME})?`,
    'iy',
  )
  const ALREADY_CLUSTERED = /^\s+on\s+cluster\b/i

  let out = ''
  let i = 0
  const n = sql.length
  // True while only whitespace and comments have been seen since the last `;`.
  let atStatementHead = true

  while (i < n) {
    // Literals, quoted identifiers and comments are copied through untouched. A comment
    // does not end the statement head (so `-- note` above a CREATE still rewrites it);
    // a literal does, since it is real SQL content.
    const region = readNonCode(sql, i)
    if (region) {
      out += sql.slice(i, region.end)
      if (region.kind === 'literal') atStatementHead = false
      i = region.end
      continue
    }

    const ch = sql[i]

    // Whitespace does not end the statement head.
    if (/\s/.test(ch)) {
      out += ch
      i++
      continue
    }

    if (ch === ';') {
      out += ch
      i++
      atStatementHead = true
      continue
    }

    if (atStatementHead) {
      HEAD.lastIndex = i
      const match = HEAD.exec(sql)
      if (match) {
        out += match[0]
        i += match[0].length
        if (!ALREADY_CLUSTERED.test(sql.slice(i))) {
          out += ` ON CLUSTER ${cluster}`
        }
        atStatementHead = false
        continue
      }
    }

    atStatementHead = false
    out += ch
    i++
  }

  return out
}
