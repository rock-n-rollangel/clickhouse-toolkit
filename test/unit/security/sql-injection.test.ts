/**
 * SQL injection regression tests
 *
 * These use the exact payloads that were verified against a live ClickHouse before
 * 3.0.0, not toy strings:
 *
 *  1. A caller-controlled "column name" containing '(' was returned from
 *     quoteIdentifier completely unquoted and unvalidated, so a measure key could
 *     smuggle a whole subquery into the SELECT list. The injected subquery ignored
 *     the outer query's row-level scoping and leaked every tenant's data.
 *  2. formatString escaped ' but not '\', so a value ending in a backslash consumed
 *     its own closing quote and let the *next* value break out of its literal.
 */

import { describe, it, expect } from '@jest/globals'
import { select, update, deleteFrom, Eq, Raw, Sum, Count, Avg } from '../../../src/index'
import { ClickHouseValueFormatter, ValueFormatter } from '../../../src/render/value-formatter'

// The measure key that leaked 31 tenants' data through a query scoped to one.
const LEAK_PAYLOAD = "v, (SELECT groupArray(concat(node_id,'=',toString(value))) FROM events) AS leak"

/**
 * Scan rendered SQL and return the parts that sit *outside* any string literal,
 * decoding ClickHouse literal syntax ('' for a quote, \\ for a backslash).
 * If a payload can be found in here, it escaped its literal and became SQL.
 */
function sqlOutsideLiterals(sql: string): string {
  let out = ''
  let inLiteral = false

  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i]

    if (!inLiteral) {
      if (ch === "'") {
        inLiteral = true
      } else {
        out += ch
      }
      continue
    }

    // Inside a literal: consume escapes so they cannot be mistaken for a terminator
    if (ch === '\\') {
      i++ // skip the escaped character
      continue
    }
    if (ch === "'") {
      if (sql[i + 1] === "'") {
        i++ // doubled quote is an escaped quote, still inside
        continue
      }
      inLiteral = false
    }
  }

  expect(inLiteral).toBe(false) // every literal must be closed
  return out
}

describe('SQL injection regressions', () => {
  describe('quoteIdentifier rejects author-supplied SQL', () => {
    it('rejects the injected measure key used as a column', () => {
      const query = select([LEAK_PAYLOAD]).from('events')

      expect(() => query.toSQL()).toThrow(/is not a valid identifier/)
    })

    it('rejects the injected measure key used as an alias', () => {
      const query = select({ [LEAK_PAYLOAD]: 'value' }).from('events')

      expect(() => query.toSQL()).toThrow(/is not a valid identifier/)
    })

    it('names Raw() as the supported way to pass SQL', () => {
      const query = select([LEAK_PAYLOAD]).from('events')

      expect(() => query.toSQL()).toThrow(/Raw\(/)
    })

    it('rejects an ORDER BY column containing a parenthesis', () => {
      const query = select(['id'])
        .from('events')
        .orderBy([{ column: 'sum(value)', direction: 'DESC' }])

      expect(() => query.toSQL()).toThrow(/is not a valid identifier/)
    })

    it('rejects a GROUP BY key containing a parenthesis', () => {
      const query = select(['id']).from('events').groupBy(['toDate(created_at)'])

      expect(() => query.toSQL()).toThrow(/is not a valid identifier/)
    })

    it('rejects an identifier carrying a backtick, which would close the quoting', () => {
      const query = select(['id']).from('events').groupBy(['a`, (SELECT 1) AS `b'])

      expect(() => query.toSQL()).toThrow(/is not a valid identifier/)
    })
  })

  describe('GROUP BY / ORDER BY relocate the capability to Raw()', () => {
    // 3.0.0 rejects a bare 'toDate(created_at)' string, so both clauses gained a
    // RawExpr slot. The capability moves to an explicit API; it is not removed.
    it('groups by an expression passed through Raw()', () => {
      const { sql } = select(['id'])
        .from('events')
        .groupBy([Raw('toDate(created_at)')])
        .toSQL()

      expect(sql).toBe('SELECT `id` FROM `events` GROUP BY toDate(created_at)')
    })

    it('orders by an expression passed through Raw()', () => {
      const { sql } = select(['id'])
        .from('events')
        .orderBy([{ column: Raw('sum(value)'), direction: 'DESC' }])
        .toSQL()

      expect(sql).toBe('SELECT `id` FROM `events` ORDER BY sum(value) DESC')
    })

    it('mixes identifier and Raw keys in one clause', () => {
      const { sql } = select(['id'])
        .from('events')
        .groupBy(['node_id', Raw('toDate(created_at)')])
        .orderBy([
          { column: Raw('sum(value)'), direction: 'DESC' },
          { column: 'node_id', direction: 'ASC' },
        ])
        .toSQL()

      expect(sql).toBe(
        'SELECT `id` FROM `events` GROUP BY `node_id`, toDate(created_at) ' + 'ORDER BY sum(value) DESC, `node_id` ASC',
      )
    })

    it('still validates the plain-string arm of both clauses', () => {
      expect(() => select(['id']).from('events').groupBy(['node_id', 'toDate(created_at)']).toSQL()).toThrow(
        /is not a valid identifier/,
      )
      expect(() =>
        select(['id'])
          .from('events')
          .orderBy([{ column: 'sum(value)', direction: 'DESC' }])
          .toSQL(),
      ).toThrow(/is not a valid identifier/)
    })
  })

  describe('function arguments are quoted, not trusted', () => {
    // Column names passed as function arguments used to be returned unquoted, so the
    // typed helpers - the path the docs steer callers to - were themselves an injection
    // vector. This is the payload verified through the built library.
    const FN_ARG_PAYLOAD = 'x) , (SELECT groupArray(node_id) FROM events'

    it('rejects the injected column name passed through Sum()', () => {
      const query = select({ leak: Sum(FN_ARG_PAYLOAD) }).from('events')

      expect(() => query.toSQL()).toThrow(/is not a valid identifier/)
    })

    it('rejects it through the other aggregate helpers too', () => {
      expect(() =>
        select([Count(FN_ARG_PAYLOAD)])
          .from('events')
          .toSQL(),
      ).toThrow(/is not a valid identifier/)
      expect(() =>
        select([Avg(FN_ARG_PAYLOAD)])
          .from('events')
          .toSQL(),
      ).toThrow(/is not a valid identifier/)
    })

    it('quotes ordinary column arguments instead of inlining them', () => {
      const { sql } = select({ total: Sum('amount') })
        .from('events')
        .toSQL()

      expect(sql).toBe('SELECT sum(`amount`) AS `total` FROM `events`')
    })

    it('still renders count(*), whose argument is a star and not an identifier', () => {
      const { sql } = select([Count()]).from('events').toSQL()

      expect(sql).toBe('SELECT count(*) FROM `events`')
    })

    it('still allows a deliberate expression argument through Raw()', () => {
      const { sql } = select({ total: Sum(Raw('if(ok, value, 0)')) })
        .from('events')
        .toSQL()

      expect(sql).toBe('SELECT sum(if(ok, value, 0)) AS `total` FROM `events`')
    })

    it('still accepts SQL passed explicitly through Raw()', () => {
      const { sql } = select(['id', Raw('sum(value)')])
        .from('events')
        .toSQL()

      expect(sql).toBe('SELECT `id`, sum(value) FROM `events`')
    })
  })

  describe('quoteIdentifier preserves the non-plain identifiers it always supported', () => {
    it('supports the star selector', () => {
      const { sql } = select(['*']).from('events').toSQL()

      expect(sql).toBe('SELECT * FROM `events`')
    })

    it('supports a numeric literal, for SELECT 1 FROM ...', () => {
      const { sql } = select(['1']).from('events').toSQL()

      expect(sql).toBe('SELECT 1 FROM `events`')
    })

    it('supports table.column', () => {
      const { sql } = select(['events.node_id']).from('events').toSQL()

      expect(sql).toBe('SELECT `events`.`node_id` FROM `events`')
    })

    it('supports hyphenated identifiers, which backticks quote safely', () => {
      const { sql } = select(['id']).from('user-profiles').toSQL()

      expect(sql).toBe('SELECT `id` FROM `user-profiles`')
    })
  })

  describe('formatString escapes backslashes', () => {
    const formatter: ValueFormatter = new ClickHouseValueFormatter()

    it('renders a trailing backslash so the literal cannot be broken out of', () => {
      // 'x\' in ClickHouse terms - the value is x followed by one backslash
      expect(formatter.formatString('x\\')).toBe("'x\\\\'")
    })

    it('round-trips a backslash as the value, not as an escape', () => {
      // A correctness bug as much as a security one: 'a\b' was read back by
      // ClickHouse as "a" + backspace, silently corrupting the value.
      expect(formatter.formatString('a\\b')).toBe("'a\\\\b'")
      expect(sqlOutsideLiterals(formatter.formatString('a\\b'))).toBe('')
    })

    it('still escapes single quotes', () => {
      expect(formatter.formatString("O'Connor")).toBe("'O''Connor'")
    })

    it('escapes backslashes before quotes, so neither mangles the other', () => {
      expect(formatter.formatString("\\'")).toBe("'\\\\'''")
    })

    it('does not let a trailing backslash release the following value', () => {
      // The live breakout: SELECT ... WHERE node_status = 'x\' AND device_status = ' OR 1=1 --'
      const query = select(['id'])
        .from('events')
        .where({ node_status: Eq('x\\'), device_status: Eq(' OR 1=1 --') })

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id` FROM `events` WHERE `node_status` = 'x\\\\' AND `device_status` = ' OR 1=1 --'")

      // The payload stays data: nothing of it survives outside a string literal
      const outside = sqlOutsideLiterals(sql)
      expect(outside).not.toContain('OR 1=1')
      expect(outside).not.toContain('--')
    })
  })
})

describe('narrowly-typed fields are validated at render time', () => {
  // Each of these is a field whose TYPE already declares a closed set - 'ASC' | 'DESC',
  // 'ROWS' | 'RANGE', number - which the renderer treated as a runtime guarantee.
  // Types are erased at runtime, so JSON, a query string, plain JS or a hand-built AST
  // reached these unchecked. Payloads below are the ones verified against the library.

  describe('ORDER BY direction', () => {
    // The worst of the set: mid-query, so the trailing comment swallowed the real LIMIT.
    const DIRECTION_PAYLOAD = 'ASC LIMIT 1 UNION ALL SELECT groupArray(id) FROM e --'

    it('rejects an injected direction in the top-level ORDER BY', () => {
      const query = select(['a'])
        .from('e')
        .orderBy([{ column: 'a', direction: DIRECTION_PAYLOAD as never }])
        .limit(5)

      expect(() => query.toSQL()).toThrow(/Invalid order direction/)
    })

    it('rejects an injected direction in a window ORDER BY', () => {
      const query = select({
        r: Sum('v').over({
          orderBy: [{ column: { type: 'column', name: 'a' }, direction: 'ASC) , (SELECT 1' as never }],
        }),
      }).from('e')

      expect(() => query.toSQL()).toThrow(/Invalid order direction/)
    })

    it('still renders valid directions byte-identically', () => {
      const { sql } = select(['a'])
        .from('e')
        .orderBy([
          { column: 'a', direction: 'ASC' },
          { column: 'b', direction: 'DESC' },
        ])
        .toSQL()

      expect(sql).toBe('SELECT `a` FROM `e` ORDER BY `a` ASC, `b` DESC')
    })

    it('accepts lowercase and emits the canonical spelling', () => {
      const { sql } = select(['a'])
        .from('e')
        .orderBy([{ column: 'a', direction: 'asc' as never }])
        .toSQL()

      expect(sql).toBe('SELECT `a` FROM `e` ORDER BY `a` ASC')
    })
  })

  describe('window names', () => {
    const WINDOW_PAYLOAD = 'w) AS (), evil AS ('

    it('rejects an injected WINDOW declaration name', () => {
      const query = select(['a'])
        .from('e')
        .window(WINDOW_PAYLOAD, { partitionBy: ['x'] })

      expect(() => query.toSQL()).toThrow(/Invalid window name/)
    })

    it('rejects an injected OVER name even when it was declared', () => {
      // The OVER site renders before the WINDOW declaration, so it is independently
      // reachable - "safe because another check runs first" is not a guarantee.
      const query = select({ r: Sum('v').over(WINDOW_PAYLOAD) })
        .from('e')
        .window(WINDOW_PAYLOAD, { partitionBy: ['x'] })

      expect(() => query.toSQL()).toThrow(/Invalid window name/)
    })

    it('still renders a valid named window byte-identically', () => {
      const { sql } = select({ r: Sum('v').over('w') })
        .from('e')
        .window('w', { partitionBy: ['x'] })
        .toSQL()

      expect(sql).toBe('SELECT sum(`v`) OVER w AS `r` FROM `e` WINDOW w AS (PARTITION BY `x`)')
    })
  })

  describe('SETTINGS keys', () => {
    // Injecting FORMAT changes the response format; the trailing -- truncates the rest.
    const SETTINGS_PAYLOAD = 'max_threads=1 FORMAT JSON -- '

    it('rejects an injected settings key on SELECT', () => {
      const query = select(['a'])
        .from('e')
        .settings({ [SETTINGS_PAYLOAD]: 1 })

      expect(() => query.toSQL()).toThrow(/Invalid settings key/)
    })

    it('rejects an injected settings key on UPDATE', () => {
      const query = update('e')
        .set({ a: 1 })
        .where({ id: Eq(1) })
        .settings({ [SETTINGS_PAYLOAD]: 1 })

      expect(() => query.toSQL()).toThrow(/Invalid settings key/)
    })

    it('rejects an injected settings key on DELETE', () => {
      const query = deleteFrom('e')
        .where({ id: Eq(1) })
        .settings({ [SETTINGS_PAYLOAD]: 1 })

      expect(() => query.toSQL()).toThrow(/Invalid settings key/)
    })

    it('still renders valid settings byte-identically', () => {
      const { sql } = select(['a']).from('e').settings({ max_execution_time: 30 }).toSQL()

      expect(sql).toBe('SELECT `a` FROM `e` SETTINGS max_execution_time = 30')
    })
  })

  describe('window frame type and offset', () => {
    it('rejects an injected frame type', () => {
      const query = select({
        r: Sum('v').over({ frame: { type: 'ROWS) , (SELECT 1' as never, start: 'CURRENT ROW' } }),
      }).from('e')

      expect(() => query.toSQL()).toThrow(/Invalid frame type/)
    })

    it('rejects a non-numeric frame offset', () => {
      const query = select({
        r: Sum('v').over({ frame: { type: 'ROWS', start: { preceding: '1 UNION ALL SELECT 9' as never } } }),
      }).from('e')

      expect(() => query.toSQL()).toThrow(/Invalid frame offset/)
    })

    it('rejects an unknown frame bound literal', () => {
      const query = select({
        r: Sum('v').over({ frame: { type: 'ROWS', start: 'CURRENT ROW) , (SELECT 1' as never } }),
      }).from('e')

      expect(() => query.toSQL()).toThrow(/Invalid frame bound/)
    })

    it('still renders a valid frame byte-identically', () => {
      const { sql } = select({
        r: Sum('v').over({ frame: { type: 'ROWS', start: { preceding: 5 } } }),
      })
        .from('e')
        .toSQL()

      expect(sql).toBe('SELECT sum(`v`) OVER (ROWS 5 PRECEDING) AS `r` FROM `e`')
    })
  })

  describe('formatNumber', () => {
    const formatter = new ClickHouseValueFormatter()

    it('rejects a non-number rather than relying on isNaN coercion', () => {
      expect(() => formatter.formatNumber('1 UNION ALL SELECT 9' as never)).toThrow(/Expected number/)
    })

    it('rejects a numeric-looking string that used to be emitted verbatim', () => {
      // isNaN('0x10') is false, so this rendered as the raw token 0x10
      expect(() => formatter.formatNumber('0x10' as never)).toThrow(/Expected number/)
    })

    it('still formats real numbers and the special values', () => {
      expect(formatter.formatNumber(42)).toBe('42')
      expect(formatter.formatNumber(NaN)).toBe('NULL')
      expect(formatter.formatNumber(Infinity)).toBe("'inf'")
      expect(formatter.formatNumber(null as never)).toBe('NULL')
    })
  })
})
