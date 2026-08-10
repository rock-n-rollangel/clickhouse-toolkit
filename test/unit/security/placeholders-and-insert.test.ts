/**
 * Two blind-substitution bugs of the same shape as the ON CLUSTER one: SQL rewritten by
 * a regex that had no idea what a string literal or a comment was.
 *
 *  1. injectValues replaced every `?`, including ones inside literals and comments. When
 *     the counts happened to line up nothing was raised — the literal was corrupted and
 *     every later value bound one position to the left. validatePlaceholders counted the
 *     same wrong way, so it agreed with the bug instead of catching it.
 *  2. QueryRunner.insert() passed `table` and `columns` to @clickhouse/client, which
 *     interpolates both into `INSERT INTO … ( … ) FORMAT …` unquoted and unvalidated —
 *     while the builder's INSERT path had run them through quoteIdentifier all along.
 */

import { describe, it, expect } from '@jest/globals'
import { ClickHouseValueFormatter } from '../../../src/render/value-formatter'
import { QueryRunner } from '../../../src/runner/query-runner'

describe('placeholder substitution ignores literals and comments', () => {
  const formatter = new ClickHouseValueFormatter()

  describe('injectValues', () => {
    it('does not bind a value to a ? inside a string literal', () => {
      // THE SILENT CASE. Before the fix this returned
      //   SELECT * FROM t WHERE label = 'why42' AND id = 'bob'
      // - the literal corrupted, and 'bob' bound to id instead of 42. Nothing thrown.
      const sql = "SELECT * FROM t WHERE label = 'why?' AND id = ?"

      expect(formatter.injectValues(sql, [42, 'bob'])).toBe("SELECT * FROM t WHERE label = 'why?' AND id = 42")
    })

    it('binds every real slot correctly when a literal ? is present', () => {
      // Before the fix this threw "Not enough values provided" - the lucky, loud case.
      const sql = "SELECT * FROM t WHERE label = 'why?' AND id = ? AND name = ?"

      expect(formatter.injectValues(sql, [42, 'bob'])).toBe(
        "SELECT * FROM t WHERE label = 'why?' AND id = 42 AND name = 'bob'",
      )
    })

    it('ignores a ? inside a line comment', () => {
      expect(formatter.injectValues('SELECT ? -- what?\n, ?', [1, 2])).toBe('SELECT 1 -- what?\n, 2')
    })

    it('ignores a ? inside a block comment', () => {
      expect(formatter.injectValues('SELECT /* how? */ ?', [1])).toBe('SELECT /* how? */ 1')
    })

    it('ignores a ? inside a quoted identifier', () => {
      expect(formatter.injectValues('SELECT `we?ird`, ?', [7])).toBe('SELECT `we?ird`, 7')
    })

    it('ignores a ? after an escaped quote inside a literal', () => {
      expect(formatter.injectValues("SELECT 'it\\'s ok?', ?", [7])).toBe("SELECT 'it\\'s ok?', 7")
    })

    it('ignores a ? after a doubled quote inside a literal', () => {
      expect(formatter.injectValues("SELECT 'it''s ok?', ?", [7])).toBe("SELECT 'it''s ok?', 7")
    })

    it('still throws when there are genuinely too few values', () => {
      expect(() => formatter.injectValues('SELECT ?, ?', [1])).toThrow(/Not enough values provided/)
    })
  })

  describe('validatePlaceholders counts the same way', () => {
    it('does not count a ? inside a literal', () => {
      // Previously threw: it counted 2 placeholders where there is only one slot.
      expect(() =>
        formatter.validatePlaceholders("SELECT * FROM t WHERE label = 'why?' AND id = ?", [42]),
      ).not.toThrow()
    })

    it('does not count a ? inside a comment', () => {
      expect(() => formatter.validatePlaceholders('SELECT ? -- really?', [1])).not.toThrow()
    })

    it('still reports a genuine mismatch', () => {
      expect(() => formatter.validatePlaceholders('SELECT ?, ?', [1])).toThrow(/Parameter count mismatch/)
      expect(() => formatter.validatePlaceholders('SELECT ?', [1, 2])).toThrow(/Parameter count mismatch/)
    })
  })
})

describe('QueryRunner.insert validates table and columns', () => {
  // Validation happens before any network call, so an unreachable URL is fine here.
  const runner = new QueryRunner({
    url: 'http://127.0.0.1:1',
    username: 'u',
    password: 'p',
    database: 'd',
  })

  it('rejects an injected table name', async () => {
    await expect(runner.insert({ table: 't (x) SELECT * FROM secrets --', values: [] })).rejects.toThrow(
      /Invalid insert table/,
    )
  })

  it('rejects an injected column name', async () => {
    await expect(runner.insert({ table: 't', columns: ['a) SELECT * FROM secrets --'], values: [] })).rejects.toThrow(
      /Invalid insert column/,
    )
  })

  it('rejects a hyphenated name, which the client would not quote', async () => {
    await expect(runner.insert({ table: 'user-profiles', values: [] })).rejects.toThrow(/Invalid insert table/)
  })

  it('accepts a plain and a qualified table name with columns', async () => {
    // Passes validation; whether the insert then succeeds is a transport concern.
    await expect(runner.insert({ table: 'db.t', columns: ['a', 'b'], values: [] })).resolves.toBeUndefined()
  })
})
