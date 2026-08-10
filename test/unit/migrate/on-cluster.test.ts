/**
 * ON CLUSTER placement tests.
 *
 * Before 3.0.0 the clause was inserted *before* the table name, producing
 * `CREATE TABLE ON CLUSTER c t (...)`, which ClickHouse rejects with
 * SYNTAX_ERROR (code 62) at the `CLUSTER` token — so the feature never worked.
 * The correct grammar puts it after the (optionally qualified) name.
 *
 * These assert exact emitted SQL, so they double as a record of that grammar.
 */

import { describe, it, expect } from '@jest/globals'
import { applyOnCluster } from '../../../src/migrate/sql-statements'

const C = 'prod_cluster'

describe('applyOnCluster', () => {
  describe('placement', () => {
    it('places ON CLUSTER after the table name in CREATE TABLE', () => {
      expect(applyOnCluster('CREATE TABLE t (a String) ENGINE = Memory', C)).toBe(
        'CREATE TABLE t ON CLUSTER prod_cluster (a String) ENGINE = Memory',
      )
    })

    it('places ON CLUSTER after the table name in DROP TABLE', () => {
      expect(applyOnCluster('DROP TABLE t', C)).toBe('DROP TABLE t ON CLUSTER prod_cluster')
    })

    it('places ON CLUSTER after the table name in ALTER TABLE', () => {
      expect(applyOnCluster('ALTER TABLE t ADD COLUMN b UInt8', C)).toBe(
        'ALTER TABLE t ON CLUSTER prod_cluster ADD COLUMN b UInt8',
      )
    })

    it('keeps IF NOT EXISTS with the name, not the cluster', () => {
      expect(applyOnCluster('CREATE TABLE IF NOT EXISTS t (a String) ENGINE = Memory', C)).toBe(
        'CREATE TABLE IF NOT EXISTS t ON CLUSTER prod_cluster (a String) ENGINE = Memory',
      )
    })

    it('handles IF EXISTS on DROP TABLE', () => {
      expect(applyOnCluster('DROP TABLE IF EXISTS t', C)).toBe('DROP TABLE IF EXISTS t ON CLUSTER prod_cluster')
    })

    it('handles a qualified db.table name', () => {
      expect(applyOnCluster('CREATE TABLE IF NOT EXISTS db.t (a String) ENGINE = Memory', C)).toBe(
        'CREATE TABLE IF NOT EXISTS db.t ON CLUSTER prod_cluster (a String) ENGINE = Memory',
      )
    })

    it('handles a backtick-quoted qualified name', () => {
      expect(applyOnCluster('DROP TABLE `my-db`.`my-table`', C)).toBe(
        'DROP TABLE `my-db`.`my-table` ON CLUSTER prod_cluster',
      )
    })

    it('is case-insensitive about the keywords and preserves their original casing', () => {
      expect(applyOnCluster('create table if not exists t (a String) ENGINE = Memory', C)).toBe(
        'create table if not exists t ON CLUSTER prod_cluster (a String) ENGINE = Memory',
      )
    })
  })

  describe('does not double-apply', () => {
    it('leaves a statement that already has ON CLUSTER alone', () => {
      const sql = 'CREATE TABLE t ON CLUSTER other (a String) ENGINE = Memory'
      expect(applyOnCluster(sql, C)).toBe(sql)
    })

    it('does not cascade over text it inserted itself', () => {
      // The old implementation rewrote its own output on the next pass.
      const once = applyOnCluster('CREATE TABLE t (a String) ENGINE = Memory', C)
      expect(applyOnCluster(once, C)).toBe(once)
    })
  })

  describe('comments and string literals are left alone', () => {
    it('ignores a line comment containing CREATE TABLE', () => {
      const sql = '-- CREATE TABLE foo\nCREATE TABLE t (a String) ENGINE = Memory'
      expect(applyOnCluster(sql, C)).toBe(
        '-- CREATE TABLE foo\nCREATE TABLE t ON CLUSTER prod_cluster (a String) ENGINE = Memory',
      )
    })

    it('ignores a block comment containing DROP TABLE', () => {
      const sql = '/* DROP TABLE x */ DROP TABLE t'
      expect(applyOnCluster(sql, C)).toBe('/* DROP TABLE x */ DROP TABLE t ON CLUSTER prod_cluster')
    })

    it('ignores a string literal containing DROP TABLE', () => {
      const sql = "CREATE TABLE t (a String DEFAULT 'DROP TABLE x') ENGINE = Memory"
      expect(applyOnCluster(sql, C)).toBe(
        "CREATE TABLE t ON CLUSTER prod_cluster (a String DEFAULT 'DROP TABLE x') ENGINE = Memory",
      )
    })

    it('ignores CREATE TABLE appearing mid-statement rather than at a statement head', () => {
      const sql = "INSERT INTO log VALUES ('CREATE TABLE x')"
      expect(applyOnCluster(sql, C)).toBe(sql)
    })

    it('does not treat a semicolon inside a literal as a statement boundary', () => {
      // Seed data that happens to contain SQL text. If literals were not skipped, the
      // `;` would start a new "statement" and the DDL text inside the string would be
      // rewritten — corrupting the value that gets inserted.
      const sql = "INSERT INTO log VALUES ('a; CREATE TABLE evil (x String)')"
      expect(applyOnCluster(sql, C)).toBe(sql)
    })

    it('does not treat a semicolon inside a quoted identifier as a boundary', () => {
      const sql = 'CREATE TABLE `weird;name` (a String) ENGINE = Memory'
      expect(applyOnCluster(sql, C)).toBe(
        'CREATE TABLE `weird;name` ON CLUSTER prod_cluster (a String) ENGINE = Memory',
      )
    })
  })

  describe('multi-statement scripts', () => {
    it('rewrites every top-level statement head', () => {
      const sql = 'CREATE TABLE a (x String) ENGINE = Memory; DROP TABLE b; ALTER TABLE c ADD COLUMN d UInt8'
      expect(applyOnCluster(sql, C)).toBe(
        'CREATE TABLE a ON CLUSTER prod_cluster (x String) ENGINE = Memory; ' +
          'DROP TABLE b ON CLUSTER prod_cluster; ' +
          'ALTER TABLE c ON CLUSTER prod_cluster ADD COLUMN d UInt8',
      )
    })

    it('leaves non-DDL statements untouched', () => {
      const sql = 'CREATE TABLE a (x String) ENGINE = Memory; INSERT INTO a VALUES (1)'
      expect(applyOnCluster(sql, C)).toBe(
        'CREATE TABLE a ON CLUSTER prod_cluster (x String) ENGINE = Memory; INSERT INTO a VALUES (1)',
      )
    })
  })

  describe('statement kinds that are deliberately not rewritten', () => {
    it('leaves MATERIALIZED VIEW and DICTIONARY alone', () => {
      const mv = 'CREATE MATERIALIZED VIEW mv ENGINE = Memory AS SELECT 1'
      const dict = 'CREATE DICTIONARY d (id UInt64) PRIMARY KEY id'
      expect(applyOnCluster(mv, C)).toBe(mv)
      expect(applyOnCluster(dict, C)).toBe(dict)
    })
  })
})
