import { describe, it, expect } from '@jest/globals'
import { splitSqlStatements } from '../../../src/migrate/sql-statements'

describe('splitSqlStatements', () => {
  it('splits multiple statements on top-level semicolons', () => {
    const sql = 'DROP TABLE IF EXISTS a;\nRENAME TABLE b TO c;\nALTER TABLE d DELETE WHERE 1'
    expect(splitSqlStatements(sql)).toEqual([
      'DROP TABLE IF EXISTS a',
      'RENAME TABLE b TO c',
      'ALTER TABLE d DELETE WHERE 1',
    ])
  })

  it('returns a single statement unchanged when there is no semicolon', () => {
    expect(splitSqlStatements('CREATE TABLE x (id UInt32) ENGINE = Memory')).toEqual([
      'CREATE TABLE x (id UInt32) ENGINE = Memory',
    ])
  })

  it('drops trailing/empty statements from stray semicolons', () => {
    expect(splitSqlStatements('SELECT 1;')).toEqual(['SELECT 1'])
    expect(splitSqlStatements('SELECT 1;\n\n;  ;')).toEqual(['SELECT 1'])
  })

  it('does not split on semicolons inside single-quoted strings', () => {
    const sql = "INSERT INTO t VALUES ('a;b'); INSERT INTO t VALUES ('c;d')"
    expect(splitSqlStatements(sql)).toEqual(["INSERT INTO t VALUES ('a;b')", "INSERT INTO t VALUES ('c;d')"])
  })

  it('handles doubled-quote escapes inside string literals', () => {
    expect(splitSqlStatements("SELECT 'it''s;fine'; SELECT 2")).toEqual(["SELECT 'it''s;fine'", 'SELECT 2'])
  })

  it('does not split on semicolons inside line or block comments', () => {
    const sql = 'CREATE TABLE a (id UInt32); -- drop; me\n/* x; y */ DROP TABLE a'
    expect(splitSqlStatements(sql)).toEqual(['CREATE TABLE a (id UInt32)', '-- drop; me\n/* x; y */ DROP TABLE a'])
  })

  it('ignores comment-only and whitespace-only fragments', () => {
    expect(splitSqlStatements('SELECT 1; -- just a comment')).toEqual(['SELECT 1'])
    expect(splitSqlStatements('')).toEqual([])
    expect(splitSqlStatements('   \n  ')).toEqual([])
  })
})
