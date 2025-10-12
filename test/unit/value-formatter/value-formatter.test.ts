/**
 * Value Formatter Tests
 * Comprehensive tests for value-formatter utility functions
 */

import { describe, it, expect } from '@jest/globals'
import {
  ClickHouseValueFormatter,
  formatValue,
  injectValues,
  escapeString,
  valueFormatter,
} from '../../../src/core/value-formatter'

describe('ClickHouseValueFormatter', () => {
  let formatter: ClickHouseValueFormatter

  beforeEach(() => {
    formatter = new ClickHouseValueFormatter()
  })

  describe('formatString', () => {
    it('should format simple string', () => {
      const result = formatter.formatString('hello world')
      expect(result).toBe("'hello world'")
    })

    it('should escape single quotes', () => {
      const result = formatter.formatString("O'Connor")
      expect(result).toBe("'O''Connor'")
    })

    it('should handle multiple single quotes', () => {
      const result = formatter.formatString("It's O'Connor's book")
      expect(result).toBe("'It''s O''Connor''s book'")
    })

    it('should handle empty string', () => {
      const result = formatter.formatString('')
      expect(result).toBe("''")
    })

    it('should handle null string', () => {
      const result = formatter.formatString(null as any)
      expect(result).toBe('NULL')
    })

    it('should handle undefined string', () => {
      const result = formatter.formatString(undefined as any)
      expect(result).toBe('NULL')
    })

    it('should handle strings with special characters', () => {
      const result = formatter.formatString('test@example.com')
      expect(result).toBe("'test@example.com'")
    })

    it('should handle unicode strings', () => {
      const result = formatter.formatString('Hello 世界')
      expect(result).toBe("'Hello 世界'")
    })
  })

  describe('formatNumber', () => {
    it('should format positive integers', () => {
      const result = formatter.formatNumber(42)
      expect(result).toBe('42')
    })

    it('should format negative integers', () => {
      const result = formatter.formatNumber(-42)
      expect(result).toBe('-42')
    })

    it('should format positive floats', () => {
      const result = formatter.formatNumber(3.14)
      expect(result).toBe('3.14')
    })

    it('should format negative floats', () => {
      const result = formatter.formatNumber(-3.14)
      expect(result).toBe('-3.14')
    })

    it('should format zero', () => {
      const result = formatter.formatNumber(0)
      expect(result).toBe('0')
    })

    it('should format very large numbers', () => {
      const result = formatter.formatNumber(1e10)
      expect(result).toBe('10000000000')
    })

    it('should format very small numbers', () => {
      const result = formatter.formatNumber(1e-10)
      expect(result).toBe('1e-10')
    })

    it('should handle Infinity', () => {
      const result = formatter.formatNumber(Infinity)
      expect(result).toBe("'inf'")
    })

    it('should handle -Infinity', () => {
      const result = formatter.formatNumber(-Infinity)
      expect(result).toBe("'-inf'")
    })

    it('should handle NaN', () => {
      const result = formatter.formatNumber(NaN)
      expect(result).toBe('NULL')
    })

    it('should handle null number', () => {
      const result = formatter.formatNumber(null as any)
      expect(result).toBe('NULL')
    })

    it('should handle undefined number', () => {
      const result = formatter.formatNumber(undefined as any)
      expect(result).toBe('NULL')
    })
  })

  describe('formatDate', () => {
    it('should format valid date', () => {
      const date = new Date('2023-01-01T12:30:45Z')
      const result = formatter.formatDate(date)
      expect(result).toBe("'2023-01-01 12:30:45'")
    })

    it('should format date with milliseconds', () => {
      const date = new Date('2023-01-01T12:30:45.123Z')
      const result = formatter.formatDate(date)
      expect(result).toBe("'2023-01-01 12:30:45'")
    })

    it('should format date at midnight', () => {
      const date = new Date('2023-01-01T00:00:00Z')
      const result = formatter.formatDate(date)
      expect(result).toBe("'2023-01-01 00:00:00'")
    })

    it('should handle null date', () => {
      const result = formatter.formatDate(null as any)
      expect(result).toBe('NULL')
    })

    it('should handle undefined date', () => {
      const result = formatter.formatDate(undefined as any)
      expect(result).toBe('NULL')
    })

    it('should throw error for invalid date', () => {
      const invalidDate = new Date('invalid')
      expect(() => formatter.formatDate(invalidDate)).toThrow('Invalid date value: Invalid Date')
    })

    it('should throw error for non-date object', () => {
      expect(() => formatter.formatDate('not a date' as any)).toThrow('Invalid date value: not a date')
    })
  })

  describe('formatArray', () => {
    it('should format array of strings', () => {
      const result = formatter.formatArray(['a', 'b', 'c'])
      expect(result).toBe("['a', 'b', 'c']")
    })

    it('should format array of numbers', () => {
      const result = formatter.formatArray([1, 2, 3])
      expect(result).toBe('[1, 2, 3]')
    })

    it('should format array of booleans', () => {
      const result = formatter.formatArray([true, false, true])
      expect(result).toBe('[true, false, true]')
    })

    it('should format mixed type array', () => {
      const result = formatter.formatArray([1, 'hello', true, null])
      expect(result).toBe("[1, 'hello', true, NULL]")
    })

    it('should format nested arrays', () => {
      const result = formatter.formatArray([
        [1, 2],
        [3, 4],
      ])
      expect(result).toBe('[[1, 2], [3, 4]]')
    })

    it('should format empty array', () => {
      const result = formatter.formatArray([])
      expect(result).toBe('[]')
    })

    it('should handle null array', () => {
      const result = formatter.formatArray(null as any)
      expect(result).toBe('NULL')
    })

    it('should handle undefined array', () => {
      const result = formatter.formatArray(undefined as any)
      expect(result).toBe('NULL')
    })

    it('should throw error for non-array', () => {
      expect(() => formatter.formatArray('not an array' as any)).toThrow('Expected array, got: string')
    })

    it('should format array with special characters', () => {
      const result = formatter.formatArray(["O'Connor", "Jane's"])
      expect(result).toBe("['O''Connor', 'Jane''s']")
    })
  })

  describe('formatBoolean', () => {
    it('should format true as 1', () => {
      const result = formatter.formatBoolean(true)
      expect(result).toBe('true')
    })

    it('should format false as 0', () => {
      const result = formatter.formatBoolean(false)
      expect(result).toBe('false')
    })

    it('should handle null boolean', () => {
      const result = formatter.formatBoolean(null as any)
      expect(result).toBe('NULL')
    })

    it('should handle undefined boolean', () => {
      const result = formatter.formatBoolean(undefined as any)
      expect(result).toBe('NULL')
    })
  })

  describe('formatNull', () => {
    it('should format null as NULL', () => {
      const result = formatter.formatNull()
      expect(result).toBe('NULL')
    })
  })

  describe('formatValue', () => {
    it('should format string value', () => {
      const result = formatter.formatValue('hello')
      expect(result).toBe("'hello'")
    })

    it('should format number value', () => {
      const result = formatter.formatValue(42)
      expect(result).toBe('42')
    })

    it('should format boolean value', () => {
      const result = formatter.formatValue(true)
      expect(result).toBe('true')
    })

    it('should format date value', () => {
      const date = new Date('2023-01-01T12:00:00Z')
      const result = formatter.formatValue(date)
      expect(result).toBe("'2023-01-01 12:00:00'")
    })

    it('should format array value', () => {
      const result = formatter.formatValue([1, 2, 3])
      expect(result).toBe('[1, 2, 3]')
    })

    it('should format null value', () => {
      const result = formatter.formatValue(null)
      expect(result).toBe('NULL')
    })

    it('should format undefined value', () => {
      const result = formatter.formatValue(undefined)
      expect(result).toBe('NULL')
    })

    it('should format object as ClickHouse Map', () => {
      const obj = { name: 'John', age: 30 }
      const result = formatter.formatValue(obj)
      expect(result).toBe("{'name': 'John', 'age': 30}")
    })

    it('should handle object with single quotes in Map format', () => {
      const obj = { name: "O'Connor" }
      const result = formatter.formatValue(obj)
      expect(result).toBe("{'name': 'O''Connor'}")
    })

    it('should fallback to string for unknown types', () => {
      const result = formatter.formatValue(Symbol('test'))
      expect(result).toBe("'Symbol(test)'")
    })
  })

  describe('injectValues', () => {
    it('should inject values into SQL with placeholders', () => {
      const sql = 'SELECT * FROM users WHERE id = ? AND name = ?'
      const values = [1, 'John']
      const result = formatter.injectValues(sql, values)
      expect(result).toBe("SELECT * FROM users WHERE id = 1 AND name = 'John'")
    })

    it('should handle single placeholder', () => {
      const sql = 'SELECT * FROM users WHERE id = ?'
      const values = [42]
      const result = formatter.injectValues(sql, values)
      expect(result).toBe('SELECT * FROM users WHERE id = 42')
    })

    it('should handle multiple placeholders', () => {
      const sql = 'INSERT INTO users (id, name, age, active) VALUES (?, ?, ?, ?)'
      const values = [1, 'John', 30, true]
      const result = formatter.injectValues(sql, values)
      expect(result).toBe("INSERT INTO users (id, name, age, active) VALUES (1, 'John', 30, true)")
    })

    it('should handle no placeholders', () => {
      const sql = 'SELECT * FROM users'
      const values = []
      const result = formatter.injectValues(sql, values)
      expect(result).toBe('SELECT * FROM users')
    })

    it('should handle null values', () => {
      const sql = 'SELECT * FROM users WHERE deleted_at = ?'
      const values = [null]
      const result = formatter.injectValues(sql, values)
      expect(result).toBe('SELECT * FROM users WHERE deleted_at = NULL')
    })

    it('should handle array values', () => {
      const sql = 'SELECT * FROM users WHERE id IN ?'
      const values = [[1, 2, 3]]
      const result = formatter.injectValues(sql, values)
      expect(result).toBe('SELECT * FROM users WHERE id IN [1, 2, 3]')
    })

    it('should handle date values', () => {
      const sql = 'SELECT * FROM users WHERE created_at > ?'
      const date = new Date('2023-01-01T00:00:00Z')
      const values = [date]
      const result = formatter.injectValues(sql, values)
      expect(result).toBe("SELECT * FROM users WHERE created_at > '2023-01-01 00:00:00'")
    })

    it('should throw error when not enough values', () => {
      const sql = 'SELECT * FROM users WHERE id = ? AND name = ?'
      const values = [1]
      expect(() => formatter.injectValues(sql, values)).toThrow('Not enough values provided. Expected 2, got 1')
    })

    it('should handle empty values array', () => {
      const sql = 'SELECT * FROM users'
      const values: any[] = []
      const result = formatter.injectValues(sql, values)
      expect(result).toBe('SELECT * FROM users')
    })

    it('should handle null values array', () => {
      const sql = 'SELECT * FROM users'
      const values: any[] = null as any
      const result = formatter.injectValues(sql, values)
      expect(result).toBe('SELECT * FROM users')
    })
  })

  describe('validatePlaceholders', () => {
    it('should pass validation for correct number of placeholders', () => {
      const sql = 'SELECT * FROM users WHERE id = ? AND name = ?'
      const values = [1, 'John']
      expect(() => formatter.validatePlaceholders(sql, values)).not.toThrow()
    })

    it('should throw error for too few values', () => {
      const sql = 'SELECT * FROM users WHERE id = ? AND name = ?'
      const values = [1]
      expect(() => formatter.validatePlaceholders(sql, values)).toThrow(
        'Parameter count mismatch. SQL has 2 placeholders, but 1 values provided',
      )
    })

    it('should throw error for too many values', () => {
      const sql = 'SELECT * FROM users WHERE id = ?'
      const values = [1, 'John']
      expect(() => formatter.validatePlaceholders(sql, values)).toThrow(
        'Parameter count mismatch. SQL has 1 placeholders, but 2 values provided',
      )
    })

    it('should pass validation for no placeholders', () => {
      const sql = 'SELECT * FROM users'
      const values: any[] = []
      expect(() => formatter.validatePlaceholders(sql, values)).not.toThrow()
    })

    it('should handle multiple occurrences of same placeholder', () => {
      const sql = 'SELECT * FROM users WHERE id = ? OR parent_id = ?'
      const values = [1, 1]
      expect(() => formatter.validatePlaceholders(sql, values)).not.toThrow()
    })
  })
})

describe('Utility Functions', () => {
  describe('formatValue', () => {
    it('should use singleton formatter', () => {
      const result = formatValue('test')
      expect(result).toBe("'test'")
    })

    it('should handle different types', () => {
      expect(formatValue(42)).toBe('42')
      expect(formatValue(true)).toBe('true')
      expect(formatValue(null)).toBe('NULL')
    })
  })

  describe('injectValues', () => {
    it('should use singleton formatter', () => {
      const sql = 'SELECT * FROM users WHERE id = ?'
      const values = [1]
      const result = injectValues(sql, values)
      expect(result).toBe('SELECT * FROM users WHERE id = 1')
    })
  })

  describe('escapeString', () => {
    it('should escape single quotes', () => {
      const result = escapeString("O'Connor")
      expect(result).toBe("O''Connor")
    })

    it('should handle multiple single quotes', () => {
      const result = escapeString("It's O'Connor's book")
      expect(result).toBe("It''s O''Connor''s book")
    })

    it('should handle no single quotes', () => {
      const result = escapeString('hello world')
      expect(result).toBe('hello world')
    })

    it('should handle empty string', () => {
      const result = escapeString('')
      expect(result).toBe('')
    })
  })

  describe('valueFormatter singleton', () => {
    it('should be instance of ClickHouseValueFormatter', () => {
      expect(valueFormatter).toBeInstanceOf(ClickHouseValueFormatter)
    })

    it('should work consistently', () => {
      const result1 = valueFormatter.formatValue('test')
      const result2 = valueFormatter.formatValue('test')
      expect(result1).toBe(result2)
      expect(result1).toBe("'test'")
    })
  })
})
