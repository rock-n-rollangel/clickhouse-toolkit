/**
 * Predicate Builder Tests
 * Comprehensive tests for predicate-builder utility functions
 */

import { describe, it, expect } from '@jest/globals'
import { parseColumnRef, operatorToPredicate } from '../../../src/core/predicate-builder'
import {
  Eq,
  Ne,
  Gt,
  Gte,
  Lt,
  Lte,
  In,
  NotIn,
  Between,
  Like,
  ILike,
  IsNull,
  IsNotNull,
  HasAny,
  HasAll,
  InTuple,
  EqCol,
} from '../../../src/core/operators'
import { ArrayValue, Predicate } from 'src'

describe('Predicate Builder', () => {
  describe('parseColumnRef', () => {
    it('should parse simple column name', () => {
      const result = parseColumnRef('name')

      expect(result).toEqual({
        type: 'column',
        name: 'name',
      })
    })

    it('should parse table.column reference', () => {
      const result = parseColumnRef('users.id')

      expect(result).toEqual({
        type: 'column',
        name: 'id',
        table: 'users',
      })
    })

    it('should handle column names with underscores', () => {
      const result = parseColumnRef('user_name')

      expect(result).toEqual({
        type: 'column',
        name: 'user_name',
      })
    })

    it('should handle column names with special characters', () => {
      const result = parseColumnRef('user-name')

      expect(result).toEqual({
        type: 'column',
        name: 'user-name',
      })
    })

    it('should handle empty string', () => {
      const result = parseColumnRef('')

      expect(result).toEqual({
        type: 'column',
        name: '',
      })
    })
  })

  describe('operatorToPredicate', () => {
    describe('Comparison Operators', () => {
      it('should create equality predicate', () => {
        const operator = Eq('active')
        const result = operatorToPredicate('status', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'status' },
          operator: '=',
          right: { type: 'value', value: 'active' },
        })
      })

      it('should create inequality predicate', () => {
        const operator = Ne('inactive')
        const result = operatorToPredicate('status', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'status' },
          operator: '!=',
          right: { type: 'value', value: 'inactive' },
        })
      })

      it('should create greater than predicate', () => {
        const operator = Gt(18)
        const result = operatorToPredicate('age', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'age' },
          operator: '>',
          right: { type: 'value', value: 18 },
        })
      })

      it('should create greater than or equal predicate', () => {
        const operator = Gte(18)
        const result = operatorToPredicate('age', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'age' },
          operator: '>=',
          right: { type: 'value', value: 18 },
        })
      })

      it('should create less than predicate', () => {
        const operator = Lt(65)
        const result = operatorToPredicate('age', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'age' },
          operator: '<',
          right: { type: 'value', value: 65 },
        })
      })

      it('should create less than or equal predicate', () => {
        const operator = Lte(65)
        const result = operatorToPredicate('age', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'age' },
          operator: '<=',
          right: { type: 'value', value: 65 },
        })
      })
    })

    describe('Column Comparison', () => {
      it('should create column equality predicate', () => {
        const operator = EqCol('other_table.id')
        const result = operatorToPredicate('users.id', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'id', table: 'users' },
          operator: '=',
          right: { type: 'column', name: 'id', table: 'other_table' },
        })
      })
    })

    describe('Array Operators', () => {
      it('should create IN predicate', () => {
        const operator = In([1, 2, 3, 4, 5])
        const result = operatorToPredicate('id', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'id' },
          operator: 'IN',
          right: { type: 'array', values: [1, 2, 3, 4, 5] },
        })
      })

      it('should create NOT IN predicate', () => {
        const operator = NotIn(['admin', 'superuser'])
        const result = operatorToPredicate('role', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'role' },
          operator: 'NOT IN',
          right: { type: 'array', values: ['admin', 'superuser'] },
        })
      })

      it('should create empty IN predicate', () => {
        const operator = In([])
        const result = operatorToPredicate('id', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'id' },
          operator: 'IN',
          right: { type: 'array', values: [] },
        })
      })
    })

    describe('Range Operators', () => {
      it('should create BETWEEN predicate', () => {
        const operator = Between(18, 65)
        const result = operatorToPredicate('age', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'age' },
          operator: 'BETWEEN',
          right: { type: 'tuple', values: [18, 65] },
        })
      })

      it('should create BETWEEN predicate with dates', () => {
        const startDate = new Date('2023-01-01')
        const endDate = new Date('2023-12-31')
        const operator = Between(startDate, endDate)
        const result = operatorToPredicate('created_at', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'created_at' },
          operator: 'BETWEEN',
          right: { type: 'tuple', values: [startDate, endDate] },
        })
      })
    })

    describe('Pattern Matching', () => {
      it('should create LIKE predicate', () => {
        const operator = Like('John%')
        const result = operatorToPredicate('name', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'name' },
          operator: 'LIKE',
          right: { type: 'value', value: 'John%' },
        })
      })

      it('should create ILIKE predicate', () => {
        const operator = ILike('john%')
        const result = operatorToPredicate('name', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'name' },
          operator: 'ILIKE',
          right: { type: 'value', value: 'john%' },
        })
      })
    })

    describe('Null Checking', () => {
      it('should create IS NULL predicate', () => {
        const operator = IsNull()
        const result = operatorToPredicate('deleted_at', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'deleted_at' },
          operator: 'IS NULL',
          right: { type: 'value', value: null },
        })
      })

      it('should create IS NOT NULL predicate', () => {
        const operator = IsNotNull()
        const result = operatorToPredicate('email', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'email' },
          operator: 'IS NOT NULL',
          right: { type: 'value', value: null },
        })
      })
    })

    describe('ClickHouse Array Operators', () => {
      it('should create HAS ANY predicate', () => {
        const operator = HasAny(['tag1', 'tag2'])
        const result = operatorToPredicate('tags', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'tags' },
          operator: 'HAS ANY',
          right: { type: 'array', values: ['tag1', 'tag2'] },
        })
      })

      it('should create HAS ALL predicate', () => {
        const operator = HasAll(['required_tag1', 'required_tag2'])
        const result = operatorToPredicate('tags', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: 'tags' },
          operator: 'HAS ALL',
          right: { type: 'array', values: ['required_tag1', 'required_tag2'] },
        })
      })

      it('should create IN TUPLE predicate', () => {
        const operator = InTuple([
          ['John', 'Doe'],
          ['Jane', 'Smith'],
        ])
        const result = operatorToPredicate('(first_name, last_name)', operator)

        expect(result).toEqual({
          type: 'predicate',
          left: { type: 'column', name: '(first_name, last_name)' },
          operator: 'IN TUPLE',
          right: {
            type: 'array',
            values: [
              ['John', 'Doe'],
              ['Jane', 'Smith'],
            ],
          },
        })
      })
    })

    describe('Data Types', () => {
      it('should handle string values', () => {
        const operator = Eq('test string')
        const result = operatorToPredicate('name', operator) as Predicate

        expect(result.right).toEqual({
          type: 'value',
          value: 'test string',
        })
      })

      it('should handle numeric values', () => {
        const operator = Eq(42.5)
        const result = operatorToPredicate('price', operator) as Predicate

        expect(result.right).toEqual({
          type: 'value',
          value: 42.5,
        })
      })

      it('should handle boolean values', () => {
        const operator = Eq(true)
        const result = operatorToPredicate('is_active', operator) as Predicate

        expect(result.right).toEqual({
          type: 'value',
          value: true,
        })
      })

      it('should handle null values', () => {
        const operator = Eq(null)
        const result = operatorToPredicate('optional_field', operator) as Predicate

        expect(result.right).toEqual({
          type: 'value',
          value: null,
        })
      })

      it('should handle date values', () => {
        const date = new Date('2023-01-01T00:00:00Z')
        const operator = Eq(date)
        const result = operatorToPredicate('created_at', operator) as Predicate

        expect(result.right).toEqual({
          type: 'value',
          value: date,
        })
      })
    })

    describe('Table.Column References', () => {
      it('should handle table.column in left side', () => {
        const operator = Eq('active')
        const result = operatorToPredicate('users.status', operator) as Predicate

        expect(result.left).toEqual({
          type: 'column',
          name: 'status',
          table: 'users',
        })
      })

      it('should handle table.column in EqCol right side', () => {
        const operator = EqCol('orders.user_id')
        const result = operatorToPredicate('users.id', operator) as Predicate

        expect(result.right).toEqual({
          type: 'column',
          name: 'user_id',
          table: 'orders',
        })
      })
    })

    describe('Custom parseColumnRef Function', () => {
      it('should use custom parseColumnRef function', () => {
        const customParseColumnRef = (col: string) => ({
          type: 'column' as const,
          name: col.toUpperCase(),
        })

        const operator = Eq('active')
        const result = operatorToPredicate('status', operator, customParseColumnRef) as Predicate

        expect(result.left).toEqual({
          type: 'column',
          name: 'STATUS',
        })
      })

      it('should use custom parseColumnRef for EqCol', () => {
        const customParseColumnRef = (col: string) => ({
          type: 'column' as const,
          name: col.replaceAll('.', '_'),
        })

        const operator = EqCol('other.table.id')
        const result = operatorToPredicate('users.id', operator, customParseColumnRef) as Predicate

        expect(result.right).toEqual({
          type: 'column',
          name: 'other_table_id',
        })
      })
    })

    describe('Error Handling', () => {
      it('should throw error for unsupported operator type', () => {
        const unsupportedOperator = { type: 'unsupported', value: 'test' } as any

        expect(() => operatorToPredicate('column', unsupportedOperator)).toThrow(
          'Unsupported operator type: unsupported',
        )
      })
    })

    describe('Edge Cases', () => {
      it('should handle empty column name', () => {
        const operator = Eq('value')
        const result = operatorToPredicate('', operator) as Predicate

        expect(result.left).toEqual({
          type: 'column',
          name: '',
        })
      })

      it('should handle complex column names', () => {
        const operator = Eq('value')
        const result = operatorToPredicate('table_name.column_name', operator) as Predicate

        expect(result.left).toEqual({
          type: 'column',
          name: 'column_name',
          table: 'table_name',
        })
      })

      it('should handle arrays with mixed types', () => {
        const operator = In([1, 'string', true, null])
        const result = operatorToPredicate('mixed_column', operator) as Predicate

        expect(result.right).toEqual({
          type: 'array',
          values: [1, 'string', true, null],
        })
      })

      it('should handle large arrays', () => {
        const largeArray = Array.from({ length: 1000 }, (_, i) => i)
        const operator = In(largeArray)
        const result = operatorToPredicate('id', operator) as Predicate

        expect((result.right as ArrayValue).values).toHaveLength(1000)
        expect((result.right as ArrayValue).values[0]).toBe(0)
        expect((result.right as ArrayValue).values[999]).toBe(999)
      })
    })
  })
})
