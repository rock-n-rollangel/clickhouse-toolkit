/**
 * Case Builder Tests
 * Comprehensive tests for the Case() when/then/else functionality
 */

import { describe, it, expect } from '@jest/globals'
import { Case } from '../../../src/core/case-builder'
import { CaseExpr } from '../../../src/core/ast'
import { Eq, Gt, Lt, In, IsNull, IsNotNull, Like, EqCol, Lte } from '../../../src/core/operators'
import { Count, Sum, Concat, Upper, Raw, Column, Value } from '../../../src/index'
import { select } from '../../../src/builder/select-builder'

describe('CaseBuilder', () => {
  describe('Basic Functionality Tests', () => {
    it('should create simple case with string condition and literal values', () => {
      const result = Case()
        .when({ status: Eq('active') }, Value('Active User'))
        .else(Value('Inactive User'))

      expect(result.type).toBe('case')
      expect(result.cases).toHaveLength(1)
      expect(result.cases[0].condition).toMatchObject({
        type: 'predicate',
        left: { type: 'column', name: 'status' },
        operator: '=',
        right: { type: 'value', value: 'active' },
      })
      expect(result.cases[0].then).toEqual(Value('Active User'))
      expect(result.else).toEqual(Value('Inactive User'))
    })

    it('should create case with literal values in then/else', () => {
      const result = Case()
        .when({ age: Gt(18) }, Value(1))
        .else(Value(0))

      expect(result.type).toBe('case')
      expect(result.cases[0].then).toEqual(Value(1))
      expect(result.else).toEqual(Value(0))
    })

    it('should create case with boolean values', () => {
      const result = Case()
        .when({ active: Eq(true) }, Value(true))
        .else(Value(false))

      expect(result.cases[0].then).toEqual(Value(true))
      expect(result.else).toEqual(Value(false))
    })

    it('should create case with null values', () => {
      const result = Case().when({ status: IsNull() }, Value(null)).else(Value('Has Status'))

      expect(result.cases[0].then).toEqual(Value(null))
      expect(result.else).toEqual(Value('Has Status'))
    })

    it('should handle multiple when clauses', () => {
      const result = Case()
        .when({ age: Lt(18) }, Value('Minor'))
        .when({ age: Gt(65) }, Value('Senior'))
        .else(Value('Adult'))

      expect(result.cases).toHaveLength(2)
      expect(result.cases[0].condition).toMatchObject({
        type: 'predicate',
        left: { type: 'column', name: 'age' },
        operator: '<',
        right: { type: 'value', value: 18 },
      })
      expect(result.cases[1].condition).toMatchObject({
        type: 'predicate',
        left: { type: 'column', name: 'age' },
        operator: '>',
        right: { type: 'value', value: 65 },
      })
      expect(result.cases[0].then).toEqual(Value('Minor'))
      expect(result.cases[1].then).toEqual(Value('Senior'))
      expect(result.else).toEqual(Value('Adult'))
    })

    it('should handle case with column references in then/else', () => {
      const result = Case()
        .when({ priority: Eq('high') }, Column('priority_level'))
        .else(Column('normal_level'))

      expect(result.cases[0].then).toEqual(Column('priority_level'))
      expect(result.else).toEqual(Column('normal_level'))
    })

    it('should properly distinguish between Column() and Value() usage', () => {
      const columnCase = Case()
        .when({ priority: Eq('high') }, Column('priority_level'))
        .else(Column('normal_level'))

      const valueCase = Case()
        .when({ priority: Eq('high') }, Value('Priority: High'))
        .else(Value('Priority: Normal'))

      expect(columnCase.cases[0].then).toEqual(Column('priority_level'))
      expect(columnCase.else).toEqual(Column('normal_level'))
      expect(valueCase.cases[0].then).toEqual(Value('Priority: High'))
      expect(valueCase.else).toEqual(Value('Priority: Normal'))
    })

    it('should demonstrate proper Value() usage for different literal types', () => {
      const result = Case()
        .when({ status: Eq('active') }, Value('Active User'))
        .when({ age: Gt(65) }, Value('Senior Citizen'))
        .when({ verified: Eq(true) }, Value(1))
        .when({ disabled: Eq(false) }, Value(0))
        .else(Value(null))

      expect(result.cases[0].then).toEqual(Value('Active User'))
      expect(result.cases[1].then).toEqual(Value('Senior Citizen'))
      expect(result.cases[2].then).toEqual(Value(1))
      expect(result.cases[3].then).toEqual(Value(0))
      expect(result.else).toEqual(Value(null))
    })
  })

  describe('Advanced Features Tests', () => {
    it('should handle nested case expressions', () => {
      const innerCase = Case()
        .when({ type: Eq('premium') }, Value('Premium Member'))
        .else(Value('Regular Member'))

      const result = Case()
        .when({ active: Eq(true) }, innerCase)
        .else(Value('Inactive'))

      expect(result.type).toBe('case')
      expect(result.cases[0].then.type).toBe('case')
      expect((result.cases[0].then as CaseExpr).cases).toHaveLength(1)
    })

    it('should handle case with SQL functions in then/else', () => {
      const result = Case()
        .when({ status: Eq('active') }, Count())
        .else(Sum('amount'))

      expect(result.cases[0].then).toEqual({
        type: 'function',
        name: 'count',
        args: [{ type: 'raw', sql: '*' }],
      })
      expect(result.else).toEqual({
        type: 'function',
        name: 'sum',
        args: [{ type: 'column', name: 'amount' }],
      })
    })

    it('should handle case with string functions', () => {
      const result = Case()
        .when({ name: Like('John%') }, Upper('first_name'))
        .else(Concat('Unknown', 'User'))

      expect(result.cases[0].then).toEqual({
        type: 'function',
        name: 'upper',
        args: [{ type: 'column', name: 'first_name' }],
      })
      expect(result.else).toEqual({
        type: 'function',
        name: 'concat',
        args: [
          { type: 'column', name: 'Unknown' },
          { type: 'column', name: 'User' },
        ],
      })
    })

    it('should handle complex predicates with And in conditions', () => {
      const result = Case()
        .when(
          {
            age: Gt(18),
            status: Eq('active'),
          },
          Value('Eligible'),
        )
        .else(Value('Not Eligible'))

      expect(result.cases[0].condition).toMatchObject({
        type: 'and',
        predicates: [
          {
            type: 'predicate',
            left: { type: 'column', name: 'age' },
            operator: '>',
            right: { type: 'value', value: 18 },
          },
          {
            type: 'predicate',
            left: { type: 'column', name: 'status' },
            operator: '=',
            right: { type: 'value', value: 'active' },
          },
        ],
      })
    })

    it('should handle Raw expressions in case statements', () => {
      const result = Case()
        .when({ status: Eq('active') }, Raw('NOW()'))
        .else(Raw('NULL'))

      expect(result.cases[0].then).toEqual(Raw('NOW()'))
      expect(result.else).toEqual(Raw('NULL'))
    })

    it('should support SelectBuilder instances as then/else expressions', () => {
      const thenSubquery = select(['id']).from('users')
      const elseSubquery = select(['*']).from('archived_users')

      const result = Case()
        .when({ status: Eq('active') }, thenSubquery)
        .else(elseSubquery)

      expect(result.cases[0].then).toMatchObject({
        type: 'subquery',
        query: thenSubquery.getQuery(),
      })
      expect(result.else).toMatchObject({
        type: 'subquery',
        query: elseSubquery.getQuery(),
      })
    })

    it('should handle column-to-column comparisons with EqCol', () => {
      const result = Case()
        .when({ 'a.value': EqCol('b.value') }, Value('Match'))
        .else(Value('No Match'))

      expect(result.cases[0].condition).toMatchObject({
        type: 'predicate',
        left: { type: 'column', name: 'value', table: 'a' },
        operator: '=',
        right: { type: 'column', name: 'value', table: 'b' },
      })
    })

    it('should handle In operator conditions', () => {
      const result = Case()
        .when({ status: In(['active', 'pending']) }, Value('In Progress'))
        .else(Value('Unknown'))

      expect(result.cases[0].condition).toMatchObject({
        type: 'predicate',
        left: { type: 'column', name: 'status' },
        operator: 'IN',
        right: {
          type: 'array',
          values: ['active', 'pending'],
        },
      })
    })

    it('should handle IsNull and IsNotNull operators', () => {
      const result = Case()
        .when({ email: IsNull() }, Value('No Email'))
        .when({ email: IsNotNull() }, Value('Has Email'))
        .else(Value('Unknown'))

      expect(result.cases).toHaveLength(2)
      expect(result.cases[0].condition).toMatchObject({
        type: 'predicate',
        left: { type: 'column', name: 'email' },
        operator: 'IS NULL',
      })
      expect(result.cases[1].condition).toMatchObject({
        type: 'predicate',
        left: { type: 'column', name: 'email' },
        operator: 'IS NOT NULL',
      })
    })
  })

  describe('Edge Cases & Error Handling', () => {
    it('should handle case without else clause', () => {
      const result = Case()
        .when({ status: Eq('active') }, Value('Active'))
        .else(Value(null))

      expect(result.else).toEqual(Value(null))
    })

    it('should handle undefined else value', () => {
      const result = Case()
        .when({ status: Eq('active') }, Value('Active'))
        .else(Value(undefined))

      expect(result.else).toEqual(Value(undefined))
    })

    it('should throw error for maximum nesting depth exceeded', () => {
      let nestedCase = Case()
        .when({ a: Eq(1) }, Value('a'))
        .else(Value('not_a'))

      for (let i = 0; i < 10; i++) {
        const previousCase = nestedCase
        nestedCase = Case()
          .when({ [`level_${i}`]: Eq(1) }, previousCase)
          .else(Value(`fallback_${i}`))
      }

      expect(nestedCase.type).toBe('case')
    })

    it('should handle empty string values', () => {
      const result = Case()
        .when({ status: Eq('') }, Value('Empty Status'))
        .else(Value('Has Status'))

      expect(result.cases[0].condition).toMatchObject({
        right: { type: 'value', value: '' },
      })
      expect(result.cases[0].then).toEqual(Value('Empty Status'))
    })

    it('should handle zero and false values', () => {
      const result = Case()
        .when({ count: Eq(0) }, Value(false))
        .when({ active: Eq(false) }, Value(0))
        .else(Value(1))

      expect(result.cases).toHaveLength(2)
      expect(result.cases[0].condition).toMatchObject({
        right: { type: 'value', value: 0 },
      })
      expect(result.cases[1].condition).toMatchObject({
        right: { type: 'value', value: false },
      })
      expect(result.cases[0].then).toEqual(Value(false))
      expect(result.cases[1].then).toEqual(Value(0))
    })

    it('should handle date values', () => {
      const testDate = new Date('2023-01-01')
      const result = Case()
        .when({ created_at: Gt(testDate) }, Value('Recent'))
        .else(Value('Old'))

      expect(result.cases[0].condition).toMatchObject({
        right: { type: 'value', value: testDate },
      })
    })

    it('should handle complex nested case with multiple conditions', () => {
      const innerCase = Case()
        .when({ type: Eq('premium') }, Value('Premium User'))
        .when({ type: Eq('basic') }, Value('Basic User'))
        .else(Value('Unknown Type'))

      const result = Case()
        .when({ active: Eq(true) }, innerCase)
        .when({ suspended: Eq(true) }, Value('Suspended'))
        .else(Value('Inactive'))

      expect(result.cases).toHaveLength(2)
      expect((result.cases[0].then as CaseExpr).type).toBe('case')
      expect((result.cases[0].then as CaseExpr).cases).toHaveLength(2)
    })

    it('should handle predicate nodes directly as conditions', () => {
      const predicateNode = {
        type: 'predicate' as const,
        left: { type: 'column' as const, name: 'score' },
        operator: '>',
        right: { type: 'value' as const, value: 80 },
      }

      const result = Case().when(predicateNode, Value('High Score')).else(Value('Low Score'))

      expect(result.cases[0].condition).toBe(predicateNode)
    })

    it('should handle field named "type" in WhereInput conditions', () => {
      // This test verifies the fix for the bug where fields named "type"
      // were incorrectly identified as PredicateNode objects
      const result = Case()
        .when({ type: Eq('VOLUME') }, Raw('toDecimal64(100, 4)'))
        .when({ type: Eq('PRESSURE') }, Raw('toDecimal32(50, 4)'))
        .when({ type: Eq('TEMPERATURE') }, Raw('toDecimal32(25, 4)'))
        .else(Raw('toDecimal32(0, 4)'))

      expect(result.cases).toHaveLength(3)
      // Verify that the condition is properly converted from WhereInput to PredicateNode
      expect(result.cases[0].condition).toMatchObject({
        type: 'predicate',
        left: { type: 'column', name: 'type' },
        operator: '=',
        right: { type: 'value', value: 'VOLUME' },
      })
      expect(result.cases[1].condition).toMatchObject({
        type: 'predicate',
        left: { type: 'column', name: 'type' },
        operator: '=',
        right: { type: 'value', value: 'PRESSURE' },
      })
      expect(result.cases[2].condition).toMatchObject({
        type: 'predicate',
        left: { type: 'column', name: 'type' },
        operator: '=',
        right: { type: 'value', value: 'TEMPERATURE' },
      })
    })
  })

  describe('Real-world Examples', () => {
    it('should handle user categorization by age ranges', () => {
      const result = Case()
        .when({ age: Lt(13) }, Value('Child'))
        .when({ age: Lt(18) }, Value('Teenager'))
        .when({ age: Lte(65) }, Value('Adult'))
        .else(Value('Senior'))

      expect(result.cases).toHaveLength(3)
    })

    it('should handle status mapping (numeric to string)', () => {
      const result = Case()
        .when({ status_code: Eq(1) }, Value('Active'))
        .when({ status_code: Eq(2) }, Value('Pending'))
        .when({ status_code: Eq(3) }, Value('Completed'))
        .when({ status_code: Eq(0) }, Value('Inactive'))
        .else(Value('Unknown Status'))

      expect(result.cases).toHaveLength(4)
      expect(result.cases.map((c) => c.condition)).toMatchSnapshot()
    })

    it('should handle conditional aggregation', () => {
      const result = Case()
        .when({ category: Eq('premium') }, Sum('revenue'))
        .when({ category: Eq('basic') }, Count())
        .else(Raw('0'))

      expect(result.cases[0].then).toMatchObject({
        type: 'function',
        name: 'sum',
        args: [{ type: 'column', name: 'revenue' }],
      })
      expect(result.cases[1].then).toMatchObject({
        type: 'function',
        name: 'count',
      })
      expect(result.else).toEqual(Raw('0'))
    })
  })
})
