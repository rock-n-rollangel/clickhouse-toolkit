/**
 * Operators Tests
 * Tests for all operator functions and combinators
 */

import { describe, it, expect } from '@jest/globals'
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
  RegExp,
  IsNull,
  IsNotNull,
  HasAny,
  HasAll,
  InTuple,
  And,
  Or,
  Not,
  Raw,
  startsWith,
  endsWith,
  contains,
} from '../../../src/index'

describe('Operators', () => {
  describe('Comparison Operators', () => {
    it('should create Eq operator', () => {
      const operator = Eq('active')
      expect(operator).toEqual({
        type: 'eq',
        value: 'active',
      })
    })

    it('should create Ne operator', () => {
      const operator = Ne('inactive')
      expect(operator).toEqual({
        type: 'ne',
        value: 'inactive',
      })
    })

    it('should create Gt operator', () => {
      const operator = Gt(18)
      expect(operator).toEqual({
        type: 'gt',
        value: 18,
      })
    })

    it('should create Gte operator', () => {
      const operator = Gte(18)
      expect(operator).toEqual({
        type: 'gte',
        value: 18,
      })
    })

    it('should create Lt operator', () => {
      const operator = Lt(65)
      expect(operator).toEqual({
        type: 'lt',
        value: 65,
      })
    })

    it('should create Lte operator', () => {
      const operator = Lte(65)
      expect(operator).toEqual({
        type: 'lte',
        value: 65,
      })
    })
  })

  describe('Array Operators', () => {
    it('should create In operator', () => {
      const operator = In(['active', 'pending'])
      expect(operator).toEqual({
        type: 'in',
        value: ['active', 'pending'],
      })
    })

    it('should create NotIn operator', () => {
      const operator = NotIn(['inactive', 'deleted'])
      expect(operator).toEqual({
        type: 'not_in',
        value: ['inactive', 'deleted'],
      })
    })

    it('should create HasAny operator', () => {
      const operator = HasAny(['tag1', 'tag2'])
      expect(operator).toEqual({
        type: 'has_any',
        value: ['tag1', 'tag2'],
      })
    })

    it('should create HasAll operator', () => {
      const operator = HasAll(['tag1', 'tag2'])
      expect(operator).toEqual({
        type: 'has_all',
        value: ['tag1', 'tag2'],
      })
    })

    it('should create InTuple operator', () => {
      const operator = InTuple([
        ['John', 25],
        ['Jane', 30],
      ])
      expect(operator).toEqual({
        type: 'in_tuple',
        value: [
          ['John', 25],
          ['Jane', 30],
        ],
      })
    })
  })

  describe('String Operators', () => {
    it('should create Like operator', () => {
      const operator = Like('John%')
      expect(operator).toEqual({
        type: 'like',
        value: 'John%',
      })
    })

    it('should create ILike operator', () => {
      const operator = ILike('john%')
      expect(operator).toEqual({
        type: 'ilike',
        value: 'john%',
      })
    })

    it('should create RegExp operator', () => {
      const operator = RegExp('^[A-Z]', 'i')
      expect(operator).toEqual({
        type: 'regexp',
        value: {
          pattern: '^[A-Z]',
          flags: 'i',
        },
      })
    })

    it('should create startsWith helper', () => {
      const operator = startsWith('John')
      expect(operator).toEqual({
        type: 'like',
        value: 'John%',
      })
    })

    it('should create endsWith helper', () => {
      const operator = endsWith('son')
      expect(operator).toEqual({
        type: 'like',
        value: '%son',
      })
    })

    it('should create contains helper', () => {
      const operator = contains('ohn')
      expect(operator).toEqual({
        type: 'like',
        value: '%ohn%',
      })
    })
  })

  describe('Range Operators', () => {
    it('should create Between operator', () => {
      const operator = Between(18, 65)
      expect(operator).toEqual({
        type: 'between',
        value: [18, 65],
      })
    })
  })

  describe('Null Operators', () => {
    it('should create IsNull operator', () => {
      const operator = IsNull()
      expect(operator).toEqual({
        type: 'is_null',
        value: null,
      })
    })

    it('should create IsNotNull operator', () => {
      const operator = IsNotNull()
      expect(operator).toEqual({
        type: 'is_not_null',
        value: null,
      })
    })
  })

  describe('Combinators', () => {
    it('should create And combinator', () => {
      const combinator = And({ status: Eq('active') }, { age: Gt(18) })
      expect(combinator).toEqual({
        type: 'and',
        predicates: [{ status: Eq('active') }, { age: Gt(18) }],
      })
    })

    it('should create Or combinator', () => {
      const combinator = Or({ status: Eq('active') }, { status: Eq('pending') })
      expect(combinator).toEqual({
        type: 'or',
        predicates: [{ status: Eq('active') }, { status: Eq('pending') }],
      })
    })

    it('should create Not combinator', () => {
      const combinator = Not({ status: Eq('inactive') })
      expect(combinator).toEqual({
        type: 'not',
        predicates: [{ status: Eq('inactive') }],
      })
    })

    it('should create And combinator with operators', () => {
      const combinator = And(Gte(25), Lt(30))
      expect(combinator).toEqual({
        type: 'and',
        predicates: [Gte(25), Lt(30)],
      })
    })

    it('should create Or combinator with operators', () => {
      const combinator = Or(Eq('active'), Eq('pending'))
      expect(combinator).toEqual({
        type: 'or',
        predicates: [Eq('active'), Eq('pending')],
      })
    })

    it('should create Not combinator with operator', () => {
      const combinator = Not(Eq(27))
      expect(combinator).toEqual({
        type: 'not',
        predicates: [Eq(27)],
      })
    })

    it('should create nested combinators', () => {
      const combinator = And(Gte(25), Lt(30), Not(Eq(27)))
      expect(combinator).toEqual({
        type: 'and',
        predicates: [Gte(25), Lt(30), Not(Eq(27))],
      })
    })
  })

  describe('Raw Operator', () => {
    it('should create Raw operator', () => {
      const operator = Raw("age > 18 AND status = 'active'")
      expect(operator).toEqual({
        type: 'raw',
        sql: "age > 18 AND status = 'active'",
      })
    })

    it('should not warn in production for Raw operator', () => {
      const originalEnv = process.env.NODE_ENV
      process.env.NODE_ENV = 'production'

      const consoleSpy = jest.spyOn(console, 'warn').mockImplementation()

      Raw('age > 18')

      expect(consoleSpy).not.toHaveBeenCalled()

      consoleSpy.mockRestore()
      process.env.NODE_ENV = originalEnv
    })
  })

  describe('Type Safety', () => {
    it('should handle different data types', () => {
      const operators = [
        Eq('string'),
        Eq(123),
        Eq(true),
        Eq(null),
        Eq(new Date('2023-01-01')),
        Gt(85.5),
        In([1, 2, 3]),
        In(['a', 'b', 'c']),
        Between(1, 100),
        Like('pattern%'),
        RegExp('^test', 'i'),
      ]

      operators.forEach((operator) => {
        expect(operator).toBeDefined()
        expect(operator.type).toBeDefined()
        expect(operator.value).toBeDefined()
      })
    })

    it('should handle nested combinators', () => {
      const complexCombinator = And(
        { status: Eq('active') },
        Or({ age: Gt(18) }, { age: Lt(65) }),
        Not({ deleted: Eq(true) }),
      )

      expect(complexCombinator).toEqual({
        type: 'and',
        predicates: [
          { status: Eq('active') },
          {
            type: 'or',
            predicates: [{ age: Gt(18) }, { age: Lt(65) }],
          },
          {
            type: 'not',
            predicates: [{ deleted: Eq(true) }],
          },
        ],
      })
    })
  })

  describe('Edge Cases', () => {
    it('should handle special characters in Like patterns', () => {
      const operator = Like('test%_[]')
      expect(operator).toEqual({
        type: 'like',
        value: 'test%_[]',
      })
    })

    it('should handle empty strings', () => {
      const operator = Eq('')
      expect(operator).toEqual({
        type: 'eq',
        value: '',
      })
    })

    it('should handle zero values', () => {
      const operator = Gt(0)
      expect(operator).toEqual({
        type: 'gt',
        value: 0,
      })
    })

    it('should handle negative numbers', () => {
      const operator = Lt(-10)
      expect(operator).toEqual({
        type: 'lt',
        value: -10,
      })
    })

    it('should handle floating point numbers', () => {
      const operator = Between(1.5, 99.9)
      expect(operator).toEqual({
        type: 'between',
        value: [1.5, 99.9],
      })
    })
  })
})
