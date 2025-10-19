/**
 * SQL Helper Functions Tests
 * Tests for SQL function helpers
 */

import { describe, it, expect } from '@jest/globals'
import {
  Count,
  Sum,
  Avg,
  Min,
  Max,
  CountDistinct,
  Concat,
  Upper,
  Lower,
  Trim,
  Substring,
  Round,
  Floor,
  Ceil,
  Abs,
  If,
  Coalesce,
  Now,
  Today,
  ToDate,
  ToDateTime,
  FormatDateTime,
  Cast,
  ToString,
  ToInt,
  ToFloat,
  GroupArray,
  UniqExact,
  Median,
} from '../../../src/index'

describe('SQL Helper Functions', () => {
  describe('Aggregate Functions', () => {
    it('should create Count function', () => {
      expect(Count()).toEqual({ type: 'function', name: 'count', args: [{ type: 'raw', sql: '*' }] })
      expect(Count('*')).toEqual({ type: 'function', name: 'count', args: [{ type: 'column', name: '*' }] })
      expect(Count('id')).toEqual({ type: 'function', name: 'count', args: [{ type: 'column', name: 'id' }] })
    })

    it('should create Sum function', () => {
      expect(Sum('amount')).toEqual({ type: 'function', name: 'sum', args: [{ type: 'column', name: 'amount' }] })
    })

    it('should create Avg function', () => {
      expect(Avg('age')).toEqual({ type: 'function', name: 'avg', args: [{ type: 'column', name: 'age' }] })
    })

    it('should create Min function', () => {
      expect(Min('price')).toEqual({ type: 'function', name: 'min', args: [{ type: 'column', name: 'price' }] })
    })

    it('should create Max function', () => {
      expect(Max('price')).toEqual({ type: 'function', name: 'max', args: [{ type: 'column', name: 'price' }] })
    })

    it('should create CountDistinct function', () => {
      expect(CountDistinct('user_id')).toEqual({
        type: 'function',
        name: 'count',
        args: [{ type: 'function', name: 'distinct', args: [{ type: 'column', name: 'user_id' }] }],
      })
    })
  })

  describe('String Functions', () => {
    it('should create Concat function', () => {
      expect(Concat('first_name', 'last_name')).toEqual({
        type: 'function',
        name: 'concat',
        args: [
          { type: 'column', name: 'first_name' },
          { type: 'column', name: 'last_name' },
        ],
      })
      expect(Concat('a', 'b', 'c')).toEqual({
        type: 'function',
        name: 'concat',
        args: [
          { type: 'column', name: 'a' },
          { type: 'column', name: 'b' },
          { type: 'column', name: 'c' },
        ],
      })
    })

    it('should create Upper function', () => {
      expect(Upper('name')).toEqual({ type: 'function', name: 'upper', args: [{ type: 'column', name: 'name' }] })
    })

    it('should create Lower function', () => {
      expect(Lower('name')).toEqual({ type: 'function', name: 'lower', args: [{ type: 'column', name: 'name' }] })
    })

    it('should create Trim function', () => {
      expect(Trim('name')).toEqual({ type: 'function', name: 'trim', args: [{ type: 'column', name: 'name' }] })
    })

    it('should create Substring function', () => {
      expect(Substring('name', 1, 5)).toEqual({
        type: 'function',
        name: 'substring',
        args: [
          { type: 'column', name: 'name' },
          { type: 'value', value: 1 },
          { type: 'value', value: 5 },
        ],
      })
      expect(Substring('name', 1)).toEqual({
        type: 'function',
        name: 'substring',
        args: [
          { type: 'column', name: 'name' },
          { type: 'value', value: 1 },
        ],
      })
    })
  })

  describe('Math Functions', () => {
    it('should create Round function', () => {
      expect(Round('price')).toEqual({
        type: 'function',
        name: 'round',
        args: [
          { type: 'column', name: 'price' },
          { type: 'value', value: 0 },
        ],
      })
      expect(Round('price', 2)).toEqual({
        type: 'function',
        name: 'round',
        args: [
          { type: 'column', name: 'price' },
          { type: 'value', value: 2 },
        ],
      })
    })

    it('should create Floor function', () => {
      expect(Floor('value')).toEqual({ type: 'function', name: 'floor', args: [{ type: 'column', name: 'value' }] })
    })

    it('should create Ceil function', () => {
      expect(Ceil('value')).toEqual({ type: 'function', name: 'ceil', args: [{ type: 'column', name: 'value' }] })
    })

    it('should create Abs function', () => {
      expect(Abs('value')).toEqual({ type: 'function', name: 'abs', args: [{ type: 'column', name: 'value' }] })
    })
  })

  describe('Conditional Functions', () => {
    it('should create If function', () => {
      expect(If('age > 18', '1', '0')).toEqual({
        type: 'function',
        name: 'if',
        args: [
          { type: 'raw', sql: 'age > 18' },
          { type: 'column', name: '1' },
          { type: 'column', name: '0' },
        ],
      })
    })

    it('should create Coalesce function', () => {
      expect(Coalesce('name', 'default_name')).toEqual({
        type: 'function',
        name: 'coalesce',
        args: [
          { type: 'column', name: 'name' },
          { type: 'column', name: 'default_name' },
        ],
      })
      expect(Coalesce('a', 'b', 'c')).toEqual({
        type: 'function',
        name: 'coalesce',
        args: [
          { type: 'column', name: 'a' },
          { type: 'column', name: 'b' },
          { type: 'column', name: 'c' },
        ],
      })
    })
  })

  describe('Date/Time Functions', () => {
    it('should create Now function', () => {
      expect(Now()).toEqual({ type: 'function', name: 'now', args: [] })
    })

    it('should create Today function', () => {
      expect(Today()).toEqual({ type: 'function', name: 'today', args: [] })
    })

    it('should create ToDate function', () => {
      expect(ToDate('created_at')).toEqual({
        type: 'function',
        name: 'toDate',
        args: [{ type: 'column', name: 'created_at' }],
      })
    })

    it('should create ToDateTime function', () => {
      expect(ToDateTime('created_at')).toEqual({
        type: 'function',
        name: 'toDateTime',
        args: [{ type: 'column', name: 'created_at' }],
      })
    })

    it('should create FormatDateTime function', () => {
      expect(FormatDateTime('created_at', '%Y-%m-%d')).toEqual({
        type: 'function',
        name: 'formatDateTime',
        args: [
          { type: 'column', name: 'created_at' },
          { type: 'value', value: '%Y-%m-%d' },
        ],
      })
    })
  })

  describe('Type Conversion Functions', () => {
    it('should create Cast function', () => {
      expect(Cast('value', 'String')).toEqual({
        type: 'function',
        name: 'cast',
        args: [
          { type: 'column', name: 'value' },
          { type: 'raw', sql: 'String' },
        ],
      })
      expect(Cast('age', 'UInt32')).toEqual({
        type: 'function',
        name: 'cast',
        args: [
          { type: 'column', name: 'age' },
          { type: 'raw', sql: 'UInt32' },
        ],
      })
    })

    it('should create ToString function', () => {
      expect(ToString('id')).toEqual({ type: 'function', name: 'toString', args: [{ type: 'column', name: 'id' }] })
    })

    it('should create ToInt function', () => {
      expect(ToInt('value')).toEqual({ type: 'function', name: 'toInt32', args: [{ type: 'column', name: 'value' }] })
    })

    it('should create ToFloat function', () => {
      expect(ToFloat('value')).toEqual({
        type: 'function',
        name: 'toFloat64',
        args: [{ type: 'column', name: 'value' }],
      })
    })
  })

  describe('ClickHouse-Specific Functions', () => {
    it('should create GroupArray function', () => {
      expect(GroupArray('tags')).toEqual({
        type: 'function',
        name: 'groupArray',
        args: [{ type: 'column', name: 'tags' }],
      })
    })

    it('should create UniqExact function', () => {
      expect(UniqExact('user_id')).toEqual({
        type: 'function',
        name: 'uniqExact',
        args: [{ type: 'column', name: 'user_id' }],
      })
    })

    it('should create Median function', () => {
      expect(Median('age')).toEqual({ type: 'function', name: 'median', args: [{ type: 'column', name: 'age' }] })
    })
  })
})
