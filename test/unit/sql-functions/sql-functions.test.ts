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
      expect(Count()).toBe('count(*)')
      expect(Count('*')).toBe('count(*)')
      expect(Count('id')).toBe('count(id)')
    })

    it('should create Sum function', () => {
      expect(Sum('amount')).toBe('sum(amount)')
    })

    it('should create Avg function', () => {
      expect(Avg('age')).toBe('avg(age)')
    })

    it('should create Min function', () => {
      expect(Min('price')).toBe('min(price)')
    })

    it('should create Max function', () => {
      expect(Max('price')).toBe('max(price)')
    })

    it('should create CountDistinct function', () => {
      expect(CountDistinct('user_id')).toBe('count(distinct user_id)')
    })
  })

  describe('String Functions', () => {
    it('should create Concat function', () => {
      expect(Concat('first_name', 'last_name')).toBe('concat(first_name, last_name)')
      expect(Concat('a', 'b', 'c')).toBe('concat(a, b, c)')
    })

    it('should create Upper function', () => {
      expect(Upper('name')).toBe('upper(name)')
    })

    it('should create Lower function', () => {
      expect(Lower('name')).toBe('lower(name)')
    })

    it('should create Trim function', () => {
      expect(Trim('name')).toBe('trim(name)')
    })

    it('should create Substring function', () => {
      expect(Substring('name', 1, 5)).toBe('substring(name, 1, 5)')
      expect(Substring('name', 1)).toBe('substring(name, 1)')
    })
  })

  describe('Math Functions', () => {
    it('should create Round function', () => {
      expect(Round('price')).toBe('round(price, 0)')
      expect(Round('price', 2)).toBe('round(price, 2)')
    })

    it('should create Floor function', () => {
      expect(Floor('value')).toBe('floor(value)')
    })

    it('should create Ceil function', () => {
      expect(Ceil('value')).toBe('ceil(value)')
    })

    it('should create Abs function', () => {
      expect(Abs('value')).toBe('abs(value)')
    })
  })

  describe('Conditional Functions', () => {
    it('should create If function', () => {
      expect(If('age > 18', '1', '0')).toBe('if(age > 18, 1, 0)')
    })

    it('should create Coalesce function', () => {
      expect(Coalesce('name', 'default_name')).toBe('coalesce(name, default_name)')
      expect(Coalesce('a', 'b', 'c')).toBe('coalesce(a, b, c)')
    })
  })

  describe('Date/Time Functions', () => {
    it('should create Now function', () => {
      expect(Now()).toBe('now()')
    })

    it('should create Today function', () => {
      expect(Today()).toBe('today()')
    })

    it('should create ToDate function', () => {
      expect(ToDate('created_at')).toBe('toDate(created_at)')
    })

    it('should create ToDateTime function', () => {
      expect(ToDateTime('created_at')).toBe('toDateTime(created_at)')
    })

    it('should create FormatDateTime function', () => {
      expect(FormatDateTime('created_at', '%Y-%m-%d')).toBe("formatDateTime(created_at, '%Y-%m-%d')")
    })
  })

  describe('Type Conversion Functions', () => {
    it('should create Cast function', () => {
      expect(Cast('value', 'String')).toBe('cast(value as String)')
      expect(Cast('age', 'UInt32')).toBe('cast(age as UInt32)')
    })

    it('should create ToString function', () => {
      expect(ToString('id')).toBe('toString(id)')
    })

    it('should create ToInt function', () => {
      expect(ToInt('value')).toBe('toInt32(value)')
    })

    it('should create ToFloat function', () => {
      expect(ToFloat('value')).toBe('toFloat64(value)')
    })
  })

  describe('ClickHouse-Specific Functions', () => {
    it('should create GroupArray function', () => {
      expect(GroupArray('tags')).toBe('groupArray(tags)')
    })

    it('should create UniqExact function', () => {
      expect(UniqExact('user_id')).toBe('uniqExact(user_id)')
    })

    it('should create Median function', () => {
      expect(Median('age')).toBe('median(age)')
    })
  })
})
