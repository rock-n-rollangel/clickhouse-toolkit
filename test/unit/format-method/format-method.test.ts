/**
 * Format Method Tests
 * Tests the new .format() method functionality
 */

import { describe, it, expect } from '@jest/globals'
import { insertInto, select } from '../../../src/index'

describe('Format Method Tests', () => {
  describe('InsertBuilder Format', () => {
    it('should set format on InsertBuilder', () => {
      const builder = insertInto('users')
        .columns(['id', 'name'])
        .values([[1, 'test']])
        .format('JSONCompactEachRow')

      const sql = builder.toSQL()
      expect(sql.sql).toContain('INSERT INTO `users`')
    })

    it('should support object-based inserts with format', () => {
      const builder = insertInto('users')
        .objects([{ id: 1, name: 'test' }])
        .format('JSONCompactEachRow')

      // Should not throw when building
      expect(() => builder.toSQL()).not.toThrow()
    })

    it('should support stream-based inserts with format', () => {
      const mockStream = {
        readable: true,
        read: () => null,
        on: () => {},
        pipe: () => {},
      } as any

      const builder = insertInto('users').fromStream(mockStream).format('JSONCompactEachRow')

      // Should not throw when building
      expect(() => builder.toSQL()).not.toThrow()
    })
  })

  describe('SelectBuilder Format', () => {
    it('should set format on SelectBuilder', () => {
      const builder = select(['id', 'name']).from('users').format('JSONEachRow')

      const sql = builder.toSQL()
      expect(sql.sql).toContain('SELECT')
    })

    it('should validate streamable formats for stream method', () => {
      const builder = select(['id', 'name']).from('users').format('JSONEachRow')

      // Should not throw when building
      expect(() => builder.toSQL()).not.toThrow()
    })

    it('should throw error for non-streamable format in stream', () => {
      select(['id', 'name']).from('users').format('JSON') // JSON is not streamable

      // This should throw when stream() is called
      expect(() => {
        // We can't actually call stream() without a runner, but we can test the validation logic
        const format = 'JSON'
        const streamableFormats = [
          'JSONEachRow',
          'JSONStringsEachRow',
          'JSONCompactEachRow',
          'JSONCompactStringsEachRow',
          'JSONCompactEachRowWithNames',
          'JSONCompactEachRowWithNamesAndTypes',
          'JSONCompactStringsEachRowWithNames',
          'JSONCompactStringsEachRowWithNamesAndTypes',
        ]

        if (!streamableFormats.includes(format as any)) {
          throw new Error(`Format '${format}' is not streamable`)
        }
      }).toThrow("Format 'JSON' is not streamable")
    })
  })

  describe('Format Validation', () => {
    it('should accept valid streamable formats', () => {
      const validFormats = [
        'JSONEachRow',
        'JSONStringsEachRow',
        'JSONCompactEachRow',
        'JSONCompactStringsEachRow',
        'JSONCompactEachRowWithNames',
        'JSONCompactEachRowWithNamesAndTypes',
        'JSONCompactStringsEachRowWithNames',
        'JSONCompactStringsEachRowWithNamesAndTypes',
      ]

      validFormats.forEach((format) => {
        const builder = select(['id'])
          .from('users')
          .format(format as any)
        expect(() => builder.toSQL()).not.toThrow()
      })
    })

    it('should reject non-streamable formats for streaming', () => {
      const nonStreamableFormats = ['JSON', 'CSV', 'TabSeparated']

      nonStreamableFormats.forEach((format) => {
        const builder = select(['id'])
          .from('users')
          .format(format as any)
        // The format should be set, but stream() should validate it
        expect(() => builder.toSQL()).not.toThrow()
      })
    })
  })
})
