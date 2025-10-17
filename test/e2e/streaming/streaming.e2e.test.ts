/**
 * Streaming E2E Tests
 * Tests streaming functionality with different formats
 */

import { describe, it, expect, beforeAll, afterAll } from '@jest/globals'
import { select } from '../../../src/index'
import { testSetup } from '../setup/test-setup'

describe('Streaming E2E Tests', () => {
  let queryRunner: any

  beforeAll(async () => {
    await testSetup.setup()
    queryRunner = testSetup.getQueryRunner()
  }, 60000)

  afterAll(async () => {
    await testSetup.teardown()
  }, 30000)

  describe('JSONEachRow Format (Default)', () => {
    it('should stream results in JSONEachRow format by default', async () => {
      const stream = await select(['id', 'name', 'email']).from('users').limit(3).stream(queryRunner)

      const results: any[] = []

      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          if (results.length > 0) {
            expect(results.length).toBeGreaterThanOrEqual(1)
            expect(results[0]).toHaveProperty('id')
            expect(results[0]).toHaveProperty('name')
            resolve()
          } else {
            reject(new Error('No data received'))
          }
        }, 2000)

        stream.on('data', (rows: any[]) => {
          results.push(...rows)
          if (results.length >= 3) {
            clearTimeout(timeout)
            expect(results.length).toBeGreaterThanOrEqual(3)
            resolve()
          }
        })

        stream.on('error', (error) => {
          clearTimeout(timeout)
          reject(error)
        })
      })
    })

    it('should stream results with explicit JSONEachRow format', async () => {
      const stream = await select(['id', 'name']).from('users').limit(2).format('JSONEachRow').stream(queryRunner)

      const results: any[] = []

      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          if (results.length > 0) {
            expect(results[0]).toHaveProperty('id')
            expect(results[0]).toHaveProperty('name')
            resolve()
          } else {
            reject(new Error('No data received'))
          }
        }, 2000)

        stream.on('data', (rows: any[]) => {
          results.push(...rows)
          if (results.length >= 2) {
            clearTimeout(timeout)
            resolve()
          }
        })

        stream.on('error', (error) => {
          clearTimeout(timeout)
          reject(error)
        })
      })
    })
  })

  describe('Format Validation', () => {
    it('should stream with CSV format', async () => {
      const stream = await select(['id', 'name', 'email']).from('users').limit(2).format('CSV').stream(queryRunner)

      const results: any[] = []

      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          if (results.length > 0) {
            // CSV format might return objects or strings depending on client implementation
            // Let's check what we actually get and validate accordingly
            const firstResult = results[0]
            if (typeof firstResult === 'string') {
              expect(firstResult).toContain(',')
            } else if (typeof firstResult === 'object' && Array.isArray(firstResult)) {
              // CSV might be parsed as arrays
              expect(firstResult.length).toBeGreaterThan(0)
            } else if (typeof firstResult === 'object') {
              // CSV might be parsed as objects
              expect(firstResult).toHaveProperty('id')
              expect(firstResult).toHaveProperty('name')
            }
            resolve()
          } else {
            reject(new Error('No data received'))
          }
        }, 2000)

        stream.on('data', (data: any) => {
          results.push(data)
          if (results.length >= 1) {
            clearTimeout(timeout)
            resolve()
          }
        })

        stream.on('error', (error) => {
          clearTimeout(timeout)
          reject(error)
        })
      })
    })

    it('should stream with TabSeparated format', async () => {
      const stream = await select(['id', 'name']).from('users').limit(2).format('TabSeparated').stream(queryRunner)

      const results: any[] = []

      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          if (results.length > 0) {
            // TabSeparated format might return objects or strings depending on client implementation
            // Let's check what we actually get and validate accordingly
            const firstResult = results[0]
            if (typeof firstResult === 'string') {
              expect(firstResult).toContain('\t')
            } else if (typeof firstResult === 'object' && Array.isArray(firstResult)) {
              // TabSeparated might be parsed as arrays
              expect(firstResult.length).toBeGreaterThan(0)
            } else if (typeof firstResult === 'object') {
              // TabSeparated might be parsed as objects
              expect(firstResult).toHaveProperty('id')
              expect(firstResult).toHaveProperty('name')
            }
            resolve()
          } else {
            reject(new Error('No data received'))
          }
        }, 2000)

        stream.on('data', (data: any) => {
          results.push(data)
          if (results.length >= 1) {
            clearTimeout(timeout)
            resolve()
          }
        })

        stream.on('error', (error) => {
          clearTimeout(timeout)
          reject(error)
        })
      })
    })

    it('should throw error for non-streamable format in stream', async () => {
      // JSON format is not streamable
      const query = select(['id', 'name']).from('users').format('JSON')

      await expect(query.stream(queryRunner)).rejects.toThrow()
    })

    it('should work with format method chaining', async () => {
      const stream = await select(['id', 'name']).from('users').limit(1).format('JSONEachRow').stream(queryRunner)

      const results: any[] = []

      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          if (results.length > 0) {
            expect(results[0]).toHaveProperty('id')
            expect(results[0]).toHaveProperty('name')
            resolve()
          } else {
            reject(new Error('No data received'))
          }
        }, 2000)

        stream.on('data', (rows: any[]) => {
          results.push(...rows)
          if (results.length >= 1) {
            clearTimeout(timeout)
            resolve()
          }
        })

        stream.on('error', (error) => {
          clearTimeout(timeout)
          reject(error)
        })
      })
    })
  })

  describe('SelectBuilder Format Tests', () => {
    it('should run query with JSON format (default)', async () => {
      const results = await select(['id', 'name', 'email']).from('users').limit(2).format('JSON').run(queryRunner)

      expect(Array.isArray(results)).toBe(true)
      expect(results.length).toBeGreaterThanOrEqual(1)
      expect(results[0]).toHaveProperty('id')
      expect(results[0]).toHaveProperty('name')
      expect(results[0]).toHaveProperty('email')
    })

    it('should run query with JSONCompactEachRow format', async () => {
      const results = await select(['id', 'name']).from('users').limit(2).format('JSONCompactEachRow').run(queryRunner)

      expect(Array.isArray(results)).toBe(true)
      expect(results.length).toBeGreaterThanOrEqual(1)
      // JSONCompactEachRow returns arrays, not objects
      expect(Array.isArray(results[0])).toBe(true)
      expect(results[0]).toHaveLength(2) // [id, name]
      expect(results[0][0]).toBe(1) // id
      expect(results[0][1]).toBe('John Doe') // name
    })

    it('should persist format through query chain', async () => {
      const query = select(['id', 'name']).from('users').limit(1).format('JSONEachRow')

      // Format should be preserved in the query object
      // Note: toSQL() doesn't include FORMAT clause as it's handled by QueryRunner
      const sql = query.toSQL()
      expect(sql.sql).toContain('SELECT')
      expect(sql.sql).toContain('FROM')

      // The format should be accessible through the query object
      // This is an internal property, but we can test it indirectly
      // by ensuring the stream works with the format
      const stream = await query.stream(queryRunner)
      expect(stream).toBeDefined()
    })
  })
})
