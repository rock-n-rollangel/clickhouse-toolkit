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
      const stream = await select(['id', 'name']).from('users').limit(2).stream(queryRunner, 'JSONEachRow')

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
})
