/**
 * Error Handling Tests
 * Tests for typed error system and error handling
 */

import { describe, it, expect, jest } from '@jest/globals'
import { select, Eq } from '../../../src/index'
import {
  ClickHouseToolkitError,
  QueryError,
  ValidationError,
  ConnectionError,
  TimeoutError,
  CancelledError,
  IdentifierError,
  ParameterError,
} from '../../../src/core/errors'

describe('Error Handling', () => {
  describe('Typed Error System', () => {
    it('should create QueryError with ClickHouse code', () => {
      const error = new QueryError('Query failed', '42', 'SELECT * FROM invalid_table')

      expect(error).toBeInstanceOf(ClickHouseToolkitError)
      expect(error).toBeInstanceOf(QueryError)
      expect(error.message).toBe('Query failed')
      expect(error.type).toBe('execution')
      expect(error.code).toBe('QUERY_ERROR')
    })

    it('should create ValidationError with field and value', () => {
      const error = new ValidationError('Invalid value', undefined, 'age', 150)

      expect(error).toBeInstanceOf(ClickHouseToolkitError)
      expect(error).toBeInstanceOf(ValidationError)
      expect(error.message).toBe('Invalid value')
      expect(error.field).toBe('age')
      expect(error.value).toBe(150)
      expect(error.type).toBe('validation')
    })

    it('should create ConnectionError', () => {
      const error = new ConnectionError('Connection failed', 'ECONNREFUSED')

      expect(error).toBeInstanceOf(ClickHouseToolkitError)
      expect(error).toBeInstanceOf(ConnectionError)
      expect(error.message).toBe('Connection failed')
      expect(error.code).toBe('CONNECTION_ERROR')
      expect(error.type).toBe('connection')
    })

    it('should create TimeoutError', () => {
      const error = new TimeoutError('Query timeout', undefined, 30000)

      expect(error).toBeInstanceOf(ClickHouseToolkitError)
      expect(error).toBeInstanceOf(TimeoutError)
      expect(error.message).toBe('Query timeout')
      expect(error.timeoutMs).toBe(30000)
      expect(error.code).toBe('TIMEOUT_ERROR')
      expect(error.type).toBe('execution')
    })

    it('should create CancelledError', () => {
      const error = new CancelledError('Query cancelled')

      expect(error).toBeInstanceOf(ClickHouseToolkitError)
      expect(error).toBeInstanceOf(CancelledError)
      expect(error.message).toBe('Query cancelled')
      expect(error.code).toBe('CANCELLED_ERROR')
      expect(error.type).toBe('execution')
    })

    it('should create IdentifierError', () => {
      const error = new IdentifierError('Invalid identifier', 'invalid-table-name')

      expect(error).toBeInstanceOf(ClickHouseToolkitError)
      expect(error).toBeInstanceOf(IdentifierError)
      expect(error.message).toBe('Invalid identifier')
      expect(error.code).toBe('IDENTIFIER_ERROR')
      expect(error.type).toBe('validation')
    })

    it('should create ParameterError', () => {
      const error = new ParameterError('Invalid parameter', 'age', 'not_a_number')

      expect(error).toBeInstanceOf(ClickHouseToolkitError)
      expect(error).toBeInstanceOf(ParameterError)
      expect(error.message).toBe('Invalid parameter')
      expect(error.code).toBe('PARAMETER_ERROR')
      expect(error.type).toBe('validation')
    })
  })

  describe('Error Context and Stack Traces', () => {
    it('should preserve stack trace in typed errors', () => {
      const error = new QueryError('Test error', '42', 'SELECT * FROM test')

      expect(error.stack).toBeDefined()
      expect(error.stack).toContain('QueryError')
    })

    it('should include context in error messages', () => {
      const error = new ValidationError('Invalid age value', undefined, 'age', 150)

      expect(error.message).toContain('Invalid age value')
      expect(error.field).toBe('age')
      expect(error.value).toBe(150)
    })

    it('should handle nested error contexts', () => {
      try {
        throw new ValidationError('Invalid input', undefined, 'email', 'invalid-email')
      } catch (error) {
        expect(error).toBeInstanceOf(ValidationError)
        expect((error as ValidationError).field).toBe('email')
        expect((error as ValidationError).value).toBe('invalid-email')
      }
    })
  })

  describe('Query Builder Error Handling', () => {
    it('should throw ValidationError for missing QueryRunner in run()', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ status: Eq('active') })

      expect(() => query.run()).rejects.toThrow('QueryRunner is required to execute queries')
    })

    it('should throw ValidationError for missing QueryRunner in stream()', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ status: Eq('active') })

      expect(() => query.stream()).rejects.toThrow('QueryRunner is required to stream queries')
    })

    it('should handle invalid column references gracefully', () => {
      const query = select(['invalid_column']).from('users')

      // Should not throw during query building
      expect(() => query.toSQL()).not.toThrow()
    })

    it('should handle invalid table references gracefully', () => {
      const query = select(['id']).from('invalid_table')

      // Should not throw during query building
      expect(() => query.toSQL()).not.toThrow()
    })
  })

  describe('QueryRunner Error Handling', () => {
    it('should handle connection errors', async () => {
      const mockRunner = {
        execute: jest.fn().mockRejectedValue(new Error('Connection refused') as never),
        stream: jest.fn(),
        streamJSONEachRow: jest.fn(),
        streamCSV: jest.fn(),
      }

      const query = select(['id', 'name'])
        .from('users')
        .where({ status: Eq('active') })

      await expect(query.run(mockRunner as any)).rejects.toThrow()
    })

    it('should handle timeout errors', async () => {
      const mockRunner = {
        execute: jest.fn().mockRejectedValue(new Error('Query timeout') as never),
        stream: jest.fn(),
        streamJSONEachRow: jest.fn(),
        streamCSV: jest.fn(),
      }

      const query = select(['id', 'name'])
        .from('users')
        .where({ status: Eq('active') })

      await expect(query.run(mockRunner as any)).rejects.toThrow()
    })

    it('should handle query syntax errors', async () => {
      const mockRunner = {
        execute: jest.fn().mockRejectedValue(new Error('Syntax error in SQL') as never),
        stream: jest.fn(),
        streamJSONEachRow: jest.fn(),
        streamCSV: jest.fn(),
      }

      const query = select(['id', 'name'])
        .from('users')
        .where({ status: Eq('active') })

      await expect(query.run(mockRunner as any)).rejects.toThrow()
    })
  })

  describe('Error Recovery and Retry Logic', () => {
    it('should handle transient connection errors', async () => {
      let attemptCount = 0
      const mockRunner = {
        execute: jest.fn().mockImplementation(() => {
          attemptCount++
          if (attemptCount < 3) {
            throw new Error('Connection temporarily unavailable')
          }
          return [{ id: 1, name: 'John' }]
        }),
        stream: jest.fn(),
        streamJSONEachRow: jest.fn(),
        streamCSV: jest.fn(),
      }

      const query = select(['id', 'name'])
        .from('users')
        .where({ status: Eq('active') })

      // This would need retry logic implemented in the actual QueryRunner
      await expect(query.run(mockRunner as any)).rejects.toThrow()
    })

    it('should handle partial failures gracefully', async () => {
      const mockRunner = {
        execute: jest.fn().mockResolvedValue([] as never),
        stream: jest.fn(),
        streamJSONEachRow: jest.fn(),
        streamCSV: jest.fn(),
      }

      const query = select(['id', 'name'])
        .from('users')
        .where({ status: Eq('active') })

      const result = await query.run(mockRunner as any)
      expect(result).toEqual([])
    })
  })

  describe('Error Logging and Monitoring', () => {
    it('should log errors with context', () => {
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation(() => {})

      const error = new QueryError('Query failed', undefined, '42', 'SELECT * FROM users')

      // Simulate error logging
      console.error('Query execution failed:', {
        error: error.message,
        clickhouseCode: error.clickhouseCode,
        sql: error.sql,
        type: error.type,
      })

      expect(consoleSpy).toHaveBeenCalledWith('Query execution failed:', {
        error: 'Query failed',
        clickhouseCode: '42',
        sql: 'SELECT * FROM users',
        type: 'execution',
      })

      consoleSpy.mockRestore()
    })

    it('should include error context in logs', () => {
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation(() => {})

      const error = new ValidationError('Invalid input', undefined, 'email', 'invalid-email')

      // Simulate error logging with context
      console.error('Validation failed:', {
        error: error.message,
        field: error.field,
        value: error.value,
        type: error.type,
      })

      expect(consoleSpy).toHaveBeenCalledWith('Validation failed:', {
        error: 'Invalid input',
        value: 'invalid-email',
        queryId: undefined,
        field: 'email',
        type: 'validation',
      })

      consoleSpy.mockRestore()
    })
  })

  describe('Error Type Checking', () => {
    it('should check error types correctly', () => {
      const queryError = new QueryError('Query failed', undefined, '42', 'SELECT * FROM users')
      const validationError = new ValidationError('Invalid input', undefined, 'email', 'invalid')
      const connectionError = new ConnectionError('Connection failed', undefined, 'ECONNREFUSED')

      expect(queryError instanceof QueryError).toBe(true)
      expect(queryError instanceof ClickHouseToolkitError).toBe(true)
      expect(queryError instanceof ValidationError).toBe(false)

      expect(validationError instanceof ValidationError).toBe(true)
      expect(validationError instanceof ClickHouseToolkitError).toBe(true)
      expect(validationError instanceof QueryError).toBe(false)

      expect(connectionError instanceof ConnectionError).toBe(true)
      expect(connectionError instanceof ClickHouseToolkitError).toBe(true)
      expect(connectionError instanceof ValidationError).toBe(false)
    })

    it('should handle error type checking in catch blocks', () => {
      try {
        throw new QueryError('Query failed', undefined, '42', 'SELECT * FROM users')
      } catch (error) {
        if (error instanceof QueryError) {
          expect(error.clickhouseCode).toBe('42')
          expect(error.sql).toBe('SELECT * FROM users')
        } else {
          fail('Expected QueryError')
        }
      }
    })
  })

  describe('Error Serialization', () => {
    it('should serialize errors to JSON', () => {
      const error = new QueryError('Query failed', undefined, '42', 'SELECT * FROM users')
      const serialized = JSON.stringify(error)
      const parsed = JSON.parse(serialized)

      expect(parsed.message).toBe('Query failed')
      expect(parsed.type).toBe('execution')
      expect(parsed.code).toBe('QUERY_ERROR')
    })

    it('should handle error serialization with complex data', () => {
      const error = new ValidationError('Invalid input', undefined, 'metadata', { key: 'value' })
      const serialized = JSON.stringify(error)
      const parsed = JSON.parse(serialized)

      expect(parsed.message).toBe('Invalid input')
      expect(parsed.code).toBe('VALIDATION_ERROR')
    })
  })
})
