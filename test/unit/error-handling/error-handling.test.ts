/**
 * Error Handling Tests
 * Comprehensive tests for error handling and custom error classes
 */

import { describe, it, expect } from '@jest/globals'
import {
  ClickHouseToolkitError,
  QueryError,
  TimeoutError,
  CancelledError,
  ValidationError,
  IdentifierError,
  ParameterError,
  MigrationError,
  DriftError,
  ConnectionError,
  SchemaError,
  createQueryError,
  createTimeoutError,
  createValidationError,
  createIdentifierError,
  createParameterError,
  createMigrationError,
  createConnectionError,
  createSchemaError,
} from '../../../src/core/errors'

describe('ClickHouseToolkitError', () => {
  class TestError extends ClickHouseToolkitError {
    readonly code = 'TEST_ERROR'
    readonly type = 'test'
  }

  it('should create error with basic properties', () => {
    const error = new TestError('Test error message')

    expect(error).toBeInstanceOf(Error)
    expect(error).toBeInstanceOf(ClickHouseToolkitError)
    expect(error.message).toBe('Test error message')
    expect(error.name).toBe('TestError')
    expect(error.code).toBe('TEST_ERROR')
    expect(error.type).toBe('test')
    expect(error.timestamp).toBeInstanceOf(Date)
    expect(error.queryId).toBeUndefined()
  })

  it('should create error with query ID', () => {
    const queryId = 'query_123'
    const error = new TestError('Test error message', queryId)

    expect(error.queryId).toBe(queryId)
  })

  it('should serialize to JSON correctly', () => {
    const error = new TestError('Test error message', 'query_123')
    const json = error.toJSON()

    expect(json).toEqual({
      name: 'TestError',
      code: 'TEST_ERROR',
      type: 'test',
      message: 'Test error message',
      queryId: 'query_123',
      timestamp: expect.any(String),
    })
  })

  it('should have timestamp in ISO format', () => {
    const error = new TestError('Test error message')
    const json = error.toJSON()

    expect(json.timestamp).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
  })
})

describe('QueryError', () => {
  it('should create QueryError with basic properties', () => {
    const error = new QueryError('Query failed')

    expect(error).toBeInstanceOf(QueryError)
    expect(error).toBeInstanceOf(ClickHouseToolkitError)
    expect(error.code).toBe('QUERY_ERROR')
    expect(error.type).toBe('execution')
    expect(error.message).toBe('Query failed')
    expect(error.clickhouseCode).toBeUndefined()
    expect(error.sql).toBeUndefined()
  })

  it('should create QueryError with ClickHouse code and SQL', () => {
    const error = new QueryError('Query failed', 'query_123', 'SYNTAX_ERROR', 'SELECT * FROM invalid')

    expect(error.queryId).toBe('query_123')
    expect(error.clickhouseCode).toBe('SYNTAX_ERROR')
    expect(error.sql).toBe('SELECT * FROM invalid')
  })

  it('should serialize to JSON with additional properties', () => {
    const error = new QueryError('Query failed', 'query_123', 'SYNTAX_ERROR', 'SELECT * FROM invalid')
    const json = error.toJSON()

    expect(json.code).toBe('QUERY_ERROR')
    expect(json.type).toBe('execution')
    expect(json.queryId).toBe('query_123')
  })
})

describe('TimeoutError', () => {
  it('should create TimeoutError with basic properties', () => {
    const error = new TimeoutError('Query timed out')

    expect(error).toBeInstanceOf(TimeoutError)
    expect(error).toBeInstanceOf(ClickHouseToolkitError)
    expect(error.code).toBe('TIMEOUT_ERROR')
    expect(error.type).toBe('execution')
    expect(error.message).toBe('Query timed out')
    expect(error.timeoutMs).toBeUndefined()
  })

  it('should create TimeoutError with timeout value', () => {
    const error = new TimeoutError('Query timed out', 'query_123', 5000)

    expect(error.queryId).toBe('query_123')
    expect(error.timeoutMs).toBe(5000)
  })
})

describe('CancelledError', () => {
  it('should create CancelledError with basic properties', () => {
    const error = new CancelledError('Query was cancelled')

    expect(error).toBeInstanceOf(CancelledError)
    expect(error).toBeInstanceOf(ClickHouseToolkitError)
    expect(error.code).toBe('CANCELLED_ERROR')
    expect(error.type).toBe('execution')
    expect(error.message).toBe('Query was cancelled')
  })

  it('should create CancelledError with query ID', () => {
    const error = new CancelledError('Query was cancelled', 'query_123')

    expect(error.queryId).toBe('query_123')
  })
})

describe('ValidationError', () => {
  it('should create ValidationError with basic properties', () => {
    const error = new ValidationError('Validation failed')

    expect(error).toBeInstanceOf(ValidationError)
    expect(error).toBeInstanceOf(ClickHouseToolkitError)
    expect(error.code).toBe('VALIDATION_ERROR')
    expect(error.type).toBe('validation')
    expect(error.message).toBe('Validation failed')
    expect(error.field).toBeUndefined()
    expect(error.value).toBeUndefined()
  })

  it('should create ValidationError with field and value', () => {
    const error = new ValidationError('Invalid field value', 'query_123', 'age', 'invalid')

    expect(error.queryId).toBe('query_123')
    expect(error.field).toBe('age')
    expect(error.value).toBe('invalid')
  })
})

describe('IdentifierError', () => {
  it('should create IdentifierError with basic properties', () => {
    const error = new IdentifierError('Invalid identifier', 'invalid_table')

    expect(error).toBeInstanceOf(IdentifierError)
    expect(error).toBeInstanceOf(ClickHouseToolkitError)
    expect(error.code).toBe('IDENTIFIER_ERROR')
    expect(error.type).toBe('validation')
    expect(error.message).toBe('Invalid identifier')
    expect(error.identifier).toBe('invalid_table')
  })

  it('should create IdentifierError with query ID', () => {
    const error = new IdentifierError('Invalid identifier', 'invalid_table', 'query_123')

    expect(error.queryId).toBe('query_123')
    expect(error.identifier).toBe('invalid_table')
  })
})

describe('ParameterError', () => {
  it('should create ParameterError with basic properties', () => {
    const error = new ParameterError('Invalid parameter', 'invalid_param')

    expect(error).toBeInstanceOf(ParameterError)
    expect(error).toBeInstanceOf(ClickHouseToolkitError)
    expect(error.code).toBe('PARAMETER_ERROR')
    expect(error.type).toBe('validation')
    expect(error.message).toBe('Invalid parameter')
    expect(error.parameter).toBe('invalid_param')
  })

  it('should create ParameterError with query ID', () => {
    const error = new ParameterError('Invalid parameter', 'invalid_param', 'query_123')

    expect(error.queryId).toBe('query_123')
    expect(error.parameter).toBe('invalid_param')
  })
})

describe('MigrationError', () => {
  it('should create MigrationError with basic properties', () => {
    const error = new MigrationError('Migration failed')

    expect(error).toBeInstanceOf(MigrationError)
    expect(error).toBeInstanceOf(ClickHouseToolkitError)
    expect(error.code).toBe('MIGRATION_ERROR')
    expect(error.type).toBe('migration')
    expect(error.message).toBe('Migration failed')
    expect(error.migrationId).toBeUndefined()
    expect(error.step).toBeUndefined()
  })

  it('should create MigrationError with migration ID and step', () => {
    const error = new MigrationError('Migration failed', 'query_123', 'migration_001', 'up')

    expect(error.queryId).toBe('query_123')
    expect(error.migrationId).toBe('migration_001')
    expect(error.step).toBe('up')
  })
})

describe('DriftError', () => {
  it('should create DriftError with basic properties', () => {
    const differences = [
      {
        type: 'column_missing',
        table: 'users',
        column: 'email',
        expected: 'String',
        actual: undefined,
      },
    ]
    const error = new DriftError('Schema drift detected', differences)

    expect(error).toBeInstanceOf(DriftError)
    expect(error).toBeInstanceOf(ClickHouseToolkitError)
    expect(error.code).toBe('DRIFT_ERROR')
    expect(error.type).toBe('migration')
    expect(error.message).toBe('Schema drift detected')
    expect(error.differences).toEqual(differences)
  })

  it('should create DriftError with query ID', () => {
    const differences = [
      {
        type: 'column_type_mismatch',
        table: 'users',
        column: 'age',
        expected: 'UInt32',
        actual: 'String',
      },
    ]
    const error = new DriftError('Schema drift detected', differences, 'query_123')

    expect(error.queryId).toBe('query_123')
    expect(error.differences).toEqual(differences)
  })
})

describe('ConnectionError', () => {
  it('should create ConnectionError with basic properties', () => {
    const error = new ConnectionError('Connection failed')

    expect(error).toBeInstanceOf(ConnectionError)
    expect(error).toBeInstanceOf(ClickHouseToolkitError)
    expect(error.code).toBe('CONNECTION_ERROR')
    expect(error.type).toBe('connection')
    expect(error.message).toBe('Connection failed')
    expect(error.url).toBeUndefined()
  })

  it('should create ConnectionError with URL', () => {
    const error = new ConnectionError('Connection failed', 'query_123', 'http://localhost:8123')

    expect(error.queryId).toBe('query_123')
    expect(error.url).toBe('http://localhost:8123')
  })
})

describe('SchemaError', () => {
  it('should create SchemaError with basic properties', () => {
    const error = new SchemaError('Schema error')

    expect(error).toBeInstanceOf(SchemaError)
    expect(error).toBeInstanceOf(ClickHouseToolkitError)
    expect(error.code).toBe('SCHEMA_ERROR')
    expect(error.type).toBe('schema')
    expect(error.message).toBe('Schema error')
    expect(error.table).toBeUndefined()
    expect(error.column).toBeUndefined()
  })

  it('should create SchemaError with table and column', () => {
    const error = new SchemaError('Schema error', 'query_123', 'users', 'email')

    expect(error.queryId).toBe('query_123')
    expect(error.table).toBe('users')
    expect(error.column).toBe('email')
  })
})

describe('Error Factory Functions', () => {
  describe('createQueryError', () => {
    it('should create QueryError instance', () => {
      const error = createQueryError('Query failed', 'query_123', 'SYNTAX_ERROR', 'SELECT * FROM invalid')

      expect(error).toBeInstanceOf(QueryError)
      expect(error.message).toBe('Query failed')
      expect(error.queryId).toBe('query_123')
      expect(error.clickhouseCode).toBe('SYNTAX_ERROR')
      expect(error.sql).toBe('SELECT * FROM invalid')
    })
  })

  describe('createTimeoutError', () => {
    it('should create TimeoutError instance', () => {
      const error = createTimeoutError('Query timed out', 'query_123', 5000)

      expect(error).toBeInstanceOf(TimeoutError)
      expect(error.message).toBe('Query timed out')
      expect(error.queryId).toBe('query_123')
      expect(error.timeoutMs).toBe(5000)
    })
  })

  describe('createValidationError', () => {
    it('should create ValidationError instance', () => {
      const error = createValidationError('Validation failed', 'query_123', 'age', 'invalid')

      expect(error).toBeInstanceOf(ValidationError)
      expect(error.message).toBe('Validation failed')
      expect(error.queryId).toBe('query_123')
      expect(error.field).toBe('age')
      expect(error.value).toBe('invalid')
    })
  })

  describe('createIdentifierError', () => {
    it('should create IdentifierError instance', () => {
      const error = createIdentifierError('Invalid identifier', 'invalid_table', 'query_123')

      expect(error).toBeInstanceOf(IdentifierError)
      expect(error.message).toBe('Invalid identifier')
      expect(error.identifier).toBe('invalid_table')
      expect(error.queryId).toBe('query_123')
    })
  })

  describe('createParameterError', () => {
    it('should create ParameterError instance', () => {
      const error = createParameterError('Invalid parameter', 'invalid_param', 'query_123')

      expect(error).toBeInstanceOf(ParameterError)
      expect(error.message).toBe('Invalid parameter')
      expect(error.parameter).toBe('invalid_param')
      expect(error.queryId).toBe('query_123')
    })
  })

  describe('createMigrationError', () => {
    it('should create MigrationError instance', () => {
      const error = createMigrationError('Migration failed', 'query_123', 'migration_001', 'up')

      expect(error).toBeInstanceOf(MigrationError)
      expect(error.message).toBe('Migration failed')
      expect(error.queryId).toBe('query_123')
      expect(error.migrationId).toBe('migration_001')
      expect(error.step).toBe('up')
    })
  })

  describe('createConnectionError', () => {
    it('should create ConnectionError instance', () => {
      const error = createConnectionError('Connection failed', 'query_123', 'http://localhost:8123')

      expect(error).toBeInstanceOf(ConnectionError)
      expect(error.message).toBe('Connection failed')
      expect(error.queryId).toBe('query_123')
      expect(error.url).toBe('http://localhost:8123')
    })
  })

  describe('createSchemaError', () => {
    it('should create SchemaError instance', () => {
      const error = createSchemaError('Schema error', 'query_123', 'users', 'email')

      expect(error).toBeInstanceOf(SchemaError)
      expect(error.message).toBe('Schema error')
      expect(error.queryId).toBe('query_123')
      expect(error.table).toBe('users')
      expect(error.column).toBe('email')
    })
  })
})

describe('Error Inheritance and Polymorphism', () => {
  it('should allow catching by base class', () => {
    const error = new QueryError('Query failed', 'query_123')

    expect(() => {
      throw error
    }).toThrow(ClickHouseToolkitError)
  })

  it('should allow catching by specific error class', () => {
    const error = new ValidationError('Validation failed')

    expect(() => {
      throw error
    }).toThrow(ValidationError)
  })

  it('should maintain error stack trace', () => {
    function throwError() {
      throw new QueryError('Query failed')
    }

    expect(() => throwError()).toThrow('Query failed')
  })
})

describe('Error Serialization', () => {
  it('should serialize all error types to JSON', () => {
    const errors = [
      new QueryError('Query failed', 'query_123', 'SYNTAX_ERROR', 'SELECT * FROM invalid'),
      new TimeoutError('Query timed out', 'query_123', 5000),
      new ValidationError('Validation failed', 'query_123', 'age', 'invalid'),
      new MigrationError('Migration failed', 'query_123', 'migration_001', 'up'),
      new ConnectionError('Connection failed', 'query_123', 'http://localhost:8123'),
    ]

    errors.forEach((error) => {
      const json = error.toJSON()

      expect(json).toHaveProperty('name')
      expect(json).toHaveProperty('code')
      expect(json).toHaveProperty('type')
      expect(json).toHaveProperty('message')
      expect(json).toHaveProperty('queryId')
      expect(json).toHaveProperty('timestamp')

      expect(json.name).toBe(error.constructor.name)
      expect(json.code).toBe(error.code)
      expect(json.type).toBe(error.type)
      expect(json.message).toBe(error.message)
    })
  })
})
