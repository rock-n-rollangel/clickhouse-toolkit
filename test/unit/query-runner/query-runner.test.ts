/**
 * QueryRunner Tests
 * Comprehensive tests for the QueryRunner class
 */

import { describe, it, expect, beforeEach, jest, afterEach } from '@jest/globals'
import { QueryRunner, createQueryRunner, QueryRunnerOptions, QueryRequest } from '../../../src/runner/query-runner'
import { createClient } from '@clickhouse/client'

// Mock the ClickHouse client
jest.mock('@clickhouse/client', () => ({
  createClient: jest.fn(),
}))

const mockCreateClient = createClient as jest.MockedFunction<typeof createClient>

describe('QueryRunner', () => {
  let mockClient: any
  let queryRunner: QueryRunner
  let options: QueryRunnerOptions

  beforeEach(() => {
    // Reset all mocks
    jest.clearAllMocks()

    // Create mock client
    mockClient = {
      command: jest.fn(),
      query: jest.fn(),
      close: jest.fn(),
    }

    mockCreateClient.mockReturnValue(mockClient)

    // Default options
    options = {
      url: 'http://localhost:8123',
      username: 'default',
      password: '',
      database: 'test',
      timeout: 30000,
      retries: 3,
      keepAlive: true,
    }

    queryRunner = new QueryRunner(options)
  })

  afterEach(() => {
    jest.restoreAllMocks()
  })

  describe('Constructor', () => {
    it('should create QueryRunner with default options', () => {
      expect(queryRunner).toBeInstanceOf(QueryRunner)
      expect(mockCreateClient).toHaveBeenCalledWith({
        url: options.url,
        username: options.username,
        password: options.password,
        database: options.database,
        clickhouse_settings: undefined,
        keep_alive: { enabled: true },
      })
    })

    it('should create QueryRunner with custom settings', () => {
      const customOptions = {
        ...options,
        settings: { max_execution_time: 60 },
        keepAlive: false,
      }

      new QueryRunner(customOptions)

      expect(mockCreateClient).toHaveBeenCalledWith({
        url: customOptions.url,
        username: customOptions.username,
        password: customOptions.password,
        database: customOptions.database,
        clickhouse_settings: { max_execution_time: 60 },
        keep_alive: { enabled: false },
      })
    })

    it('should create QueryRunner without optional parameters', () => {
      const minimalOptions = {
        url: 'http://localhost:8123',
        username: 'default',
        password: '',
        database: 'test',
      }

      new QueryRunner(minimalOptions)

      expect(mockCreateClient).toHaveBeenCalledWith({
        url: minimalOptions.url,
        username: minimalOptions.username,
        password: minimalOptions.password,
        database: minimalOptions.database,
        clickhouse_settings: undefined,
        keep_alive: { enabled: false },
      })
    })
  })

  describe('command', () => {
    it('should execute command successfully', async () => {
      const request: QueryRequest = {
        sql: 'CREATE TABLE test (id UInt32) ENGINE = Memory',
      }

      mockClient.command.mockResolvedValue(undefined)

      await queryRunner.command(request)

      expect(mockClient.command).toHaveBeenCalledWith({
        query: request.sql,
        clickhouse_settings: undefined,
        abort_signal: new AbortController().signal,
      })
    })

    it('should execute command with settings', async () => {
      const request: QueryRequest = {
        sql: 'CREATE TABLE test (id UInt32) ENGINE = Memory',
        settings: { max_execution_time: 60 },
      }

      mockClient.command.mockResolvedValue(undefined)

      await queryRunner.command(request)

      expect(mockClient.command).toHaveBeenCalledWith({
        query: request.sql,
        clickhouse_settings: { max_execution_time: 60 },
        abort_signal: new AbortController().signal,
      })
    })

    it('should handle command execution errors', async () => {
      const request: QueryRequest = {
        sql: 'INVALID SQL',
      }

      const error = new Error('SQL syntax error')
      mockClient.command.mockRejectedValue(error)

      await expect(queryRunner.command(request)).rejects.toThrow()
    })
  })

  describe('execute', () => {
    it('should execute query and return results', async () => {
      const request: QueryRequest = {
        sql: 'SELECT id, name FROM users',
      }

      const mockResult = {
        json: jest
          .fn()
          .mockResolvedValue([{ id: 1, name: 'John' } as never, { id: 2, name: 'Jane' } as never] as never),
      }

      mockClient.query.mockResolvedValue(mockResult)

      const result = await queryRunner.execute(request)

      expect(mockClient.query).toHaveBeenCalledWith({
        query: request.sql,
        format: 'JSON',
        clickhouse_settings: undefined,
        abort_signal: new AbortController().signal,
      })
      expect(result).toEqual([
        { id: 1, name: 'John' },
        { id: 2, name: 'Jane' },
      ])
    })

    it('should execute query with settings', async () => {
      const request: QueryRequest = {
        sql: 'SELECT * FROM large_table',
        settings: { max_execution_time: 120 },
      }

      const mockResult = {
        json: jest.fn().mockResolvedValue([] as never),
      }

      mockClient.query.mockResolvedValue(mockResult)

      await queryRunner.execute(request)

      expect(mockClient.query).toHaveBeenCalledWith({
        query: request.sql,
        format: 'JSON',
        clickhouse_settings: { max_execution_time: 120 },
        abort_signal: new AbortController().signal,
      })
    })

    it('should handle non-array results', async () => {
      const request: QueryRequest = {
        sql: 'SELECT count() FROM users',
      }

      const mockResult = {
        json: jest.fn().mockResolvedValue({ data: [{ 'count()': 100 }] } as never),
      }

      mockClient.query.mockResolvedValue(mockResult)

      const result = await queryRunner.execute(request)

      expect(result).toEqual([{ 'count()': 100 }])
    })

    it('should handle query execution errors', async () => {
      const request: QueryRequest = {
        sql: 'SELECT * FROM non_existent_table',
      }

      const error = new Error('Table does not exist')
      mockClient.query.mockRejectedValue(error)

      await expect(queryRunner.execute(request)).rejects.toThrow()
    })
  })

  describe('stream', () => {
    it('should create stream successfully', async () => {
      const request: QueryRequest = {
        sql: 'SELECT * FROM users',
      }

      const mockStream = {
        on: jest.fn(),
        pipe: jest.fn(),
      }

      const mockResult = {
        stream: jest.fn().mockReturnValue(mockStream),
      }

      mockClient.query.mockResolvedValue(mockResult)

      const stream = await queryRunner.stream(request)

      expect(mockClient.query).toHaveBeenCalledWith({
        query: request.sql,
        format: 'JSONEachRow',
        clickhouse_settings: undefined,
        abort_signal: new AbortController().signal,
      })
      expect(stream).toBe(mockStream)
    })

    it('should create stream with settings', async () => {
      const request: QueryRequest = {
        sql: 'SELECT * FROM users',
        settings: { max_execution_time: 60 },
      }

      const mockStream = {
        on: jest.fn(),
        pipe: jest.fn(),
      }

      const mockResult = {
        stream: jest.fn().mockReturnValue(mockStream),
      }

      mockClient.query.mockResolvedValue(mockResult)

      await queryRunner.stream(request)

      expect(mockClient.query).toHaveBeenCalledWith({
        query: request.sql,
        format: 'JSONEachRow',
        clickhouse_settings: { max_execution_time: 60 },
        abort_signal: new AbortController().signal,
      })
    })

    it('should handle stream creation errors', async () => {
      const request: QueryRequest = {
        sql: 'SELECT * FROM non_existent_table',
      }

      const error = new Error('Table does not exist')
      mockClient.query.mockRejectedValue(error)

      await expect(queryRunner.stream(request)).rejects.toThrow()
    })
  })

  describe('streamJSONEachRow', () => {
    it('should create JSONEachRow stream successfully', async () => {
      const request: QueryRequest = {
        sql: 'SELECT * FROM users',
      }

      const mockStream = {
        on: jest.fn(),
        pipe: jest.fn(),
      }

      const mockResult = {
        stream: jest.fn().mockReturnValue(mockStream),
      }

      mockClient.query.mockResolvedValue(mockResult)

      const stream = await queryRunner.stream({ ...request, format: 'JSONEachRow' })

      expect(mockClient.query).toHaveBeenCalledWith({
        query: 'SELECT * FROM users',
        format: 'JSONEachRow',
        clickhouse_settings: undefined,
        abort_signal: new AbortController().signal,
      })
      expect(stream).toBe(mockStream)
    })

    it('should create JSONEachRow stream with settings', async () => {
      const request: QueryRequest = {
        sql: 'SELECT * FROM users',
        settings: { max_execution_time: 60 },
      }

      const mockStream = {
        on: jest.fn(),
        pipe: jest.fn(),
      }

      const mockResult = {
        stream: jest.fn().mockReturnValue(mockStream),
      }

      mockClient.query.mockResolvedValue(mockResult)

      await queryRunner.stream({ ...request, format: 'JSONEachRow' })

      expect(mockClient.query).toHaveBeenCalledWith({
        query: 'SELECT * FROM users',
        format: 'JSONEachRow',
        clickhouse_settings: { max_execution_time: 60 },
        abort_signal: new AbortController().signal,
      })
    })
  })

  describe('streamCSV', () => {
    it('should create CSV stream successfully', async () => {
      const request: QueryRequest = {
        sql: 'SELECT * FROM users',
      }

      const mockStream = {
        on: jest.fn(),
        pipe: jest.fn(),
      }

      const mockResult = {
        stream: jest.fn().mockReturnValue(mockStream),
      }

      mockClient.query.mockResolvedValue(mockResult)

      const stream = await queryRunner.stream({ ...request, format: 'CSV' })

      expect(mockClient.query).toHaveBeenCalledWith({
        query: 'SELECT * FROM users',
        format: 'CSV',
        clickhouse_settings: undefined,
        abort_signal: new AbortController().signal,
      })
      expect(stream).toBe(mockStream)
    })

    it('should create CSV stream with settings', async () => {
      const request: QueryRequest = {
        sql: 'SELECT * FROM users',
        settings: { max_execution_time: 60 },
      }

      const mockStream = {
        on: jest.fn(),
        pipe: jest.fn(),
      }

      const mockResult = {
        stream: jest.fn().mockReturnValue(mockStream),
      }

      mockClient.query.mockResolvedValue(mockResult)

      await queryRunner.stream({ ...request, format: 'CSV' })

      expect(mockClient.query).toHaveBeenCalledWith({
        query: 'SELECT * FROM users',
        format: 'CSV',
        clickhouse_settings: { max_execution_time: 60 },
        abort_signal: new AbortController().signal,
      })
    })
  })

  describe('testConnection', () => {
    it('should return true for successful connection', async () => {
      mockClient.query.mockResolvedValue({ json: jest.fn().mockResolvedValue([{ '1': 1 }] as never) })

      const result = await queryRunner.testConnection()

      expect(result).toBe(true)
      expect(mockClient.query).toHaveBeenCalledWith({ query: 'SELECT 1' })
    })

    it('should return false for failed connection', async () => {
      mockClient.query.mockRejectedValue(new Error('Connection failed'))

      const result = await queryRunner.testConnection()

      expect(result).toBe(false)
    })
  })

  describe('getServerVersion', () => {
    it('should return server version successfully', async () => {
      const mockResult = {
        json: jest.fn().mockResolvedValue([{ 'version()': '22.8.0' }] as never),
      }

      mockClient.query.mockResolvedValue(mockResult)

      const version = await queryRunner.getServerVersion()

      expect(version).toBe('22.8.0')
      expect(mockClient.query).toHaveBeenCalledWith({ query: 'SELECT version()' })
    })

    it('should return unknown for failed version query', async () => {
      mockClient.query.mockRejectedValue(new Error('Version query failed'))

      const version = await queryRunner.getServerVersion()

      expect(version).toBe('unknown')
    })
  })

  describe('close', () => {
    it('should close the connection', async () => {
      mockClient.close.mockResolvedValue(undefined)

      await queryRunner.close()

      expect(mockClient.close).toHaveBeenCalled()
    })
  })

  describe('Error Handling', () => {
    it('should handle timeout errors', async () => {
      const request: QueryRequest = {
        sql: 'SELECT * FROM users',
      }

      const timeoutError = new Error('Request timeout') as any
      timeoutError.code = 'ETIMEDOUT'
      mockClient.query.mockRejectedValue(timeoutError)

      await expect(queryRunner.execute(request)).rejects.toThrow()
    })

    it('should handle connection errors', async () => {
      const request: QueryRequest = {
        sql: 'SELECT * FROM users',
      }

      const connectionError = new Error('Connection refused') as any
      connectionError.code = 'ECONNREFUSED'
      mockClient.query.mockRejectedValue(connectionError)

      await expect(queryRunner.execute(request)).rejects.toThrow()
    })

    it('should handle abort errors', async () => {
      const request: QueryRequest = {
        sql: 'SELECT * FROM users',
      }

      const abortError = new Error('Operation was aborted')
      abortError.name = 'AbortError'
      mockClient.query.mockRejectedValue(abortError)

      await expect(queryRunner.execute(request)).rejects.toThrow()
    })
  })

  describe('Timeout Handling', () => {
    it('should create abort signal when timeout is specified', () => {
      const runnerWithTimeout = new QueryRunner({
        ...options,
        timeout: 5000,
      })

      // Access private method through any cast for testing
      const abortSignal = (runnerWithTimeout as any).createAbortSignal()

      expect(abortSignal).toBeDefined()
      expect(abortSignal.aborted).toBe(false)
    })

    it('should not create abort signal when timeout is not specified', () => {
      const runnerWithoutTimeout = new QueryRunner({
        ...options,
        timeout: 0,
      })

      const abortSignal = (runnerWithoutTimeout as any).createAbortSignal()

      expect(abortSignal).toBeUndefined()
    })
  })

  describe('Query Context', () => {
    it('should generate unique query IDs', () => {
      const context1 = (queryRunner as any).createContext({ sql: 'SELECT 1' })
      const context2 = (queryRunner as any).createContext({ sql: 'SELECT 2' })

      expect(context1.queryId).toBeDefined()
      expect(context2.queryId).toBeDefined()
      expect(context1.queryId).not.toBe(context2.queryId)
    })

    it('should include timestamp in context', () => {
      const before = new Date()
      const context = (queryRunner as any).createContext({ sql: 'SELECT 1' })
      const after = new Date()

      expect(context.timestamp).toBeInstanceOf(Date)
      expect(context.timestamp.getTime()).toBeGreaterThanOrEqual(before.getTime())
      expect(context.timestamp.getTime()).toBeLessThanOrEqual(after.getTime())
    })

    it('should include settings in context', () => {
      const request = { sql: 'SELECT 1', settings: { max_execution_time: 60 } }
      const context = (queryRunner as any).createContext(request)

      expect(context.settings).toEqual({ max_execution_time: 60 })
    })
  })
})

describe('createQueryRunner Factory', () => {
  it('should create QueryRunner instance', () => {
    const options: QueryRunnerOptions = {
      url: 'http://localhost:8123',
      username: 'default',
      password: '',
      database: 'test',
    }

    const runner = createQueryRunner(options)

    expect(runner).toBeInstanceOf(QueryRunner)
    expect(mockCreateClient).toHaveBeenCalled()
  })
})
