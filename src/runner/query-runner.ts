/**
 * QueryRunner - Transport adapter for executing queries
 * Handles HTTP/native adapters, retries, timeouts, streaming
 */

import { createClient, ClickHouseClient } from '@clickhouse/client'
import { QueryContext } from '../core/ir'
import {
  ClickHouseToolkitError,
  CancelledError,
  createQueryError,
  createTimeoutError,
  createConnectionError,
} from '../core/errors'
import { Logger, LoggerContext, createLoggerContext } from '../core/logger'

export interface QueryRunnerOptions {
  url: string
  username: string
  password: string
  database: string
  settings?: Record<string, any>
  timeout?: number
  retries?: number
  keepAlive?: boolean
  logger?: Logger
}

export interface QuerySettings {
  max_execution_time?: number
  max_threads?: number
  readonly?: string
  [key: string]: any
}

export interface QueryRequest {
  sql: string
  settings?: QuerySettings
}

export class QueryRunner {
  private client: ClickHouseClient
  private options: QueryRunnerOptions
  private logger: LoggerContext

  constructor(options: QueryRunnerOptions) {
    this.options = options
    this.logger = createLoggerContext({ component: 'QueryRunner' })

    this.logger.info('Initializing QueryRunner', {
      url: options.url,
      database: options.database,
      timeout: options.timeout,
      retries: options.retries,
    })

    this.client = createClient({
      url: options.url,
      username: options.username,
      password: options.password,
      database: options.database,
      clickhouse_settings: options.settings,
      keep_alive: options.keepAlive ? { enabled: true } : { enabled: false },
    })
  }

  async command(request: QueryRequest): Promise<void> {
    const context = this.createContext(request)
    const queryLogger = this.logger.withQueryId(context.queryId).withOperation('command')

    queryLogger.debug('Executing command', {
      sql: request.sql,
      settings: request.settings,
    })

    try {
      await queryLogger.timeAsync('command_execution', async () => {
        return await this.client.command({
          query: request.sql,
          clickhouse_settings: request.settings,
          abort_signal: this.createAbortSignal(),
        })
      })

      queryLogger.info('Command executed successfully', {
        sql: request.sql,
      })
    } catch (error) {
      queryLogger.error('Command execution failed', {
        sql: request.sql,
        error: error instanceof Error ? error.message : String(error),
      })
      throw this.handleError(error, context)
    }
  }

  /**
   * Execute a query and return results
   */
  async execute<T = unknown>(request: QueryRequest): Promise<T[]> {
    const context = this.createContext(request)
    const queryLogger = this.logger.withQueryId(context.queryId).withOperation('execute')

    queryLogger.debug('Executing query', {
      sql: request.sql,
      settings: request.settings,
    })

    try {
      const result = await queryLogger.timeAsync('query_execution', async () => {
        return await this.client.query({
          query: request.sql,
          clickhouse_settings: request.settings,
          abort_signal: this.createAbortSignal(),
        })
      })

      const data = await result.json<T[]>()
      const finalData = Array.isArray(data) ? data : (data as any).data || []

      queryLogger.info('Query executed successfully', {
        resultCount: finalData.length,
        sql: request.sql,
      })

      return finalData
    } catch (error) {
      queryLogger.error('Query execution failed', {
        sql: request.sql,
        error: error instanceof Error ? error.message : String(error),
      })
      throw this.handleError(error, context)
    }
  }

  /**
   * Execute a query and return a stream using ClickHouse client's native streaming
   */
  async stream<T = unknown>(request: QueryRequest): Promise<NodeJS.ReadableStream> {
    const context = this.createContext(request)
    const queryLogger = this.logger.withQueryId(context.queryId).withOperation('stream')

    queryLogger.debug('Starting query stream', {
      sql: request.sql,
      settings: request.settings,
    })

    try {
      const result = await queryLogger.timeAsync('stream_creation', async () => {
        return await this.client.query({
          query: request.sql,
          format: 'JSONEachRow',
          clickhouse_settings: request.settings,
          abort_signal: this.createAbortSignal(),
        })
      })

      queryLogger.info('Query stream created successfully', {
        sql: request.sql,
      })

      // Use ClickHouse client's native stream() method
      return result.stream<T>()
    } catch (error) {
      queryLogger.error('Query stream creation failed', {
        sql: request.sql,
        error: error instanceof Error ? error.message : String(error),
      })
      throw this.handleError(error, context)
    }
  }

  /**
   * Execute a query with JSONEachRow format for streaming
   * Uses ClickHouse client's native streaming with JSONEachRow format
   */
  async streamJSONEachRow<T = unknown>(request: QueryRequest): Promise<NodeJS.ReadableStream> {
    const context = this.createContext(request)
    const queryLogger = this.logger.withQueryId(context.queryId).withOperation('streamJSONEachRow')

    queryLogger.debug('Starting JSONEachRow stream', {
      sql: request.sql,
      format: 'JSONEachRow',
    })

    try {
      const result = await queryLogger.timeAsync('json_each_row_stream_creation', async () => {
        return await this.client.query({
          query: request.sql,
          format: 'JSONEachRow',
          clickhouse_settings: request.settings,
          abort_signal: this.createAbortSignal(),
        })
      })

      queryLogger.info('JSONEachRow stream created successfully', {
        sql: request.sql,
      })

      // Use ClickHouse client's native stream() method
      return result.stream<T>()
    } catch (error) {
      queryLogger.error('JSONEachRow stream creation failed', {
        sql: request.sql,
        error: error instanceof Error ? error.message : String(error),
      })
      throw this.handleError(error, context)
    }
  }

  /**
   * Execute a query with CSV format for streaming
   * Uses ClickHouse client's native streaming with CSV format
   */
  async streamCSV(request: QueryRequest): Promise<NodeJS.ReadableStream> {
    const context = this.createContext(request)
    const queryLogger = this.logger.withQueryId(context.queryId).withOperation('streamCSV')

    queryLogger.debug('Starting CSV stream', {
      sql: request.sql,
      format: 'CSV',
    })

    try {
      const result = await queryLogger.timeAsync('csv_stream_creation', async () => {
        return await this.client.query({
          query: request.sql,
          format: 'CSV',
          clickhouse_settings: request.settings,
          abort_signal: this.createAbortSignal(),
        })
      })

      queryLogger.info('CSV stream created successfully', {
        sql: request.sql,
      })

      // Use ClickHouse client's native stream() method
      return result.stream()
    } catch (error) {
      queryLogger.error('CSV stream creation failed', {
        sql: request.sql,
        error: error instanceof Error ? error.message : String(error),
      })
      throw this.handleError(error, context)
    }
  }

  /**
   * Test the connection
   */
  async testConnection(): Promise<boolean> {
    try {
      await this.client.query({ query: 'SELECT 1' })
      return true
    } catch (error) {
      this.logger.error('testConnection', error)
      return false
    }
  }

  /**
   * Get server version
   */
  async getServerVersion(): Promise<string> {
    try {
      const result = await this.client.query({ query: 'SELECT version()' })
      const data = await result.json<Array<{ version(): string }>>()
      return data[0]['version()']
    } catch {
      return 'unknown'
    }
  }

  /**
   * Close the connection
   */
  async close(): Promise<void> {
    await this.client.close()
  }

  // Private methods

  private createContext(request: QueryRequest): QueryContext {
    return {
      queryId: this.generateQueryId(),
      timestamp: new Date(),
      settings: request.settings || {},
    }
  }

  private generateQueryId(): string {
    return `query_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
  }

  private createAbortSignal(): AbortSignal | undefined {
    if (this.options.timeout) {
      const controller = new AbortController()
      setTimeout(() => controller.abort(), this.options.timeout)
      return controller.signal
    }
    return undefined
  }

  private handleError(error: any, context: QueryContext): ClickHouseToolkitError {
    // Enhanced error handling with context
    if (error.name === 'AbortError' || error.code === 'ABORT_ERR') {
      return new CancelledError(`Query was cancelled: ${error.message}`, context.queryId)
    }

    if (error.code === 'ETIMEDOUT' || error.message?.includes('timeout')) {
      return createTimeoutError(`Query timed out: ${error.message}`, context.queryId, this.options.timeout)
    }

    if (error.code === 'ECONNREFUSED' || error.code === 'ENOTFOUND') {
      return createConnectionError(`Connection failed: ${error.message}`, context.queryId, this.options.url)
    }

    // Extract ClickHouse error code if available
    const clickhouseCode = error.code || error.error_code
    const sql = context.settings?.sql || 'N/A'

    return createQueryError(`ClickHouse query failed: ${error.message}`, context.queryId, clickhouseCode, sql)
  }
}

// Factory function
export function createQueryRunner(options: QueryRunnerOptions): QueryRunner {
  return new QueryRunner(options)
}
