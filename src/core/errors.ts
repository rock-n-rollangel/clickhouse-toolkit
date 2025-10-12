/**
 * Typed error system for ClickHouse Toolkit
 * Machine-readable error codes with context
 */

export abstract class ClickHouseToolkitError extends Error {
  abstract readonly code: string
  abstract readonly type: string
  readonly queryId?: string
  readonly timestamp: Date

  constructor(message: string, queryId?: string) {
    super(message)
    this.name = this.constructor.name
    this.timestamp = new Date()
    this.queryId = queryId
  }

  toJSON() {
    return {
      name: this.name,
      code: this.code,
      type: this.type,
      message: this.message,
      queryId: this.queryId,
      timestamp: this.timestamp.toISOString(),
    }
  }
}

// Query execution errors
export class QueryError extends ClickHouseToolkitError {
  readonly code = 'QUERY_ERROR'
  readonly type = 'execution'
  readonly clickhouseCode?: string
  readonly sql?: string

  constructor(message: string, queryId?: string, clickhouseCode?: string, sql?: string) {
    super(message, queryId)
    this.clickhouseCode = clickhouseCode
    this.sql = sql
  }
}

export class TimeoutError extends ClickHouseToolkitError {
  readonly code = 'TIMEOUT_ERROR'
  readonly type = 'execution'
  readonly timeoutMs?: number

  constructor(message: string, queryId?: string, timeoutMs?: number) {
    super(message, queryId)
    this.timeoutMs = timeoutMs
  }
}

export class CancelledError extends ClickHouseToolkitError {
  readonly code = 'CANCELLED_ERROR'
  readonly type = 'execution'

  constructor(message: string, queryId?: string) {
    super(message, queryId)
  }
}

// Validation errors
export class ValidationError extends ClickHouseToolkitError {
  readonly code = 'VALIDATION_ERROR'
  readonly type = 'validation'
  readonly field?: string
  readonly value?: any

  constructor(message: string, queryId?: string, field?: string, value?: any) {
    super(message, queryId)
    this.field = field
    this.value = value
  }
}

export class IdentifierError extends ClickHouseToolkitError {
  readonly code = 'IDENTIFIER_ERROR'
  readonly type = 'validation'
  readonly identifier: string

  constructor(message: string, identifier: string, queryId?: string) {
    super(message, queryId)
    this.identifier = identifier
  }
}

export class ParameterError extends ClickHouseToolkitError {
  readonly code = 'PARAMETER_ERROR'
  readonly type = 'validation'
  readonly parameter: string

  constructor(message: string, parameter: string, queryId?: string) {
    super(message, queryId)
    this.parameter = parameter
  }
}

// Migration errors
export class MigrationError extends ClickHouseToolkitError {
  readonly code = 'MIGRATION_ERROR'
  readonly type = 'migration'
  readonly migrationId?: string
  readonly step?: string

  constructor(message: string, queryId?: string, migrationId?: string, step?: string) {
    super(message, queryId)
    this.migrationId = migrationId
    this.step = step
  }
}

export class DriftError extends ClickHouseToolkitError {
  readonly code = 'DRIFT_ERROR'
  readonly type = 'migration'
  readonly differences: Array<{
    type: string
    table: string
    column?: string
    expected?: any
    actual?: any
  }>

  constructor(message: string, differences: any[], queryId?: string) {
    super(message, queryId)
    this.differences = differences
  }
}

// Connection errors
export class ConnectionError extends ClickHouseToolkitError {
  readonly code = 'CONNECTION_ERROR'
  readonly type = 'connection'
  readonly url?: string

  constructor(message: string, queryId?: string, url?: string) {
    super(message, queryId)
    this.url = url
  }
}

// Schema errors
export class SchemaError extends ClickHouseToolkitError {
  readonly code = 'SCHEMA_ERROR'
  readonly type = 'schema'
  readonly table?: string
  readonly column?: string

  constructor(message: string, queryId?: string, table?: string, column?: string) {
    super(message, queryId)
    this.table = table
    this.column = column
  }
}

// Error factory functions
export function createQueryError(message: string, queryId?: string, clickhouseCode?: string, sql?: string): QueryError {
  return new QueryError(message, queryId, clickhouseCode, sql)
}

export function createTimeoutError(message: string, queryId?: string, timeoutMs?: number): TimeoutError {
  return new TimeoutError(message, queryId, timeoutMs)
}

export function createValidationError(message: string, queryId?: string, field?: string, value?: any): ValidationError {
  return new ValidationError(message, queryId, field, value)
}

export function createIdentifierError(message: string, identifier: string, queryId?: string): IdentifierError {
  return new IdentifierError(message, identifier, queryId)
}

export function createParameterError(message: string, parameter: string, queryId?: string): ParameterError {
  return new ParameterError(message, parameter, queryId)
}

export function createMigrationError(
  message: string,
  queryId?: string,
  migrationId?: string,
  step?: string,
): MigrationError {
  return new MigrationError(message, queryId, migrationId, step)
}

export function createConnectionError(message: string, queryId?: string, url?: string): ConnectionError {
  return new ConnectionError(message, queryId, url)
}

export function createSchemaError(message: string, queryId?: string, table?: string, column?: string): SchemaError {
  return new SchemaError(message, queryId, table, column)
}
