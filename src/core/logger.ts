/**
 * Structured logging system for ClickHouse Toolkit
 * Supports multiple log levels, structured data, and external loggers
 */

export type LogLevel = 'debug' | 'info' | 'warn' | 'error'

export interface LogEntry {
  level: LogLevel
  message: string
  timestamp: Date
  context?: Record<string, any>
  queryId?: string
  component?: string
  operation?: string
  duration?: number
  error?: Error
}

export interface Logger {
  debug(message: string, context?: Record<string, any>): void
  info(message: string, context?: Record<string, any>): void
  warn(message: string, context?: Record<string, any>): void
  error(message: string, context?: Record<string, any>): void
}

export interface LoggerOptions {
  level?: LogLevel
  component?: string
  includeTimestamp?: boolean
  includeQueryId?: boolean
  formatter?: (entry: LogEntry) => string
}

// Default console logger
export class ConsoleLogger implements Logger {
  private options: LoggerOptions

  constructor(options: LoggerOptions = {}) {
    this.options = {
      level: 'info',
      component: 'clickhouse-toolkit',
      includeTimestamp: true,
      includeQueryId: true,
      ...options,
    }
  }

  debug(message: string, context?: Record<string, any>): void {
    this.log('debug', message, context)
  }

  info(message: string, context?: Record<string, any>): void {
    this.log('info', message, context)
  }

  warn(message: string, context?: Record<string, any>): void {
    this.log('warn', message, context)
  }

  error(message: string, context?: Record<string, any>): void {
    this.log('error', message, context)
  }

  private log(level: LogLevel, message: string, context?: Record<string, any>): void {
    if (!this.shouldLog(level)) return

    const entry: LogEntry = {
      level,
      message,
      timestamp: new Date(),
      context,
      component: this.options.component,
    }

    const formatted = this.options.formatter ? this.options.formatter(entry) : this.formatEntry(entry)

    switch (level) {
      case 'debug':
        console.debug(formatted)
        break
      case 'info':
        console.info(formatted)
        break
      case 'warn':
        console.warn(formatted)
        break
      case 'error':
        console.error(formatted)
        break
    }
  }

  private shouldLog(level: LogLevel): boolean {
    const levels: LogLevel[] = ['debug', 'info', 'warn', 'error']
    const currentLevelIndex = levels.indexOf(this.options.level!)
    const messageLevelIndex = levels.indexOf(level)
    return messageLevelIndex >= currentLevelIndex
  }

  private formatEntry(entry: LogEntry): string {
    const parts: string[] = []

    if (this.options.includeTimestamp) {
      parts.push(`[${entry.timestamp.toISOString()}]`)
    }

    parts.push(`[${entry.level.toUpperCase()}]`)
    parts.push(`[${entry.component}]`)

    if (entry.queryId && this.options.includeQueryId) {
      parts.push(`[${entry.queryId}]`)
    }

    parts.push(entry.message)

    if (entry.context && Object.keys(entry.context).length > 0) {
      parts.push(JSON.stringify(entry.context))
    }

    return parts.join(' ')
  }
}

// No-op logger for production
export class NoOpLogger implements Logger {
  debug(message: string, context?: Record<string, any>): void {}
  info(message: string, context?: Record<string, any>): void {}
  warn(message: string, context?: Record<string, any>): void {}
  error(message: string, context?: Record<string, any>): void {}
}

// Structured logger for external systems
export class StructuredLogger implements Logger {
  private options: LoggerOptions
  private externalLogger?: (entry: LogEntry) => void

  constructor(options: LoggerOptions & { externalLogger?: (entry: LogEntry) => void } = {}) {
    this.options = {
      level: 'info',
      component: 'clickhouse-toolkit',
      includeTimestamp: true,
      includeQueryId: true,
      ...options,
    }
    this.externalLogger = options.externalLogger
  }

  debug(message: string, context?: Record<string, any>): void {
    this.log('debug', message, context)
  }

  info(message: string, context?: Record<string, any>): void {
    this.log('info', message, context)
  }

  warn(message: string, context?: Record<string, any>): void {
    this.log('warn', message, context)
  }

  error(message: string, context?: Record<string, any>): void {
    this.log('error', message, context)
  }

  private log(level: LogLevel, message: string, context?: Record<string, any>): void {
    if (!this.shouldLog(level)) return

    const entry: LogEntry = {
      level,
      message,
      timestamp: new Date(),
      context,
      component: this.options.component,
    }

    if (this.externalLogger) {
      this.externalLogger(entry)
    } else {
      // Fallback to console with structured output
      console.log(JSON.stringify(entry))
    }
  }

  private shouldLog(level: LogLevel): boolean {
    const levels: LogLevel[] = ['debug', 'info', 'warn', 'error']
    const currentLevelIndex = levels.indexOf(this.options.level!)
    const messageLevelIndex = levels.indexOf(level)
    return messageLevelIndex >= currentLevelIndex
  }
}

// Logger factory
export function createLogger(options?: LoggerOptions): Logger {
  if (process.env.NODE_ENV === 'production' && !process.env.CLICKHOUSE_DEBUG) {
    return new NoOpLogger()
  }

  return new ConsoleLogger(options)
}

// Logger context for request-scoped logging
export class LoggerContext {
  private logger: Logger
  private context: Record<string, any>

  constructor(logger: Logger, context: Record<string, any> = {}) {
    this.logger = logger
    this.context = context
  }

  withContext(additionalContext: Record<string, any>): LoggerContext {
    return new LoggerContext(this.logger, { ...this.context, ...additionalContext })
  }

  withQueryId(queryId: string): LoggerContext {
    return this.withContext({ queryId })
  }

  withComponent(component: string): LoggerContext {
    return this.withContext({ component })
  }

  withOperation(operation: string): LoggerContext {
    return this.withContext({ operation })
  }

  debug(message: string, context?: Record<string, any>): void {
    this.logger.debug(message, { ...this.context, ...context })
  }

  info(message: string, context?: Record<string, any>): void {
    this.logger.info(message, { ...this.context, ...context })
  }

  warn(message: string, context?: Record<string, any>): void {
    this.logger.warn(message, { ...this.context, ...context })
  }

  error(message: string, context?: Record<string, any>): void {
    this.logger.error(message, { ...this.context, ...context })
  }

  time<T>(operation: string, fn: () => T): T {
    const start = Date.now()
    this.debug(`Starting ${operation}`)

    try {
      const result = fn()
      const duration = Date.now() - start
      this.info(`Completed ${operation}`, { duration })
      return result
    } catch (error) {
      const duration = Date.now() - start
      this.error(`Failed ${operation}`, { duration, error })
      throw error
    }
  }

  async timeAsync<T>(operation: string, fn: () => Promise<T>): Promise<T> {
    const start = Date.now()
    this.debug(`Starting ${operation}`)

    try {
      const result = await fn()
      const duration = Date.now() - start
      this.info(`Completed ${operation}`, { duration })
      return result
    } catch (error) {
      const duration = Date.now() - start
      this.error(`Failed ${operation}`, { duration, error })
      throw error
    }
  }
}

// Global logger instance
let globalLogger: Logger = createLogger()

export function setGlobalLogger(logger: Logger): void {
  globalLogger = logger
}

export function getGlobalLogger(): Logger {
  return globalLogger
}

export function createLoggerContext(context?: Record<string, any>): LoggerContext {
  return new LoggerContext(globalLogger, context)
}

// Base class for all components that need logging
export abstract class LoggingComponent {
  protected logger: Logger

  constructor(logger?: Logger, componentName?: string) {
    this.logger =
      logger ||
      createLoggerContext({
        component: componentName || this.constructor.name,
      })
  }
}
