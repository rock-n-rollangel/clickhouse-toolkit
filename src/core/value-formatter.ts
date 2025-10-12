/**
 * ValueFormatter - Type-aware SQL value formatting for direct value injection
 * Replaces parameter binding with direct value embedding in SQL strings
 */

export interface ValueFormatter {
  formatString(value: string): string
  formatNumber(value: number): string
  formatDate(value: Date): string
  formatArray(value: any[]): string
  formatBoolean(value: boolean): string
  formatNull(): string
  formatMap(value: Record<string, any>): string
}

export interface SQLValue {
  value: any
  type: 'string' | 'number' | 'date' | 'array' | 'boolean' | 'null'
}

export class ClickHouseValueFormatter implements ValueFormatter {
  /**
   * Format a string value with proper escaping
   */
  formatString(value: string): string {
    if (value === null || value === undefined) {
      return 'NULL'
    }

    // Escape single quotes by doubling them
    const escaped = value.replace(/'/g, "''")
    return `'${escaped}'`
  }

  /**
   * Format a number value (no quotes needed)
   */
  formatNumber(value: number): string {
    if (value === null || value === undefined || isNaN(value)) {
      return 'NULL'
    }

    // Handle special numeric values
    if (value === Infinity) return "'inf'"
    if (value === -Infinity) return "'-inf'"

    return value.toString()
  }

  /**
   * Format a Date value to ClickHouse date format
   */
  formatDate(value: Date): string {
    if (value === null || value === undefined) {
      return 'NULL'
    }

    if (!(value instanceof Date) || isNaN(value.getTime())) {
      throw new Error(`Invalid date value: ${value}`)
    }

    // Format as YYYY-MM-DD for date, YYYY-MM-DD HH:mm:ss for datetime
    const isoString = value.toISOString()
    return `'${isoString.replace('T', ' ').replace('Z', '').split('.')[0]}'`
  }

  /**
   * Format an array value using ClickHouse array syntax
   */
  formatArray(value: any[]): string {
    if (value === null || value === undefined) {
      return 'NULL'
    }

    if (!Array.isArray(value)) {
      throw new Error(`Expected array, got: ${typeof value}`)
    }

    const formattedValues = value.map((item) => this.formatValue(item))
    return `[${formattedValues.join(', ')}]`
  }

  /**
   * Format a boolean value to ClickHouse format (true/false)
   */
  formatBoolean(value: boolean): string {
    if (value === null || value === undefined) {
      return 'NULL'
    }

    return value ? 'true' : 'false'
  }

  /**
   * Format NULL value
   */
  formatNull(): string {
    return 'NULL'
  }

  /**
   * Format a Map (plain object) value using ClickHouse Map syntax
   */
  formatMap(value: Record<string, any>): string {
    if (value === null || value === undefined) {
      return 'NULL'
    }

    const entries = Object.entries(value).map(([key, val]) => {
      const formattedKey = this.formatString(key)
      const formattedValue = this.formatValue(val)
      return `${formattedKey}: ${formattedValue}`
    })

    return `{${entries.join(', ')}}`
  }

  /**
   * Main method to format any value based on its type
   */
  formatValue(value: any): string {
    // Handle null/undefined
    if (value === null || value === undefined) {
      return this.formatNull()
    }

    // Handle primitive types
    if (typeof value === 'string') {
      return this.formatString(value)
    }

    if (typeof value === 'number') {
      return this.formatNumber(value)
    }

    if (typeof value === 'boolean') {
      return this.formatBoolean(value)
    }

    if (value instanceof Date) {
      return this.formatDate(value)
    }

    if (Array.isArray(value)) {
      return this.formatArray(value)
    }

    // For plain objects (Maps), use ClickHouse Map syntax
    if (typeof value === 'object' && value.constructor === Object) {
      return this.formatMap(value)
    }

    // For other objects, try to convert to string
    if (typeof value === 'object') {
      try {
        const jsonString = JSON.stringify(value)
        return this.formatString(jsonString)
      } catch {
        return this.formatString(String(value))
      }
    }

    // Fallback to string representation
    return this.formatString(String(value))
  }

  /**
   * Inject values directly into SQL string, replacing ? placeholders
   */
  injectValues(sql: string, values: any[]): string {
    if (!values || values.length === 0) {
      return sql
    }

    let index = 0
    return sql.replace(/\?/g, () => {
      if (index >= values.length) {
        throw new Error(`Not enough values provided. Expected ${index + 1}, got ${values.length}`)
      }
      const value = values[index++]
      return this.formatValue(value)
    })
  }

  /**
   * Validate that all placeholders have corresponding values
   */
  validatePlaceholders(sql: string, values: any[]): void {
    const placeholderCount = (sql.match(/\?/g) || []).length
    if (placeholderCount !== values.length) {
      throw new Error(
        `Parameter count mismatch. SQL has ${placeholderCount} placeholders, but ${values.length} values provided`,
      )
    }
  }
}

// Singleton instance for global use
export const valueFormatter = new ClickHouseValueFormatter()

// Utility functions for backward compatibility
export function formatValue(value: any): string {
  return valueFormatter.formatValue(value)
}

export function injectValues(sql: string, values: any[]): string {
  return valueFormatter.injectValues(sql, values)
}

export function escapeString(value: string): string {
  return value.replace(/'/g, "''")
}
