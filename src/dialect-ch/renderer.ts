/**
 * ClickHouse-specific SQL renderer
 * Converts IR to SQL with ClickHouse-specific features
 */

import {
  QueryIR,
  ExprIR,
  RawPredicateIR,
  NormalizedPredicate,
  NormalizedPredicateNode,
  NormalizedAndPredicate,
  NormalizedOrPredicate,
  NormalizedNotPredicate,
} from '../core/ir'
import { Primitive } from '../core/ast'
import { createValidationError } from '../core/errors'
import { ClickHouseValueFormatter } from '../core/value-formatter'
import { Logger, LoggingComponent } from '../core/logger'

export class ClickHouseRenderer extends LoggingComponent {
  private valueFormatter = new ClickHouseValueFormatter()

  constructor(logger?: Logger) {
    super(logger, 'ClickHouseRenderer')
  }

  /**
   * Render a normalized query to SQL with direct value injection
   */
  render(query: QueryIR): { sql: string; params: Primitive[] } {
    switch (query.type) {
      case 'select':
        return this.renderSelect(query)
      case 'insert':
        return this.renderInsert(query)
      case 'update':
        return this.renderUpdate(query)
      case 'delete':
        return this.renderDelete(query)
      default:
        throw createValidationError(
          `Unsupported query type: ${(query as any).type}`,
          undefined,
          'queryType',
          (query as any).type,
        )
    }
  }

  private renderSelect(query: QueryIR): { sql: string; params: Primitive[] } {
    let sql = 'SELECT '

    // Columns
    if (query.columns && query.columns.length > 0) {
      sql += query.columns
        .map((col) => {
          const columnExpr = this.renderExpression(col, 'select')
          return col.alias ? `${columnExpr} AS ${this.quoteIdentifier(col.alias)}` : columnExpr
        })
        .join(', ')
    } else {
      sql += '*'
    }

    // FROM
    if (query.table) {
      const tableName = this.quoteIdentifier(query.table)
      const alias = query.tableAlias ? ` AS ${this.quoteIdentifier(query.tableAlias)}` : ''
      sql += ` FROM ${tableName}${alias}`
    }

    // JOIN clauses
    if (query.joins && query.joins.length > 0) {
      for (const join of query.joins) {
        const joinType = join.type.toUpperCase()
        const tableName = this.quoteIdentifier(join.table)
        const alias = join.alias ? ` AS ${this.quoteIdentifier(join.alias)}` : ''
        const onClause = this.renderPredicateNode(join.on)
        sql += ` ${joinType} JOIN ${tableName}${alias} ON ${onClause}`
      }
    }

    // PREWHERE
    const prewherePredicates = query.predicates.filter((p) => 'isPrewhere' in p && p.isPrewhere)
    if (prewherePredicates.length > 0) {
      sql += ' PREWHERE ' + this.renderPredicates(prewherePredicates)
    }

    // WHERE
    const wherePredicates = query.predicates.filter((p) => !('isPrewhere' in p && p.isPrewhere))
    if (wherePredicates.length > 0) {
      sql += ' WHERE ' + this.renderPredicates(wherePredicates, true)
    }

    // GROUP BY
    if (query.groupBy && query.groupBy.length > 0) {
      sql += ' GROUP BY ' + query.groupBy.map((col) => this.quoteIdentifier(col)).join(', ')
    }

    // HAVING
    if (query.having && query.having.length > 0) {
      sql += ' HAVING ' + this.renderPredicates(query.having)
    }

    // ORDER BY
    if (query.orderBy && query.orderBy.length > 0) {
      sql += ' ORDER BY ' + query.orderBy.map((o) => `${this.quoteIdentifier(o.column)} ${o.direction}`).join(', ')
    }

    // LIMIT
    if (query.limit) {
      sql += ` LIMIT ${query.limit}`
      if (query.offset) {
        sql += ` OFFSET ${query.offset}`
      }
    }

    // FINAL
    if (query.final) {
      sql += ' FINAL'
    }

    // SETTINGS
    if (query.settings) {
      const settings = Object.entries(query.settings)
        .map(([key, value]) => `${key} = ${this.formatSettingValue(value)}`)
        .join(', ')
      sql += ` SETTINGS ${settings}`
    }

    return { sql, params: [] }
  }

  private renderInsert(query: QueryIR): { sql: string; params: Primitive[] } {
    let sql = `INSERT INTO ${this.quoteIdentifier(query.table)}`

    if (query.columns && query.columns.length > 0) {
      sql += ` (${query.columns
        .map((col) => {
          if (col.exprType === 'column') {
            // TypeScript knows col.columnName exists for 'column' type
            const colName = col.tableName ? `${col.tableName}.${col.columnName}` : col.columnName
            return this.quoteIdentifier(colName)
          }
          throw createValidationError(
            'INSERT column list only supports column references, not expressions',
            undefined,
            'column',
            col.exprType,
          )
        })
        .join(', ')})`
    }

    sql += ' VALUES'

    // This is a simplified version - in practice, you'd want to handle
    // different value formats (VALUES, JSONEachRow, etc.)
    const values = query.values || []
    if (values.length > 0) {
      const valueRows = values.map(
        (row: any[]) => `(${row.map((val) => this.valueFormatter.formatValue(val)).join(', ')})`,
      )
      sql += ' ' + valueRows.join(', ')
    }

    return { sql, params: [] }
  }

  private renderUpdate(query: QueryIR): { sql: string; params: Primitive[] } {
    let sql = `ALTER TABLE ${this.quoteIdentifier(query.table)} UPDATE`

    // UPDATE clause (ClickHouse doesn't use SET keyword for ALTER TABLE UPDATE)
    if (query.set && Object.keys(query.set).length > 0) {
      const updateClauses = Object.entries(query.set)
        .map(([column, value]) => `${this.quoteIdentifier(column)} = ${this.valueFormatter.formatValue(value)}`)
        .join(', ')
      sql += ` ${updateClauses}`
    }

    if (query.predicates.length > 0) {
      sql += ' WHERE ' + this.renderPredicates(query.predicates, true)
    }

    if (query.settings) {
      const settings = Object.entries(query.settings)
        .map(([key, value]) => `${key} = ${this.formatSettingValue(value)}`)
        .join(', ')
      sql += ` SETTINGS ${settings}`
    }

    return { sql, params: [] }
  }

  private renderDelete(query: QueryIR): { sql: string; params: Primitive[] } {
    let sql = `ALTER TABLE ${this.quoteIdentifier(query.table)} DELETE`

    if (query.predicates.length > 0) {
      sql += ' WHERE ' + this.renderPredicates(query.predicates, true)
    }

    if (query.settings) {
      const settings = Object.entries(query.settings)
        .map(([key, value]) => `${key} = ${this.formatSettingValue(value)}`)
        .join(', ')
      sql += ` SETTINGS ${settings}`
    }

    return { sql, params: [] }
  }

  private renderPredicates(predicates: NormalizedPredicateNode[], isTopLevel: boolean = false): string {
    if (predicates.length === 1) {
      return this.renderPredicateNode(predicates[0], isTopLevel)
    }

    return predicates.map((p) => this.renderPredicateNode(p, false)).join(' AND ')
  }

  private renderPredicateNode(predicate: NormalizedPredicateNode, isTopLevel: boolean = false): string {
    switch (predicate.type) {
      case 'predicate':
        return this.renderPredicate(predicate)
      case 'and':
        return this.renderAndPredicate(predicate, isTopLevel)
      case 'or':
        return this.renderOrPredicate(predicate)
      case 'not':
        return this.renderNotPredicate(predicate)
      case 'raw_predicate':
        // Raw SQL - pass through unescaped
        return (predicate as RawPredicateIR).sql
      default:
        throw createValidationError(
          `Unsupported predicate type: ${(predicate as any).type}`,
          undefined,
          'predicateType',
          (predicate as any).type,
        )
    }
  }

  private renderPredicate(predicate: NormalizedPredicate): string {
    const left = predicate.left ? this.quoteIdentifier(predicate.left, 'predicate') : ''
    const right = this.formatPredicateRight(predicate.right)

    // In case it's a HAS ANY/ALL/IN TUPLE, we need to quote the i
    const i = this.quoteIdentifier('i')

    switch (predicate.operator) {
      case '=':
        return `${left} = ${right}`
      case '!=':
        return `${left} != ${right}`
      case '>':
        return `${left} > ${right}`
      case '>=':
        return `${left} >= ${right}`
      case '<':
        return `${left} < ${right}`
      case '<=':
        return `${left} <= ${right}`
      case 'IN':
        return `${left} IN ${right}`
      case 'NOT IN':
        return `${left} NOT IN ${right}`
      case 'EXISTS':
        return `EXISTS ${right}`
      case 'NOT EXISTS':
        return `NOT EXISTS ${right}`
      case 'BETWEEN':
        const [a, b] = predicate.right as Primitive[]
        return `${left} BETWEEN ${this.formatPredicateRight(a)} AND ${this.formatPredicateRight(b)}`
      case 'LIKE':
        return `${left} LIKE ${right}`
      case 'ILIKE':
        return `${left} ILIKE ${right}`
      case 'IS NULL':
        return `${left} IS NULL`
      case 'IS NOT NULL':
        return `${left} IS NOT NULL`
      case 'HAS ANY':
        return `arrayExists(${i} -> (${i} IN ${right}), ${left})`
      case 'HAS ALL':
        return `arrayAll(${i} -> (${i} IN ${right}), ${left})`
      case 'IN TUPLE':
        return `arrayAll(${i} -> (${i} IN ${right}), ${left})`
      default:
        throw createValidationError(
          `Unsupported operator: ${predicate.operator}`,
          undefined,
          'operator',
          predicate.operator,
        )
    }
  }

  private formatPredicateRight(right: Primitive | Primitive[] | any): string {
    // Handle subqueries
    if (right && typeof right === 'object' && right.__subquery) {
      const subquerySQL = this.renderSelect(right.__subquery).sql
      return `(${subquerySQL})`
    }

    // Handle column references in JOIN ON clauses
    if (right && typeof right === 'object' && right.type === 'column') {
      return this.quoteIdentifier(right.table ? `${right.table}.${right.name}` : right.name, 'predicate')
    }

    if (Array.isArray(right)) {
      if (right.length === 0) {
        return '(SELECT 1 WHERE 0=1)' // Empty IN clause
      }

      const formatted = right.map((val) => this.valueFormatter.formatValue(val))
      return `(${formatted.join(', ')})`
    }

    return this.valueFormatter.formatValue(right)
  }

  private renderAndPredicate(predicate: NormalizedAndPredicate, isTopLevel: boolean = false): string {
    const rendered = predicate.predicates.map((p) => this.renderPredicateNode(p)).join(' AND ')
    // Add parentheses for AND predicates created from combinators (And() function)
    // but only when they contain simple predicates (not nested OR/NOT) or when not at top level
    if (predicate.fromCombinator && predicate.predicates.length > 1) {
      const hasComplexNestedPredicates = predicate.predicates.some(
        (p) => p.type === 'or' || p.type === 'not' || (p.type === 'and' && p.fromCombinator),
      )

      // Add parentheses if:
      // 1. Not at top level, OR
      // 2. At top level but contains only simple predicates (no nested OR/NOT)
      if (!isTopLevel || !hasComplexNestedPredicates) {
        return `(${rendered})`
      }
    }
    return rendered
  }

  private renderOrPredicate(predicate: NormalizedOrPredicate): string {
    const rendered = predicate.predicates.map((p) => this.renderPredicateNode(p)).join(' OR ')
    return `(${rendered})`
  }

  private renderNotPredicate(predicate: NormalizedNotPredicate): string {
    const rendered = this.renderPredicateNode(predicate.predicate)
    return `NOT (${rendered})`
  }

  private formatSettingValue(value: any): string {
    return this.valueFormatter.formatValue(value)
  }

  /**
   * Render expression from structured IR
   */
  private renderExpression(expr: ExprIR, context: 'select' | 'predicate' | 'function' = 'select'): string {
    switch (expr.exprType) {
      case 'column':
        // TypeScript knows expr.columnName exists for 'column' type
        const colName = expr.tableName ? `${expr.tableName}.${expr.columnName}` : expr.columnName
        // Don't quote column names when they're function arguments
        if (context === 'function') {
          return colName
        }
        return this.quoteIdentifier(colName, context)

      case 'raw':
        // TypeScript knows expr.rawSql exists for 'raw' type
        return expr.rawSql

      case 'function':
        // TypeScript knows expr.functionName and expr.functionArgs exist for 'function' type
        const args = expr.functionArgs.map((arg) => this.renderExpression(arg, 'function')).join(', ')

        // Special handling for CAST
        if (expr.functionName === 'cast' && expr.functionArgs.length === 2) {
          const value = this.renderExpression(expr.functionArgs[0], 'function')
          const type = this.renderExpression(expr.functionArgs[1], 'function')
          return `CAST(${value} AS ${type})`
        }

        return `${expr.functionName}(${args})`

      case 'case':
        // TypeScript knows expr.caseCases exists for 'case' type
        const cases = expr.caseCases
          .map((c) => {
            const condition = this.renderPredicateNode(c.condition)
            const thenVal = this.renderExpression(c.then, 'select')
            return `WHEN ${condition} THEN ${thenVal}`
          })
          .join(' ')

        const elseClause = expr.caseElse ? ` ELSE ${this.renderExpression(expr.caseElse, 'select')}` : ''

        return `CASE ${cases}${elseClause} END`

      case 'subquery':
        // TypeScript knows expr.subquery exists for 'subquery' type
        const subquerySql = this.renderSelect(expr.subquery).sql
        return `(${subquerySql})`

      case 'value':
        // TypeScript knows expr.value exists for 'value' type
        return this.valueFormatter.formatValue(expr.value)

      case 'array':
        // TypeScript knows expr.values exists for 'array' type
        const formatted = expr.values.map((v) => this.valueFormatter.formatValue(v))
        return `(${formatted.join(', ')})`

      case 'tuple':
        // TypeScript knows expr.values exists for 'tuple' type
        const tupleVals = expr.values.map((v) => this.valueFormatter.formatValue(v))
        return `(${tupleVals.join(', ')})`

      default:
        throw createValidationError(
          `Unsupported expression type: ${(expr as any).exprType}`,
          undefined,
          'exprType',
          (expr as any).exprType,
        )
    }
  }

  /**
   * Render column expression (may be a simple column name or a function call) - DEPRECATED
   */
  private renderColumnExpression(expr: string): string {
    // Use quoteIdentifier which already handles functions vs simple columns
    return this.quoteIdentifier(expr, 'select')
  }

  private quoteIdentifier(identifier: string, context: 'select' | 'predicate' = 'select'): string {
    // Allow special identifiers like * (but quote function calls in predicates)
    if (identifier === '*') {
      return identifier
    }

    // Don't quote numeric literals (for EXISTS queries: SELECT 1 FROM ...)
    if (/^\d+(\.\d+)?$/.test(identifier)) {
      return identifier
    }

    // For function calls, quote only in predicate context (WHERE, HAVING)
    if (identifier.includes('(')) {
      if (context === 'predicate') {
        return `\`${identifier}\``
      } else {
        return identifier
      }
    }

    // Allow table.column format
    if (identifier.includes('.')) {
      const parts = identifier.split('.')
      if (parts.length !== 2) {
        throw createValidationError(
          `Invalid identifier: '${identifier}' - table.column format expected`,
          undefined,
          'identifier',
          identifier,
        )
      }
      // Validate each part
      parts.forEach((part) => {
        if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(part)) {
          throw createValidationError(
            `Invalid identifier: '${identifier}' contains invalid characters`,
            undefined,
            'identifier',
            identifier,
          )
        }
      })
      // Quote each part separately
      return `\`${parts[0]}\`.\`${parts[1]}\``
    }

    // Allow any identifier - escape it with backticks to prevent SQL injection
    // This is safer than validation as it neutralizes malicious SQL

    // Use backticks for quoting (ClickHouse supports both backticks and double quotes)
    return `\`${identifier}\``
  }
}
