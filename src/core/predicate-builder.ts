/**
 * Shared utility functions for building predicates from operators
 * Eliminates duplication across different query builders
 */

import { ColumnRef, PredicateNode } from './ast'
import { Operator } from './operators'
import { createValidationError } from './errors'

/**
 * Parses a column string into a ColumnRef object
 * Handles both simple column names and table.column references
 */
export function parseColumnRef(column: string): ColumnRef {
  if (column.includes('.')) {
    const [table, name] = column.split('.', 2)
    return { type: 'column', name, table }
  }
  return { type: 'column', name: column }
}

/**
 * Convert value to appropriate AST node
 */
function valueToExpr(value: any): any {
  // Check if it's a SelectBuilder (subquery)
  if (value && typeof value === 'object' && value.constructor && value.constructor.name === 'SelectBuilder') {
    return { type: 'subquery', query: value }
  }
  return { type: 'value', value: value as any }
}

/**
 * Converts an operator to a predicate node
 * Supports all standard SQL operators including ClickHouse-specific ones
 */
export function operatorToPredicate(
  column: string,
  operator: Operator,
  parseColumnRefFn: (col: string) => ColumnRef = parseColumnRef,
): PredicateNode {
  const columnRef = parseColumnRefFn(column)

  switch (operator.type) {
    case 'eq':
      return {
        type: 'predicate',
        left: columnRef,
        operator: '=',
        right: valueToExpr(operator.value),
      }
    case 'eq_col':
      return {
        type: 'predicate',
        left: columnRef,
        operator: '=',
        right: parseColumnRefFn(operator.value as string),
      }
    case 'ne':
      return {
        type: 'predicate',
        left: columnRef,
        operator: '!=',
        right: valueToExpr(operator.value),
      }
    case 'gt':
      return {
        type: 'predicate',
        left: columnRef,
        operator: '>',
        right: valueToExpr(operator.value),
      }
    case 'gte':
      return {
        type: 'predicate',
        left: columnRef,
        operator: '>=',
        right: valueToExpr(operator.value),
      }
    case 'lt':
      return {
        type: 'predicate',
        left: columnRef,
        operator: '<',
        right: valueToExpr(operator.value),
      }
    case 'lte':
      return {
        type: 'predicate',
        left: columnRef,
        operator: '<=',
        right: valueToExpr(operator.value),
      }
    case 'in':
      // Check if value is a subquery
      if (
        operator.value &&
        typeof operator.value === 'object' &&
        operator.value.constructor &&
        operator.value.constructor.name === 'SelectBuilder'
      ) {
        return {
          type: 'predicate',
          left: columnRef,
          operator: 'IN',
          right: { type: 'subquery', query: operator.value },
        }
      }
      return {
        type: 'predicate',
        left: columnRef,
        operator: 'IN',
        right: { type: 'array', values: operator.value as any[] },
      }
    case 'not_in':
      return {
        type: 'predicate',
        left: columnRef,
        operator: 'NOT IN',
        right: { type: 'array', values: operator.value as any[] },
      }
    case 'between':
      const [start, end] = operator.value as [any, any]
      return {
        type: 'predicate',
        left: columnRef,
        operator: 'BETWEEN',
        right: { type: 'tuple', values: [start, end] },
      }
    case 'like':
      return {
        type: 'predicate',
        left: columnRef,
        operator: 'LIKE',
        right: { type: 'value', value: operator.value as any },
      }
    case 'ilike':
      return {
        type: 'predicate',
        left: columnRef,
        operator: 'ILIKE',
        right: { type: 'value', value: operator.value as any },
      }
    case 'is_null':
      return {
        type: 'predicate',
        left: columnRef,
        operator: 'IS NULL',
        right: { type: 'value', value: null },
      }
    case 'is_not_null':
      return {
        type: 'predicate',
        left: columnRef,
        operator: 'IS NOT NULL',
        right: { type: 'value', value: null },
      }
    case 'has_any':
      return {
        type: 'predicate',
        left: columnRef,
        operator: 'HAS ANY',
        right: { type: 'array', values: operator.value as any[] },
      }
    case 'has_all':
      return {
        type: 'predicate',
        left: columnRef,
        operator: 'HAS ALL',
        right: { type: 'array', values: operator.value as any[] },
      }
    case 'in_tuple':
      return {
        type: 'predicate',
        left: columnRef,
        operator: 'IN TUPLE',
        right: { type: 'array', values: operator.value as any[] },
      }
    case 'exists':
      // EXISTS doesn't have a left side - it's a standalone predicate
      if (
        !operator.value ||
        typeof operator.value !== 'object' ||
        !operator.value.constructor ||
        operator.value.constructor.name !== 'SelectBuilder'
      ) {
        throw createValidationError(
          'EXISTS operator requires a SelectBuilder subquery',
          undefined,
          'operator',
          'exists',
        )
      }
      return {
        type: 'predicate',
        left: { type: 'column', name: '' }, // Empty for EXISTS
        operator: 'EXISTS',
        right: { type: 'subquery', query: operator.value },
      }
    case 'not_exists':
      // NOT EXISTS doesn't have a left side
      if (
        !operator.value ||
        typeof operator.value !== 'object' ||
        !operator.value.constructor ||
        operator.value.constructor.name !== 'SelectBuilder'
      ) {
        throw createValidationError(
          'NOT EXISTS operator requires a SelectBuilder subquery',
          undefined,
          'operator',
          'not_exists',
        )
      }
      return {
        type: 'predicate',
        left: { type: 'column', name: '' }, // Empty for NOT EXISTS
        operator: 'NOT EXISTS',
        right: { type: 'subquery', query: operator.value },
      }
    default:
      throw createValidationError(`Unsupported operator type: ${operator.type}`, undefined, 'operator', operator.type)
  }
}
