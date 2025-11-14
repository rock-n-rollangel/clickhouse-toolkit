/**
 * CASE WHEN THEN builder helper
 */

import { WhereInput, Operator } from './operators'
import { PredicateNode, Expr, CaseExpr } from './ast'
import { operatorToPredicate, parseColumnRef } from './predicate-builder'
import { createValidationError } from './errors'

/**
 * Checks if an object is a valid PredicateNode by verifying:
 * 1. It has a 'type' property
 * 2. The type is one of the valid PredicateNode types
 * 3. The object has the expected structure for that type
 */
function isPredicateNode(obj: any): obj is PredicateNode {
  if (!obj || typeof obj !== 'object') {
    return false
  }

  // Check if it has a 'type' property
  if (!('type' in obj) || typeof obj.type !== 'string') {
    return false
  }

  const type = obj.type

  // Check if type is one of the valid PredicateNode types
  switch (type) {
    case 'predicate':
      // Predicate must have left, operator, and right
      return 'left' in obj && 'operator' in obj && 'right' in obj
    case 'and':
    case 'or':
      // And/Or must have predicates array
      return 'predicates' in obj && Array.isArray(obj.predicates)
    case 'not':
      // Not must have predicate
      return 'predicate' in obj
    case 'raw_predicate':
      // RawPredicate must have sql
      return 'sql' in obj && typeof obj.sql === 'string'
    default:
      // Not a valid PredicateNode type
      return false
  }
}

export class CaseBuilder {
  private cases: Array<{ condition: PredicateNode; then: Expr }> = []
  private maxDepth: number = 0

  when(condition: WhereInput | PredicateNode, thenExpr: Expr): this {
    // Check for nested case expressions and validate depth
    if (thenExpr.type === 'case') {
      this.maxDepth++
      if (this.maxDepth > 10) {
        throw createValidationError('Maximum CASE nesting depth (10) exceeded', undefined, 'case', 'nesting')
      }
    }

    // Convert condition to PredicateNode
    let predicateNode: PredicateNode
    if (isPredicateNode(condition)) {
      predicateNode = condition
    } else {
      predicateNode = this.convertWhereInputToPredicate(condition as WhereInput)
    }

    this.cases.push({ condition: predicateNode, then: thenExpr })
    return this
  }

  else(elseExpr: Expr): CaseExpr {
    return {
      type: 'case',
      cases: this.cases,
      else: elseExpr,
    }
  }

  private convertWhereInputToPredicate(input: WhereInput): PredicateNode {
    const entries = Object.entries(input)
    if (entries.length === 1) {
      const [column, operator] = entries[0]
      return operatorToPredicate(column, operator as Operator, parseColumnRef)
    }
    return {
      type: 'and',
      predicates: entries.map(([col, op]) => operatorToPredicate(col, op as Operator, parseColumnRef)),
    }
  }
}

export function Case(): CaseBuilder {
  return new CaseBuilder()
}

export { CaseExpr }
