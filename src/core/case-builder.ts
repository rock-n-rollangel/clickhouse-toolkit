/**
 * CASE WHEN THEN builder helper
 */

import { WhereInput, Operator } from './operators'
import { PredicateNode, Expr, CaseExpr } from './ast'
import { operatorToPredicate, parseColumnRef, isPredicateNode } from './predicate-builder'
import { createValidationError } from './errors'

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
