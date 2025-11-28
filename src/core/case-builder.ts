/**
 * CASE WHEN THEN builder helper
 */

import { WhereInput, Operator } from './operators'
import { PredicateNode, Expr, CaseExpr, SelectNode } from './ast'
import { operatorToPredicate, parseColumnRef, isPredicateNode } from './predicate-builder'
import { createValidationError } from './errors'

export class CaseBuilder {
  private cases: Array<{ condition: PredicateNode; then: Expr }> = []
  private maxDepth: number = 0

  when(condition: WhereInput | PredicateNode, thenExpr: Expr | SelectBuilderLike): this {
    const normalizedThen = this.normalizeExprInput(thenExpr)

    // Check for nested case expressions and validate depth
    if (normalizedThen.type === 'case') {
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

    this.cases.push({ condition: predicateNode, then: normalizedThen })
    return this
  }

  else(elseExpr: Expr | SelectBuilderLike): CaseExpr {
    return {
      type: 'case',
      cases: this.cases,
      else: this.normalizeExprInput(elseExpr),
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

  private normalizeExprInput(expr: Expr | SelectBuilderLike): Expr {
    if (this.isSelectBuilder(expr)) {
      return {
        type: 'subquery',
        query: expr.getQuery(),
      }
    }
    return expr as Expr
  }

  private isSelectBuilder(value: any): value is SelectBuilderLike & { getQuery: () => SelectNode } {
    return (
      value &&
      typeof value === 'object' &&
      typeof value.getQuery === 'function' &&
      value.constructor &&
      value.constructor.name === 'SelectBuilder'
    )
  }
}

export function Case(): CaseBuilder {
  return new CaseBuilder()
}

export { CaseExpr }

type SelectBuilderLike = {
  getQuery: () => SelectNode
  constructor?: { name?: string }
}
