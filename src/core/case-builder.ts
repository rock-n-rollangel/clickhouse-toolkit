/**
 * CASE WHEN THEN builder helper
 */

import { WhereInput, Operator } from './operators'
import { PredicateNode, Expr, CaseExpr, ColumnRef, Value } from './ast'
import { operatorToPredicate, parseColumnRef } from './predicate-builder'
import { createValidationError } from './errors'

export class CaseBuilder {
  private cases: Array<{ condition: PredicateNode; then: Expr }> = []
  private maxDepth: number = 0

  constructor() {
    // CaseBuilder doesn't need logger as per user feedback
  }

  // THEN accepts string (column name), Expr (any expression), or Primitive (literal value)
  when(condition: WhereInput | PredicateNode, thenExpr: string | Expr | any): this {
    // Convert THEN to Expr
    let thenValue: Expr
    if (typeof thenExpr === 'string') {
      thenValue = { type: 'column', name: thenExpr } as ColumnRef
    } else if (typeof thenExpr === 'object' && thenExpr !== null && 'type' in thenExpr) {
      thenValue = thenExpr
      if (thenValue.type === 'case') {
        this.maxDepth++
        if (this.maxDepth > 10) {
          throw createValidationError('Maximum CASE nesting depth (10) exceeded', undefined, 'case', 'nesting')
        }
      }
    } else {
      thenValue = { type: 'value', value: thenExpr } as Value
    }

    // Convert condition to PredicateNode
    let predicateNode: PredicateNode
    if (typeof condition === 'object' && 'type' in condition) {
      predicateNode = condition as PredicateNode
    } else {
      predicateNode = this.convertWhereInputToPredicate(condition as WhereInput)
    }

    this.cases.push({ condition: predicateNode, then: thenValue })
    return this
  }

  else(elseExpr: string | Expr | any): CaseExpr {
    let elseValue: Expr
    if (typeof elseExpr === 'string') {
      elseValue = { type: 'column', name: elseExpr } as ColumnRef
    } else if (typeof elseExpr === 'object' && elseExpr !== null && 'type' in elseExpr) {
      elseValue = elseExpr
    } else {
      elseValue = { type: 'value', value: elseExpr } as Value
    }

    return {
      type: 'case',
      cases: this.cases,
      else: elseValue,
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
