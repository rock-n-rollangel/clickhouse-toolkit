/**
 * Regression (H1): SELECT / UPDATE / DELETE must build the same WHERE clause
 * from a shared And() combinator.
 *
 * The predicate machinery used to be copy-pasted into each builder, and the
 * copies drifted: SELECT's combinatorToPredicate omitted `fromCombinator`,
 * so `select().where(And(...))` dropped the group parentheses that
 * UPDATE/DELETE produced. After hoisting the shared machinery into
 * WhereBuilder, all three render identically.
 */
import { describe, it, expect } from '@jest/globals'
import { select, update, deleteFrom, And, Gt, Eq } from '../../../src/index'

const whereOf = (sql: string): string => {
  const i = sql.toUpperCase().indexOf('WHERE')
  return i === -1 ? '' : sql.slice(i)
}

describe('WHERE combinator consistency across builders (H1 regression)', () => {
  const cond = () => And({ age: Gt(65) }, { status: Eq('retired') })

  it('SELECT renders the And() group with the same grouping parentheses as UPDATE/DELETE', () => {
    const selWhere = whereOf(select(['id']).from('users').where(cond()).toSQL().sql)
    const updWhere = whereOf(update('users').set({ x: 1 }).where(cond()).toSQL().sql)
    const delWhere = whereOf(deleteFrom('users').where(cond()).toSQL().sql)

    expect(selWhere).toBe("WHERE (`age` > 65 AND `status` = 'retired')")
    expect(selWhere).toBe(updWhere)
    expect(selWhere).toBe(delWhere)
  })
})
