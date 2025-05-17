import { Params } from './params'

export interface WhereClause<T> {
  type: 'simple' | 'and' | 'or'
  condition: string | WhereClause<T>[]
  params?: Params<T>
}
