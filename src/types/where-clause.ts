import { Params } from './params'

export interface WhereClause {
  type: 'simple' | 'and' | 'or'
  condition: string | WhereClause[]
  params?: Params
}
