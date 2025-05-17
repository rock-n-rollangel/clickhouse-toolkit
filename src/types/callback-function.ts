import { SelectQueryBuilder } from 'src/query-builder/select-query-builder'
import { QueryBuilder } from 'src/query-builder/query-builder'

export type CallbackFunction<Q extends QueryBuilder<T>, T = any> = (qb: Q) => SelectQueryBuilder<T> | Q
