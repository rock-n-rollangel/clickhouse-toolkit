import { CallbackFunction } from 'src/types/callback-function'
import { ObjectLiteral } from '../types/object-literal'
import { Params } from '../types/params'
import { QueryBuilder } from './query-builder'

export interface WhereExpressionBuilder<T extends ObjectLiteral> {
  where(statement: string, params?: Params<T>): this
  where(qb: CallbackFunction<QueryBuilder<T>>, params?: Params<T>): this
  where(statement: ObjectLiteral): this
  where(statement: string | CallbackFunction<QueryBuilder<T>> | ObjectLiteral, params?: Params<T>): this

  andWhere(statement: string, params?: Params<T>): this
  andWhere(qb: CallbackFunction<QueryBuilder<T>>, params?: Params<T>): this
  andWhere(fieldsValues: ObjectLiteral): this
  andWhere(statement: string | CallbackFunction<QueryBuilder<T>> | ObjectLiteral, params?: Params<T>): this

  orWhere(statement: string, params?: Params<T>): this
  orWhere(qb: CallbackFunction<QueryBuilder<T>>, params?: Params<T>): this
  orWhere(fieldsValues: ObjectLiteral): this
  orWhere(statement: string | CallbackFunction<QueryBuilder<T>> | ObjectLiteral, params?: Params<T>): this
}
