import { ObjectLiteral } from '../types/object-literal'
import { Params } from '../types/params'

export interface WhereExpressionBuilder {
  where(statement: string, params?: Params): this
  where(qb: (qb: this) => this, params?: Params): this
  where(statement: ObjectLiteral): this
  where(statement: string | ((qb: this) => this) | ObjectLiteral, params?: Params): this

  andWhere(statement: string, params?: Params): this
  andWhere(qb: (qb: this) => this, params?: Params): this
  andWhere(fieldsValues: ObjectLiteral): this
  andWhere(statement: string | ((qb: this) => this) | ObjectLiteral, params?: Params): this

  orWhere(statement: string, params?: Params): this
  orWhere(qb: (qb: this) => this, params?: Params): this
  orWhere(fieldsValues: ObjectLiteral): this
  orWhere(statement: string | ((qb: this) => this) | ObjectLiteral, params?: Params): this
}
