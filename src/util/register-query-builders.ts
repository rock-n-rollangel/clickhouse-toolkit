import { DeleteQueryBuilder } from '../query-builder/delete-query-builder'
import { InsertQueryBuilder } from '../query-builder/insert-query-builder'
import { QueryBuilder } from '../query-builder/query-builder'
import { SelectQueryBuilder } from '../query-builder/select-query-builder'
import { UpdateQueryBuilder } from '../query-builder/update-query-builder'

export function registerQueryBuilders() {
  QueryBuilder.registerQueryBuilderClass('InsertQueryBuilder', (qb: QueryBuilder) => new InsertQueryBuilder(qb))
  QueryBuilder.registerQueryBuilderClass('SelectQueryBuilder', (qb: QueryBuilder) => new SelectQueryBuilder(qb))
  QueryBuilder.registerQueryBuilderClass('UpdateQueryBuilder', (qb: QueryBuilder) => new UpdateQueryBuilder(qb))
  QueryBuilder.registerQueryBuilderClass('DeleteQueryBuilder', (qb: QueryBuilder) => new DeleteQueryBuilder(qb))
}
