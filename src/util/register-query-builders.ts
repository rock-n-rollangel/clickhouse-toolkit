import { DeleteQueryBuilder } from '../query-builder/delete-query-builder'
import { InsertQueryBuilder } from '../query-builder/insert-query-builder'
import { QueryBuilder } from '../query-builder/query-builder'
import { SelectQueryBuilder } from '../query-builder/select-query-builder'
import { UpdateQueryBuilder } from '../query-builder/update-query-builder'

export function registerQueryBuilders() {
  QueryBuilder.registerQueryBuilderClass(
    'InsertQueryBuilder',
    (qb: QueryBuilder<any>) => new InsertQueryBuilder<any>(qb),
  )
  QueryBuilder.registerQueryBuilderClass(
    'SelectQueryBuilder',
    (qb: QueryBuilder<any>) => new SelectQueryBuilder<any>(qb),
  )
  QueryBuilder.registerQueryBuilderClass(
    'UpdateQueryBuilder',
    (qb: QueryBuilder<any>) => new UpdateQueryBuilder<any>(qb),
  )
  QueryBuilder.registerQueryBuilderClass(
    'DeleteQueryBuilder',
    (qb: QueryBuilder<any>) => new DeleteQueryBuilder<any>(qb),
  )
}
