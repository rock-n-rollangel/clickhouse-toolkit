import { QueryBuilder } from '@/query-builder/query-builder'
import { registerQueryBuilders } from '@/util/register-query-builders'

describe('registerQueryBuilders', () => {
  it('should register query builder classes', () => {
    registerQueryBuilders()

    expect(QueryBuilder['queryBuilderRegistry']['InsertQueryBuilder']).toBeDefined()
    expect(QueryBuilder['queryBuilderRegistry']['DeleteQueryBuilder']).toBeDefined()
    expect(QueryBuilder['queryBuilderRegistry']['SelectQueryBuilder']).toBeDefined()
    expect(QueryBuilder['queryBuilderRegistry']['UpdateQueryBuilder']).toBeDefined()
    expect(QueryBuilder['queryBuilderRegistry']['InsertQueryBuilder']).toBeInstanceOf(Function)
    expect(QueryBuilder['queryBuilderRegistry']['DeleteQueryBuilder']).toBeInstanceOf(Function)
    expect(QueryBuilder['queryBuilderRegistry']['SelectQueryBuilder']).toBeInstanceOf(Function)
    expect(QueryBuilder['queryBuilderRegistry']['UpdateQueryBuilder']).toBeInstanceOf(Function)
  })
})
