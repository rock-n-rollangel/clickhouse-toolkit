import { ObjectLiteral } from '@/types/object-literal'
import { QueryBuilder } from './query-builder'
import { Params } from '@/types/params'
import { QueryBuilderCallback } from '@/types/query-builder-callback'
import { WhereExpressionBuilder } from './where-expression-builder'

export class DeleteQueryBuilder extends QueryBuilder implements WhereExpressionBuilder {
  readonly '@instanceof' = Symbol.for('DeleteQueryBuilder')

  public where(statement: string | ((qb: this) => this) | ObjectLiteral, params?: Params): this {
    if (typeof statement === 'object' && statement !== null) return this.addWhere(statement, 'simple')
    else if (typeof statement === 'function') return this.addWhere(statement as QueryBuilderCallback, 'simple', params)
    else return this.addWhere(statement as string, 'simple', params)
  }

  public andWhere(statement: string | ((qb: this) => this) | ObjectLiteral, params?: Params): this {
    if (typeof statement === 'object' && statement !== null) return this.addWhere(statement, 'and')
    else if (typeof statement === 'function') return this.addWhere(statement as QueryBuilderCallback, 'and', params)
    else return this.addWhere(statement as string, 'and', params)
  }

  public orWhere(statement: string | ((qb: this) => this) | ObjectLiteral, params?: Params): this {
    if (typeof statement === 'object' && statement !== null) return this.addWhere(statement, 'or')
    else if (typeof statement === 'function') return this.addWhere(statement as QueryBuilderCallback, 'or', params)
    else return this.addWhere(statement as string, 'or', params)
  }

  public limit(limit: number): DeleteQueryBuilder {
    this.expressionMap.limit = limit
    return this
  }

  public offset(offset: number): DeleteQueryBuilder {
    this.expressionMap.offset = offset
    return this
  }

  public toSql(): string {
    let sql = this.parseDelete() + this.parseTable() + this.parseWhere() + this.parseLimit() + this.parseOffset()
    if (this.expressionMap.whereClauses.length > 0) sql = this.processParams(sql, this.getParameters())

    return sql
  }

  public async execute(): Promise<void> {
    await this.queryRunner.command(this.toSql(), this.getProcessedParameters())
  }

  protected parseDelete(): string {
    return `DELETE FROM`
  }

  protected parseTable(): string {
    return ` ${this.connection.getFullTablePath(this.expressionMap.table)}`
  }

  protected parseLimit(): string {
    if (!this.expressionMap.limit) return ''
    return ` LIMIT ${this.expressionMap.limit}`
  }

  protected parseOffset(): string {
    if (!this.expressionMap.offset) return ''
    return ` OFFSET ${this.expressionMap.offset}`
  }
}
