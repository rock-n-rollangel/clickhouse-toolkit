import { ObjectLiteral } from '../types/object-literal'
import { TableNameError } from '../errors/table-name'
import { UpdateValueNotSetError } from '../errors/update-value-not-set'
import { QueryBuilder } from './query-builder'
import { Params } from '../types/params'
import { QueryBuilderCallback } from '../types/query-builder-callback'
import { WhereExpressionBuilder } from './where-expression-builder'

export class UpdateQueryBuilder extends QueryBuilder implements WhereExpressionBuilder {
  readonly '@instanceof' = Symbol.for('UpdateQueryBuilder')

  public set(value: ObjectLiteral): UpdateQueryBuilder {
    this.expressionMap.updateValue = value
    this.setParameters(value)
    return this
  }

  public where(statement: string | ((qb: this) => this) | ObjectLiteral, params?: Params): this {
    this.expressionMap.parameters = []
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

  public toSql(): string {
    let sql = this.parseUpdate() + this.parseValues() + this.parseWhere()

    // We need to set updateValues to parameters first
    const dumpParams = this.expressionMap.parameters
    this.expressionMap.parameters = []
    this.setParameters(this.expressionMap.updateValue)
    for (const param of dumpParams) this.setParameters(param)

    if (this.expressionMap.parameters.length > 0) {
      sql = this.processParams(sql, this.expressionMap.parameters)
    }

    return sql
  }

  public async execute(): Promise<any> {
    return await this.queryRunner.command(this.toSql(), this.getProcessedParameters())
  }

  protected parseUpdate(): string {
    if (!this.expressionMap.table) throw new TableNameError()

    return `ALTER TABLE ${this.connection.getFullTablePath(this.expressionMap.table)} UPDATE `
  }

  protected parseValues(): string {
    if (!this.expressionMap.updateValue) {
      throw new UpdateValueNotSetError()
    }

    if (Array.isArray(this.expressionMap.updateColumns)) {
      this.expressionMap.updateValue = Object.fromEntries(
        Object.entries(this.expressionMap.updateValue).filter(([key]) =>
          this.expressionMap.updateColumns.includes(key),
        ),
      )
    }

    return Object.entries(this.expressionMap.updateValue)
      .map(([key]) => {
        return `${this.addAlias(this.connection.escape(key))} = :${key}`
      })
      .join(', ')
  }
}
