import { ObjectLiteral } from '../types/object-literal'
import { TableSchema } from '../types/table-schema'
import { QueryBuilder } from './query-builder'

export class InsertQueryBuilder extends QueryBuilder {
  readonly '@instanceof' = Symbol.for('InsertQueryBuilder')

  public values(callback: (qb: this) => this): InsertQueryBuilder
  public values(value: ObjectLiteral): InsertQueryBuilder
  public values(values: ObjectLiteral[]): InsertQueryBuilder
  public values(value: ((qb: this) => this) | ObjectLiteral | ObjectLiteral[]): InsertQueryBuilder {
    if (typeof value === 'function') {
      this.expressionMap.insertValues = value(this.createQueryBuilder()).toSql()
    } else if (Array.isArray(value)) {
      this.expressionMap.insertValues = value
    } else {
      this.expressionMap.insertValues = [value]
    }

    return this
  }

  public into(target: TableSchema | string, columns?: string[]): InsertQueryBuilder {
    if (typeof target === 'string') {
      this.expressionMap.table = target
    } else {
      this.expressionMap.table = target.name
    }

    if (columns) this.expressionMap.insertColumns = columns

    return this
  }

  public async execute(): Promise<void> {
    if (typeof this.expressionMap.insertValues === 'string') {
      await this.queryRunner.command(
        `INSERT INTO ${this.expressionMap.table} ${this.expressionMap.insertValues}`,
        this.expressionMap.processedParameters,
      )
    } else {
      await this.queryRunner.insert(this.expressionMap.insertValues, this.expressionMap.table)
    }
  }

  protected getValues(): ObjectLiteral[] | string {
    const insertColumns = this.expressionMap.insertColumns
    if (insertColumns && Array.isArray(insertColumns) && typeof this.expressionMap.insertValues !== 'string') {
      return this.expressionMap.insertValues.map((value) => {
        return Object.fromEntries(Object.entries(value).filter(([key]) => insertColumns.includes(key)))
      })
    }

    return this.expressionMap.insertValues
  }
}
