import { QueryBuilder } from './query-builder'
import { SelectStatementError } from '@/errors/select-empty'
import { TableNameError } from '@/errors/table-name'
import { QueryBuilderCallback } from '@/types/query-builder-callback'
import { Params } from '@/types/params'
import { ObjectLiteral } from '@/types/object-literal'
import { WhereExpressionBuilder } from './where-expression-builder'
import { JoinAttribute } from './join-attribute'
import { InstanceChecker } from '@/util/instance-checker'

export class SelectQueryBuilder extends QueryBuilder implements WhereExpressionBuilder {
  readonly '@instanceof' = Symbol.for('SelectQueryBuilder')

  protected parseSelect(): string {
    if (!this.expressionMap.selects.length) throw new SelectStatementError()
    return `SELECT ${this.expressionMap.selects.map((select) => this.addAlias(select)).join(', ')}`
  }

  protected parseTable(): string {
    if (!this.expressionMap.table) throw new TableNameError()

    if (this.expressionMap.tableAlias)
      return ` FROM ${this.connection.getFullTablePath(this.expressionMap.table)} AS ${this.connection.escape(this.expressionMap.tableAlias)}`
    else return ` FROM ${this.connection.getFullTablePath(this.expressionMap.table)}`
  }

  protected parseGroupBy(): string {
    this.expressionMap.groupBys = this.expressionMap.groupBys.map((groupBy) =>
      this.addAlias(this.connection.escape(groupBy)),
    )

    if (this.expressionMap.groupBys.length < 1) return ''
    else return ` GROUP BY ${this.expressionMap.groupBys.join(', ')}`
  }

  protected parseOrderBy(): string {
    this.expressionMap.orderBys = this.expressionMap.orderBys.map((orderBy) =>
      this.addAlias(this.connection.escape(orderBy)),
    )

    if (this.expressionMap.orderBys.length < 1) return ''
    else if (this.expressionMap.orderBys.length > 0 && this.expressionMap.orderByDirection)
      return ` ORDER BY ${this.expressionMap.orderBys.join(', ')} ${this.expressionMap.orderByDirection}`
    else return ` ORDER BY ${this.expressionMap.orderBys.join(', ')}`
  }

  protected parseLimit(): string {
    if (!this.expressionMap.limit) return ''
    else return ` LIMIT ${this.expressionMap.limit}`
  }

  protected parseOffset(): string {
    if (!this.expressionMap.offset) return ''
    return `OFFSET ${this.expressionMap.offset}`
  }

  protected parseJoins(): string {
    if (this.expressionMap.joinAttributes.length === 0) return ''

    return this.expressionMap.joinAttributes
      .map((joinAttribute) => {
        const tablePath = this.connection.getFullTablePath(
          InstanceChecker.isSchemaMetadata(joinAttribute.joinTable)
            ? joinAttribute.joinTable.tableMetadataArgs.name
            : joinAttribute.joinTable,
        )

        return (
          ` ${joinAttribute.direction} JOIN ${tablePath} AS ${this.connection.escape(joinAttribute.alias)}` +
          ` ON` +
          ` ${joinAttribute.condition}`
        )
      })
      .join(' ')
  }

  protected join(
    direction: 'INNER' | 'LEFT',
    alias: string,
    joinTable: string,
    condition: string,
    params?: Params,
  ): void {
    if (params) {
      this.setParameters(params)
    }

    this.expressionMap.joinAttributes.push(new JoinAttribute(joinTable, direction, alias, condition))
  }

  public select(): SelectQueryBuilder
  public select(fields: string[]): SelectQueryBuilder
  public select(field: string): SelectQueryBuilder
  public select(qb: QueryBuilderCallback, columnAlias: string): SelectQueryBuilder
  public select(field?: string | string[] | QueryBuilderCallback, columnAlias?: string): SelectQueryBuilder {
    if (!field && !columnAlias) {
      this.expressionMap.parameters = []
      this.expressionMap.selects = ['*']

      return this as any as SelectQueryBuilder
    }

    if (typeof field === 'function') {
      const qb = field(this.createQueryBuilder()) as SelectQueryBuilder
      this.expressionMap.parameters.push(qb.getParameters())
      this.expressionMap.selects = [`(${qb.toSql()}) AS ${columnAlias}`]

      return this as any as SelectQueryBuilder
    }

    this.expressionMap.selects = Array.isArray(field) ? field : [field]
    return this as any as SelectQueryBuilder
  }

  public toSql(): string {
    let sql =
      this.parseSelect() +
      this.parseTable() +
      this.parseJoins() +
      this.parseWhere() +
      this.parseGroupBy() +
      this.parseOrderBy() +
      this.parseLimit() +
      this.parseOffset()

    if (this.expressionMap.parameters.length > 0) {
      sql = this.processParams(sql, this.expressionMap.parameters)
    }

    return sql
  }

  public addSelect(field: string): SelectQueryBuilder
  public addSelect(...fields: string[]): SelectQueryBuilder
  public addSelect(callback: QueryBuilderCallback, columnAlias: string): SelectQueryBuilder
  public addSelect(field: string | string[] | QueryBuilderCallback, columnAlias?: string): SelectQueryBuilder {
    if (typeof field === 'function') {
      const qb = field(this.createQueryBuilder()) as SelectQueryBuilder
      this.expressionMap.parameters.push(qb.getParameters())
      this.addSelect(`(${qb.toSql()}) AS ${this.connection.escape(columnAlias)}`)

      return this
    }

    this.addToArray(this.expressionMap.selects, field)
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

  public groupBy(field: string): SelectQueryBuilder
  public groupBy(fields: string[]): SelectQueryBuilder
  public groupBy(field: string | string[]): SelectQueryBuilder {
    this.expressionMap.groupBys = Array.isArray(field) ? field : [field]
    return this
  }

  public orderBy(field: string): SelectQueryBuilder
  public orderBy(...fields: string[]): SelectQueryBuilder
  public orderBy(field: string, direction: string): SelectQueryBuilder
  public orderBy(field: string[], direction: string): SelectQueryBuilder
  public orderBy(field: string | string[] | ObjectLiteral, direction?: string): SelectQueryBuilder {
    if (direction) this.expressionMap.orderByDirection = this.connection.escape(direction)
    this.expressionMap.orderBys = Array.isArray(field) ? field : [field]
    return this
  }

  public from(table: string, alias?: string): SelectQueryBuilder
  public from(qb: QueryBuilderCallback): SelectQueryBuilder
  public from(table: string | QueryBuilderCallback, alias?: string): SelectQueryBuilder {
    if (typeof table === 'function') {
      this.expressionMap.table = `(${table(this.createQueryBuilder()).toSql()})`

      return this
    }

    this.expressionMap.table = table
    if (!alias) this.expressionMap.tableAlias = table.split('.').join('_')
    else this.expressionMap.tableAlias = alias

    return this
  }

  public innerJoin(table: string, alias: string, condition: string, params?: Params): this {
    this.join('INNER', alias, table, condition, params)
    return this
  }

  public leftJoin(table: string, alias: string, condition: string, params?: Params): this {
    this.join('INNER', alias, table, condition, params)
    return this
  }

  public limit(limit: number): SelectQueryBuilder {
    this.expressionMap.limit = limit
    return this
  }

  public offset(offset: number): SelectQueryBuilder {
    this.expressionMap.offset = offset
    return this
  }

  public async execute<T>(): Promise<T[]> {
    return await this.queryRunner.query(this.toSql(), this.getProcessedParameters())
  }
}
