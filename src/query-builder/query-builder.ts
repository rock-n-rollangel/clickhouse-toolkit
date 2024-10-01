import { Connection } from '../connection/connection'
import { NotAllParametersWasPassedError } from '../errors/not-all-parameters-was-passed'
import { WrongParameterColumnNameError } from '../errors/wrong-parameter-column-name'
import { QueryRunner } from '../query-runner/query-runner'
import { QueryExpressionMap } from './query-expression-map'
import { SelectQueryBuilder } from './select-query-builder'
import { Params } from '../types/params'
import { QueryBuilderCallback } from '../types/query-builder-callback'
import { InsertQueryBuilder } from './insert-query-builder'
import { UpdateQueryBuilder } from './update-query-builder'
import { ObjectLiteral } from '../types/object-literal'
import { InstanceChecker } from '../util/instance-checker'
import { DeleteQueryBuilder } from './delete-query-builder'
import { TableSchema } from '../types/table-schema'
import { WhereClauseType } from '../types/where-clause-type'
import { WhereClause } from '../types/where-clause'

export abstract class QueryBuilder {
  readonly '@instanceof' = Symbol.for('QueryBuilder')

  protected connection: Connection

  // query runner used to make execute query builder query
  protected queryRunner?: QueryRunner

  // properties needed to make final query
  protected expressionMap?: QueryExpressionMap

  // need to send parameters to make globals
  protected parentQueryBuilder?: QueryBuilder

  private static queryBuilderRegistry: Record<string, any> = {}

  constructor(queryBuilder: QueryBuilder)
  constructor(connection: Connection, queryRunner?: QueryRunner)
  constructor(connectionOrQueryBuilder: Connection | QueryBuilder, queryRunner?: QueryRunner) {
    if (InstanceChecker.isConnection(connectionOrQueryBuilder)) {
      this.connection = connectionOrQueryBuilder
      this.queryRunner = queryRunner
      this.expressionMap = new QueryExpressionMap()
    } else {
      this.connection = connectionOrQueryBuilder.connection
      this.queryRunner = connectionOrQueryBuilder.queryRunner
      this.expressionMap = connectionOrQueryBuilder.expressionMap.clone()
      this.parentQueryBuilder = connectionOrQueryBuilder
    }
  }

  public static registerQueryBuilderClass(name: string, factory: any) {
    QueryBuilder.queryBuilderRegistry[name] = factory
  }

  public abstract execute(): Promise<any>

  public toSql?(...args: any[]): string

  public select(): SelectQueryBuilder
  public select(fields: string[]): SelectQueryBuilder
  public select(field: string): SelectQueryBuilder
  public select(qb: (qb: this) => this, columnAlias: string): SelectQueryBuilder
  public select(field?: string | string[] | ((qb: this) => this), columnAlias?: string): SelectQueryBuilder {
    this.expressionMap.processedParameters = {}
    if (!field && !columnAlias) {
      this.expressionMap.parameters = []
      this.expressionMap.selects = ['*']
    } else if (typeof field === 'function') {
      const qb = field(this.createQueryBuilder())
      this.expressionMap.parameters.push(qb.getParameters())
      this.expressionMap.selects = [`(${qb.toSql()}) AS ${columnAlias}`]
    } else {
      this.expressionMap.selects = Array.isArray(field) ? field : [field]
    }

    if (InstanceChecker.isSelectQueryBuilder(this)) return this

    return QueryBuilder.queryBuilderRegistry['SelectQueryBuilder'](this)
  }

  /**
   * This method used to insert to out database
   *
   * @returns {InsertQueryBuilder}
   */
  public insert(): InsertQueryBuilder {
    this.expressionMap.table = ''
    this.expressionMap.insertValues = []
    this.expressionMap.insertColumns = []
    this.expressionMap.processedParameters = {}
    return QueryBuilder.queryBuilderRegistry['InsertQueryBuilder'](this)
  }
  /**
   * First argument is target table name or schema we want work with
   * Second argument is columns we want to update
   *
   * @param target
   * @param updateColumns
   * @returns
   */
  public update(target: string | TableSchema, updateColumns?: string[]): UpdateQueryBuilder {
    this.expressionMap.processedParameters = {}
    this.expressionMap.table = typeof target === 'string' ? target : target.name
    if (updateColumns) {
      this.expressionMap.updateColumns = updateColumns
    }

    if (InstanceChecker.isInsertQueryBuilder(this)) return this as any

    return QueryBuilder.queryBuilderRegistry['UpdateQueryBuilder'](this)
  }

  public delete(target: string | TableSchema): DeleteQueryBuilder {
    this.expressionMap.table = typeof target === 'string' ? target : target.name
    this.expressionMap.metadata = this.connection.getMetadata(this.expressionMap.table)
    this.expressionMap.processedParameters = {}

    if (InstanceChecker.isDeleteQueryBuilder(this)) return this

    return QueryBuilder.queryBuilderRegistry['DeleteQueryBuilder'](this)
  }

  protected createQueryBuilder(): this {
    const qb = new (this.constructor as any)(this)
    return qb
  }

  protected addToArray(array: any[], value: any | any[]): void {
    if (Array.isArray(value)) array.push(...value)
    else array.push(value)
  }

  protected generateParamId(): string {
    const ns = process.hrtime()[1]
    const uniquePart = ns.toString(36) + Math.random().toString(36).substring(2, 6)
    return uniquePart.slice(0, 16)
  }

  protected processParams(sql: string, params: Params[]): string {
    const pattern = /(:[a-z0-9_]+)/gi
    const matches = sql.match(pattern)?.map((match) => match.replace(/[^a-z0-9_]/gi, ''))

    if (!matches) return sql

    if (!this.expressionMap.metadata)
      this.expressionMap.metadata = this.connection.getMetadata(this.expressionMap.table)
    const columnsMap = new Map(
      this.expressionMap.metadata.columnMetadataArgs.map((column) => [column.options.name, column]),
    )

    if (params.length !== matches.length) throw new NotAllParametersWasPassedError()

    const newParams: Params = {}
    let wrappedSql = sql
    matches.forEach((paramName, paramIndex) => {
      const binding = params[paramIndex]

      if (!columnsMap.get(paramName)) throw new WrongParameterColumnNameError(paramName)

      // this needed to prevent clearing params values by child query builders
      const paramId = `$${paramIndex + 1}`
      newParams[paramId] = binding[paramName]
      const columnType = columnsMap.get(paramName).options.type
      const isArray = Array.isArray(binding[paramName]) && !/Array.+/.test(columnType)
      wrappedSql = wrappedSql.replace(`:${paramName}`, `{${paramId}: ${isArray ? `Array(${columnType})` : columnType}}`)
    })

    this.setProcessedParameters(newParams)

    return wrappedSql
  }

  protected setParameters(params: Params): void {
    if (!params) return
    Object.entries(params).forEach(([key, value]) => this.expressionMap.parameters.push({ [key]: value }))
  }

  protected getParameters(): Params[] {
    return this.expressionMap.parameters
  }

  protected setProcessedParameters(params: Params) {
    if (this.parentQueryBuilder) this.parentQueryBuilder.setProcessedParameters(params)
    this.expressionMap.processedParameters = { ...this.getProcessedParameters(), ...params }
  }

  protected getProcessedParameters(): Params {
    return this.expressionMap.processedParameters
  }

  protected getTableAlias(): string {
    return this.connection.escape(this.expressionMap.tableAlias) ?? this.connection.escape(this.expressionMap.table)
  }

  protected isSchemsColumn(columnName: string): boolean {
    if (!this.expressionMap.metadata) return false

    return !(
      this.expressionMap.metadata.columnMetadataArgs.find((column) => column.options.name === columnName) === undefined
    )
  }

  protected addAlias(name: string): string {
    const tableAlias = this.getTableAlias()
    if (this.isSchemsColumn(name) && tableAlias) {
      return `${tableAlias}.${name}`
    }

    return name
  }

  protected addWhere(fieldValues: ObjectLiteral, type: WhereClauseType): this
  protected addWhere(statement: string, type: WhereClauseType, params?: Params): this
  protected addWhere(qb: QueryBuilderCallback, type: WhereClauseType, params?: Params): this
  protected addWhere(
    statement: string | QueryBuilderCallback | ObjectLiteral,
    type: WhereClauseType,
    params?: Params,
  ): this {
    if (typeof statement === 'function') {
      const qb = statement(this.createQueryBuilder()) as SelectQueryBuilder
      const whereClauses = qb.getWhere()
      this.expressionMap.whereClauses.push({
        type: type,
        condition: whereClauses,
      })

      return this
    }

    if (!Array.isArray(statement) && typeof statement === 'object' && statement !== null) {
      Object.entries(statement).forEach(([key, value]) => {
        this.expressionMap.whereClauses.push({
          type: type,
          condition: `${this.connection.escape(key)} = :${key}`,
          params: { [key]: value },
        })
      })

      return this
    }

    this.expressionMap.whereClauses.push({
      type: type,
      condition: `${statement}`,
      params: params,
    })
    return this
  }

  protected parseWhere(): string {
    let sql = ''

    this.expressionMap.whereClauses.forEach((whereClause) => {
      sql += this.getWhereClauseCondition(whereClause)
    })

    return sql
  }

  protected getWhereClauseCondition(whereClause: WhereClause, setType = true): string {
    const type = whereClause.type === 'simple' ? 'WHERE' : whereClause.type.toUpperCase()
    // This makes params order to be correct
    if (whereClause.params) {
      this.setParameters(whereClause.params)
    }

    let condition: string
    if (Array.isArray(whereClause.condition)) {
      // We must set type of parent either it will be `simple`
      whereClause.condition[0].type = whereClause.type
      condition = whereClause.condition
        // We dont need to set type prefix or it make error like: `AND (AND f = f)`
        .map((whereClause, index) => this.getWhereClauseCondition(whereClause, index !== 0))
        .join('')
    } else {
      condition = whereClause.condition
    }

    if (setType) {
      return ` ${type} ( ${condition} )`
    } else {
      return `( ${condition} )`
    }
  }

  protected getWhere(): WhereClause[] {
    return this.expressionMap.whereClauses
  }
}
