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

/**
 * Abstract class representing a query builder for constructing and executing SQL queries.
 */
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

  /**
   * Creates an instance of QueryBuilder.
   * @param queryBuilder - An existing QueryBuilder instance or a Connection object.
   * @param queryRunner - An optional QueryRunner instance.
   */
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

  /**
   * Registers a query builder class with a given name and factory function.
   * @param name - The name of the query builder class.
   * @param factory - The factory function to create instances of the query builder.
   */
  public static registerQueryBuilderClass(name: string, factory: any) {
    QueryBuilder.queryBuilderRegistry[name] = factory
  }

  /**
   * Executes the constructed SQL query.
   * @returns A promise resolving to the result of the execution.
   */
  public abstract execute(): Promise<any>

  public toSql?(...args: any[]): [string, ObjectLiteral]

  /**
   * Specifies the fields to select in the query.
   * @param field - The fields to select; can be a single field, an array of fields, or a function returning a query builder.
   * @param columnAlias - An optional alias for the selected field.
   * @returns An instance of SelectQueryBuilder.
   */
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
   * Prepares an insert operation for the database.
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
   * Prepares an update operation for the specified target table.
   * @param target - The target table name or schema.
   * @param updateColumns - An optional array of columns to update.
   * @returns {UpdateQueryBuilder}
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

  /**
   * Prepares a delete operation for the specified target table.
   * @param target - The target table name or schema.
   * @returns {DeleteQueryBuilder}.
   */
  public delete(target: string | TableSchema): DeleteQueryBuilder {
    this.expressionMap.table = typeof target === 'string' ? target : target.name
    this.expressionMap.processedParameters = {}

    if (InstanceChecker.isDeleteQueryBuilder(this)) return this

    return QueryBuilder.queryBuilderRegistry['DeleteQueryBuilder'](this)
  }

  /**
   * Creates a new instance of the current query builder, inheriting its properties.
   * @returns A new instance of the current query builder.
   */
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

  /**
   * Processes SQL parameters and binds them to the query.
   * @param sql - The SQL query string.
   * @param params - The parameters to bind to the SQL query.
   * @returns {string} The processed SQL string with parameters.
   * @throws NotAllParametersWasPassedError if the number of parameters does not match the number of placeholders.
   * @throws WrongParameterColumnNameError if a parameter name does not match any column name.
   */
  protected preprocess(sql: string, params: Params[]): [string, ObjectLiteral] {
    const pattern = /(:[a-z0-9_]+)/gi
    const matches = sql.match(pattern)?.map((match) => match.replace(/[^a-z0-9_]/gi, ''))

    if (!matches) return [sql, {}]

    if (!this.expressionMap.metadata)
      this.expressionMap.metadata = this.connection.getMetadata(this.expressionMap.table)
    const columnsMap = new Map(
      this.expressionMap.metadata.columnMetadataArgs.map((column) => [column.options.name, column]),
    )

    if (params.length !== matches.length) throw new NotAllParametersWasPassedError()

    const processedParams: Params = {}
    let wrappedSql = sql
    matches.forEach((paramName, paramIndex) => {
      const binding = params[paramIndex]

      if (!columnsMap.get(paramName)) throw new WrongParameterColumnNameError(paramName)

      // this needed to prevent clearing params values by child query builders
      const paramId = `$${paramIndex + 1}`
      processedParams[paramId] = binding[paramName]
      const columnType = columnsMap.get(paramName).options.type
      const isArray = Array.isArray(binding[paramName]) && !/Array.+/.test(columnType)
      wrappedSql = wrappedSql.replace(`:${paramName}`, `{${paramId}: ${isArray ? `Array(${columnType})` : columnType}}`)
    })

    return [wrappedSql, processedParams]
  }

  /**
   * Sets the parameters for the query builder.
   *
   * @param params - An object containing parameters to be set. If no parameters are provided, the method does nothing.
   */
  protected setParameters(params: Params): void {
    if (!params) return
    Object.entries(params).forEach(([key, value]) => this.expressionMap.parameters.push({ [key]: value }))
  }

  /**
   * Retrieves the parameters currently set in the query builder.
   *
   * @returns An array of parameters set in the query builder.
   */
  protected getParameters(): Params[] {
    return this.expressionMap.parameters
  }

  /**
   * Sets the processed parameters for the query builder.
   *
   * This method also sets processed parameters for the parent query builder if it exists.
   * Combines the existing processed parameters with the new parameters provided.
   *
   * @param params - An object containing new parameters to be processed and set.
   */
  protected setProcessedParameters(params: Params) {
    if (this.parentQueryBuilder) this.parentQueryBuilder.setProcessedParameters(params)
    this.expressionMap.processedParameters = { ...this.getProcessedParameters(), ...params }
  }

  /**
   * Retrieves the processed parameters from the query builder.
   *
   * @returns An object containing the processed parameters.
   */
  protected getProcessedParameters(): Params {
    return this.expressionMap.processedParameters
  }

  /**
   * Retrieves the table alias for the current query builder.
   *
   * @returns The escaped table alias or, if not available, the escaped table name.
   */
  protected getTableAlias(): string {
    return this.connection.escape(this.expressionMap.tableAlias) ?? this.connection.escape(this.expressionMap.table)
  }

  /**
   * Checks if the provided column name is a schema column.
   *
   * @param columnName - The name of the column to check.
   * @returns True if the column is found in the metadata; otherwise, false.
   */
  protected isSchemsColumn(columnName: string): boolean {
    if (!this.expressionMap.metadata) return false

    return !(
      this.expressionMap.metadata.columnMetadataArgs.find((column) => column.options.name === columnName) === undefined
    )
  }

  /**
   * Adds an alias to the specified column name.
   *
   * If the column is a schema column and a table alias is available, it prefixes the column name with the table alias.
   *
   * @param name - The name of the column to add an alias to.
   * @returns The column name, potentially prefixed with the table alias.
   */
  protected addAlias(name: string): string {
    const tableAlias = this.getTableAlias()
    if (this.isSchemsColumn(name) && tableAlias) {
      return `${tableAlias}.${name}`
    }

    return name
  }

  /**
   * Adds a WHERE clause to the query builder.
   *
   * This method supports multiple overloads for different types of input:
   * - fieldValues (ObjectLiteral)
   * - statement (string)
   * - callback (QueryBuilderCallback)
   *
   * @param statement - The condition for the WHERE clause, which can be a function, string, or object.
   * @param type - The type of the WHERE clause (e.g., 'simple', 'and', 'or').
   * @param params - Optional parameters to bind to the query.
   * @returns The current instance of the query builder.
   */
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

  /**
   * Parses the WHERE clauses and generates the corresponding SQL condition.
   *
   * @returns A string representing the parsed SQL WHERE clause.
   */
  protected parseWhere(): string {
    let sql = ''

    this.expressionMap.whereClauses.forEach((whereClause) => {
      sql += this.getWhereClauseCondition(whereClause)
    })

    return sql
  }

  /**
   * Retrieves the condition string for a specific WHERE clause.
   *
   * @param whereClause - The WHERE clause to parse.
   * @param setType - Optional flag to determine if the type should be set (defaults to true).
   * @returns A string representing the condition for the WHERE clause.
   */
  protected getWhereClauseCondition(whereClause: WhereClause, setType = true): string {
    const type = whereClause.type === 'simple' ? 'WHERE' : whereClause.type.toUpperCase()
    // This makes params order to be correct
    if (whereClause.params) {
      this.setParameters(
        Object.fromEntries(
          Object.entries(whereClause.params).filter(
            ([key, value]) => !this.expressionMap?.parameters.find((param) => param[key] === value),
          ),
        ),
      )
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

  /**
   * Retrieves the list of WHERE clauses from the query builder.
   *
   * @returns An array of WHERE clauses.
   */
  protected getWhere(): WhereClause[] {
    return this.expressionMap.whereClauses
  }
}
