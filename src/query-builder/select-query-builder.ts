import { QueryBuilder } from './query-builder'
import { SelectStatementError } from '../errors/select-empty'
import { TableNameError } from '../errors/table-name'
import { Params } from '../types/params'
import { ObjectLiteral } from '../types/object-literal'
import { WhereExpressionBuilder } from './where-expression-builder'
import { JoinAttribute } from './join-attribute'
import { InstanceChecker } from '../util/instance-checker'
import { CallbackFunction } from '../types/callback-function'
import { PropertyNotFoundError } from '../errors/property-not-found'

/**
 * SelectQueryBuilder is responsible for building SQL SELECT queries.
 * It extends the base QueryBuilder class and implements the WhereExpressionBuilder interface.
 */
export class SelectQueryBuilder<T extends ObjectLiteral> extends QueryBuilder<T> implements WhereExpressionBuilder<T> {
  readonly '@instanceof' = Symbol.for('SelectQueryBuilder')

  /**
   * Parses the SELECT clause of the query.
   * @throws {SelectStatementError} If no fields are selected.
   * @returns {string} The parsed SELECT clause.
   */
  protected parseSelect(): string {
    if (!this.expressionMap.selects.length) throw new SelectStatementError()
    return `SELECT ${this.expressionMap.selects.map((select: string) => this.addAlias(select)).join(', ')}`
  }

  /**
   * Parses the FROM clause of the query.
   * @throws {TableNameError} If no table is specified.
   * @returns {string} The parsed FROM clause.
   */
  protected parseTable(): string {
    if (!this.expressionMap.table) throw new TableNameError()

    if (this.expressionMap.tableAlias)
      return ` FROM ${this.connection.getFullTablePath(this.expressionMap.table)} AS ${this.connection.escape(this.expressionMap.tableAlias)}`
    else return ` FROM ${this.connection.getFullTablePath(this.expressionMap.table)}`
  }

  /**
   * Parses the GROUP BY clause of the query.
   * @returns {string} The parsed GROUP BY clause, or an empty string if there are no groupings.
   */
  protected parseGroupBy(): string {
    this.expressionMap.groupBys = this.expressionMap.groupBys.map((groupBy) =>
      this.addAlias(this.connection.escape(groupBy)),
    )

    if (this.expressionMap.groupBys.length < 1) return ''
    else return ` GROUP BY ${this.expressionMap.groupBys.join(', ')}`
  }

  /**
   * Parses the ORDER BY clause of the query.
   * @returns {string} The parsed ORDER BY clause, or an empty string if there are no orderings.
   */
  protected parseOrderBy(): string {
    this.expressionMap.orderBys = this.expressionMap.orderBys.map((orderBy) =>
      this.addAlias(this.connection.escape(orderBy)),
    )

    if (this.expressionMap.orderBys.length < 1) return ''
    else if (this.expressionMap.orderBys.length > 0 && this.expressionMap.orderByDirection)
      return ` ORDER BY ${this.expressionMap.orderBys.join(', ')} ${this.expressionMap.orderByDirection}`
    else return ` ORDER BY ${this.expressionMap.orderBys.join(', ')}`
  }

  /**
   * Parses the LIMIT clause of the query.
   * @returns {string} The parsed LIMIT clause, or an empty string if no limit is set.
   */
  protected parseLimit(): string {
    if (!this.expressionMap.limit) return ''
    else return ` LIMIT ${this.expressionMap.limit}`
  }

  /**
   * Parses the OFFSET clause of the query.
   * @returns {string} The parsed OFFSET clause, or an empty string if no offset is set.
   */
  protected parseOffset(): string {
    if (!this.expressionMap.offset) return ''
    return ` OFFSET ${this.expressionMap.offset}`
  }

  /**
   * Parses the JOIN clauses of the query.
   * @returns {string} The parsed JOIN clauses, or an empty string if there are no joins.
   */
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

  /**
   * Adds a JOIN clause to the query.
   * @param direction The type of join (INNER or LEFT).
   * @param alias The alias for the joined table.
   * @param joinTable The name of the table to join.
   * @param condition The join condition.
   * @param params Optional parameters for the join condition.
   */
  protected join(
    direction: 'INNER' | 'LEFT',
    alias: string,
    joinTable: string,
    condition: string,
    params?: Params<T>,
  ): void {
    if (params) {
      this.setParameters(params)
    }

    this.expressionMap.joinAttributes.push(new JoinAttribute(joinTable, direction, alias, condition))
  }

  /**
   * Specifies the fields to select in the query.
   * @param field The field(s) to select, or a callback to create a sub-query.
   * @param columnAlias Optional alias for the selected field.
   * @returns {SelectQueryBuilder} The current instance for method chaining.
   */
  public select(): SelectQueryBuilder<T>
  public select(fields: string[] | (keyof T)[]): SelectQueryBuilder<T>
  public select(field: string | keyof T): SelectQueryBuilder<T>
  public select(qb: CallbackFunction<this, T>, columnAlias: string): SelectQueryBuilder<T>
  public select(
    field?: string | keyof T | string[] | (keyof T)[] | CallbackFunction<this, T>,
    columnAlias?: string,
  ): SelectQueryBuilder<T> {
    if (!field && !columnAlias) {
      this.expressionMap.parameters = []
      this.expressionMap.selects = ['*']

      return this as any as SelectQueryBuilder<T>
    }

    if (typeof field === 'function') {
      const qb = field(this.createQueryBuilder()) as SelectQueryBuilder<T>
      const [sql, params] = qb.toSql()
      this.expressionMap.parameters.push(params)
      this.expressionMap.selects = [`(${sql}) AS ${columnAlias}`]

      return this as any as SelectQueryBuilder<T>
    }

    this.expressionMap.selects = Array.isArray(field) ? field : [field]
    return this as any as SelectQueryBuilder<T>
  }

  /**
   * Converts the query builder to an SQL string.
   * @returns {[string, Params<T>]} The generated SQL query string.
   */
  public toSql(): [string, Params<T>] {
    return this.preprocess(
      this.parseSelect() +
        this.parseTable() +
        this.parseJoins() +
        this.parseWhere() +
        this.parseGroupBy() +
        this.parseOrderBy() +
        this.parseLimit() +
        this.parseOffset(),
      this.getParameters(),
    )
  }

  /**
   * Adds additional fields to the SELECT clause of the query.
   * @param field The field(s) to add to the select list, or a callback to create a sub-query.
   * @param columnAlias Optional alias for the added field.
   * @returns {SelectQueryBuilder} The current instance for method chaining.
   */
  public addSelect(field: string): SelectQueryBuilder<T>
  public addSelect(fields: string[]): SelectQueryBuilder<T>
  public addSelect(callback: CallbackFunction<this, T>, columnAlias: string): SelectQueryBuilder<T>
  public addSelect(field: string | string[] | CallbackFunction<this, T>, columnAlias?: string): SelectQueryBuilder<T> {
    if (typeof field === 'function') {
      const qb = field(this.createQueryBuilder()) as SelectQueryBuilder<T>
      const [sql, params] = qb.toSql()
      this.expressionMap.parameters.push(params)
      this.addSelect(`(${sql}) AS ${this.connection.escape(columnAlias)}`)

      return this
    }

    this.addToArray(this.expressionMap.selects, field)
    return this
  }

  /**
   * Adds a WHERE clause to the query.
   * @param statement The condition(s) to filter results, or a callback to build the condition.
   * @param params Optional parameters for the condition.
   * @returns {this} The current instance for method chaining.
   */
  public where(statement: string | CallbackFunction<this> | ObjectLiteral, params?: Params<T>): this {
    this.expressionMap.parameters = []
    if (typeof statement === 'object' && statement !== null) return this.addWhere(statement, 'simple')
    else if (typeof statement === 'function')
      return this.addWhere(statement as CallbackFunction<this>, 'simple', params)
    else return this.addWhere(statement as string, 'simple', params)
  }

  /**
   * Adds an AND WHERE clause to the query.
   * @param statement The condition(s) to filter results, or a callback to build the condition.
   * @param params Optional parameters for the condition.
   * @returns {this} The current instance for method chaining.
   */
  public andWhere(statement: string | CallbackFunction<this> | ObjectLiteral, params?: Params<T>): this {
    if (typeof statement === 'object' && statement !== null) return this.addWhere(statement, 'and')
    else if (typeof statement === 'function') return this.addWhere(statement as CallbackFunction<this>, 'and', params)
    else return this.addWhere(statement as string, 'and', params)
  }

  /**
   * Adds an OR WHERE clause to the query.
   * @param statement The condition(s) to filter results, or a callback to build the condition.
   * @param params Optional parameters for the condition.
   * @returns {this} The current instance for method chaining.
   */
  public orWhere(statement: string | CallbackFunction<this> | ObjectLiteral, params?: Params<T>): this {
    if (typeof statement === 'object' && statement !== null) return this.addWhere(statement, 'or')
    else if (typeof statement === 'function') return this.addWhere(statement as CallbackFunction<this>, 'or', params)
    else return this.addWhere(statement as string, 'or', params)
  }

  /**
   * Specifies the fields to group the results by in the query.
   * @param field The field(s) to group by, or an array of fields.
   * @returns {SelectQueryBuilder} The current instance for method chaining.
   */
  public groupBy(field: string): SelectQueryBuilder<T>
  public groupBy(fields: string[]): SelectQueryBuilder<T>
  public groupBy(field: string | string[]): SelectQueryBuilder<T> {
    this.expressionMap.groupBys = Array.isArray(field) ? field : [field]
    return this
  }

  /**
   * Specifies the fields to order the results by in the query.
   * @param field The field(s) to order by, or an array of fields.
   * @param direction The direction to order the results (e.g., 'ASC' or 'DESC').
   * @returns {SelectQueryBuilder} The current instance for method chaining.
   */
  public orderBy(field: string): SelectQueryBuilder<T>
  public orderBy(fields: string[]): SelectQueryBuilder<T>
  public orderBy(field: string, direction: string): SelectQueryBuilder<T>
  public orderBy(field: string[], direction: string): SelectQueryBuilder<T>
  public orderBy(field: string | string[] | ObjectLiteral, direction?: string): SelectQueryBuilder<T> {
    if (direction) this.expressionMap.orderByDirection = direction
    this.expressionMap.orderBys = Array.isArray(field) ? field : [field]
    return this
  }

  /**
   * Specifies the table from which to select results.
   * @param table The name of the table to select from, or a callback that returns a QueryBuilder.
   * @param alias Optional alias for the table.
   * @returns {SelectQueryBuilder} The current instance for method chaining.
   */
  public from(table: string, alias?: string): SelectQueryBuilder<T>
  public from(qb: CallbackFunction<this, T>): SelectQueryBuilder<T>
  public from(table: string | CallbackFunction<this, T>, alias?: string): SelectQueryBuilder<T> {
    if (typeof table === 'function') {
      this.expressionMap.table = `(${table(this.createQueryBuilder()).toSql()})`

      return this
    }

    this.expressionMap.table = table
    if (!alias) this.expressionMap.tableAlias = table.split('.').join('_')
    else this.expressionMap.tableAlias = alias

    return this
  }

  /**
   * Adds a JOIN clause to the query for an INNER join.
   * @param alias The alias for the joined table.
   * @param joinTable The name of the table to join.
   * @param condition The join condition.
   * @param params Optional parameters for the join condition.
   * @returns {this} The current instance for method chaining.
   */
  public innerJoin(table: string, alias: string, condition: string, params?: Params<any>): this {
    this.join('INNER', alias, table, condition, params)
    return this
  }

  /**
   * Adds a JOIN clause to the query for a LEFT join.
   * @param alias The alias for the joined table.
   * @param joinTable The name of the table to join.
   * @param condition The join condition.
   * @param params Optional parameters for the join condition.
   * @returns {this} The current instance for method chaining.
   */
  public leftJoin(table: string, alias: string, condition: string, params?: Params<any>): this {
    this.join('LEFT', alias, table, condition, params)
    return this
  }

  /**
   * Sets the maximum number of results to return from the query.
   * @param limit The maximum number of results to return.
   * @returns {SelectQueryBuilder} The current instance for method chaining.
   */
  public limit(limit: number): SelectQueryBuilder<T> {
    this.expressionMap.limit = limit
    return this
  }

  /**
   * Sets the number of results to skip before starting to return results.
   * Useful for pagination.
   * @param offset The number of results to skip.
   * @returns {SelectQueryBuilder} The current instance for method chaining.
   */
  public offset(offset: number): SelectQueryBuilder<T> {
    this.expressionMap.offset = offset
    return this
  }

  /**
   * Executes the built query and returns the results.
   * @returns {Promise<R[]>} A promise that resolves to an array of results of type R.
   */
  public async execute<R = T>(mapped?: boolean): Promise<R[]> {
    const result = await this.queryRunner.query<R>(...this.toSql())

    if (mapped) {
      const metadata = this.connection.getMetadata(this.expressionMap.table)
      if (typeof metadata.tableMetadataArgs.target === 'function') {
        return result.map((row) => {
          const target = metadata.tableMetadataArgs.target as any
          const instance = new target()

          for (const key in row) {
            const columnMetadata = metadata.columnMetadataArgs?.find((column) => column.options.name === key)
            if (columnMetadata) {
              instance[columnMetadata.propertyName] = row[key]
            } else throw new PropertyNotFoundError(key, target.name)
          }

          return instance
        })
      }
    }

    return result
  }
}
