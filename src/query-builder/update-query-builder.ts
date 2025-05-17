import { ObjectLiteral } from '../types/object-literal'
import { TableNameError } from '../errors/table-name'
import { UpdateValueNotSetError } from '../errors/update-value-not-set'
import { QueryBuilder } from './query-builder'
import { Params } from '../types/params'
import { WhereExpressionBuilder } from './where-expression-builder'
import { CallbackFunction } from 'src/types/callback-function'

/**
 * UpdateQueryBuilder is responsible for building SQL UPDATE queries.
 * It extends the base QueryBuilder class and implements the WhereExpressionBuilder interface.
 */
export class UpdateQueryBuilder<T extends ObjectLiteral> extends QueryBuilder<T> implements WhereExpressionBuilder<T> {
  readonly '@instanceof' = Symbol.for('UpdateQueryBuilder')

  /**
   * Sets the values to be updated in the query.
   * @param value An object literal containing key-value pairs for the columns to update.
   * @returns {UpdateQueryBuilder} The current instance for method chaining.
   */
  public set(value: Partial<T>): UpdateQueryBuilder<T> {
    this.expressionMap.updateValue = value
    return this
  }

  /**
   * Adds a WHERE clause to the query.
   * @param statement A string, a function that receives the query builder, or an object literal for the condition.
   * @param params Optional parameters for the condition.
   * @returns {this} The current instance for method chaining.
   */
  public where(statement: string | CallbackFunction<this, T> | ObjectLiteral, params?: Params<T>): this {
    this.expressionMap.parameters = []
    if (typeof statement === 'object' && statement !== null) return this.addWhere(statement, 'simple')
    else if (typeof statement === 'function')
      return this.addWhere(statement as CallbackFunction<this, T>, 'simple', params)
    else return this.addWhere(statement as string, 'simple', params)
  }

  /**
   * Adds an AND WHERE clause to the query.
   * @param statement A string, a function that receives the query builder, or an object literal for the condition.
   * @param params Optional parameters for the condition.
   * @returns {this} The current instance for method chaining.
   */
  public andWhere(statement: string | CallbackFunction<this, T> | ObjectLiteral, params?: Params<T>): this {
    if (typeof statement === 'object' && statement !== null) return this.addWhere(statement, 'and')
    else if (typeof statement === 'function')
      return this.addWhere(statement as CallbackFunction<this, T>, 'and', params)
    else return this.addWhere(statement as string, 'and', params)
  }

  /**
   * Adds an OR WHERE clause to the query.
   * @param statement A string, a function that receives the query builder, or an object literal for the condition.
   * @param params Optional parameters for the condition.
   * @returns {this} The current instance for method chaining.
   */
  public orWhere(statement: string | CallbackFunction<this, T> | ObjectLiteral, params?: Params<T>): this {
    if (typeof statement === 'object' && statement !== null) return this.addWhere(statement, 'or')
    else if (typeof statement === 'function') return this.addWhere(statement as CallbackFunction<this, T>, 'or', params)
    else return this.addWhere(statement as string, 'or', params)
  }

  /**
   * Converts the query builder state into an SQL UPDATE string.
   * @returns {[string, ObjectLiteral]} The generated SQL string.
   */
  public toSql(): [string, ObjectLiteral] {
    // We need to set updateValues to parameters first
    const dumpParams = this.expressionMap.parameters
    this.expressionMap.parameters = []
    this.setParameters(this.expressionMap.updateValue)
    for (const param of dumpParams) this.setParameters(param)

    return this.preprocess(this.parseUpdate() + this.parseValues() + this.parseWhere(), this.getParameters())
  }

  /**
   * Executes the built query and returns the result.
   * @returns {Promise<any>} A promise that resolves to the result of the execution.
   */
  public async execute(): Promise<any> {
    return await this.queryRunner.command(...this.toSql())
  }

  /**
   * Parses the UPDATE part of the SQL query.
   * @returns {string} The parsed UPDATE SQL string.
   * @throws {TableNameError} If the table name is not set in the expression map.
   */
  protected parseUpdate(): string {
    if (!this.expressionMap.table) throw new TableNameError()

    return `ALTER TABLE ${this.connection.getFullTablePath(this.expressionMap.table)} UPDATE `
  }

  /**
   * Parses the values to be updated in the SQL query.
   * @returns {string} The parsed values SQL string.
   * @throws {UpdateValueNotSetError} If no update values are set.
   */
  protected parseValues(): string {
    if (!this.expressionMap.updateValue) {
      throw new UpdateValueNotSetError()
    }

    if (Array.isArray(this.expressionMap.updateColumns)) {
      this.expressionMap.updateValue = Object.fromEntries(
        Object.entries(this.expressionMap.updateValue).filter(([key]) =>
          this.expressionMap.updateColumns.includes(key),
        ),
      ) as Partial<T>
    }

    return Object.entries(this.expressionMap.updateValue)
      .map(([key]) => {
        return `${this.addAlias(this.connection.escape(key))} = :${key}`
      })
      .join(', ')
  }
}
