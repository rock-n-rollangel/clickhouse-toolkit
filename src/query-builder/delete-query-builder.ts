import { ObjectLiteral } from '../types/object-literal'
import { QueryBuilder } from './query-builder'
import { Params } from '../types/params'
import { QueryBuilderCallback } from '../types/query-builder-callback'
import { WhereExpressionBuilder } from './where-expression-builder'

/**
 * A builder class for creating DELETE SQL queries.
 * This class extends the QueryBuilder and implements the WhereExpressionBuilder.
 */
export class DeleteQueryBuilder extends QueryBuilder implements WhereExpressionBuilder {
  readonly '@instanceof' = Symbol.for('DeleteQueryBuilder')

  /**
   * Adds a WHERE clause to the delete query.
   * @param statement - A string, an object, or a function representing the WHERE condition.
   * @param params - Optional parameters to bind to the query.
   * @returns The current DeleteQueryBuilder instance for method chaining.
   */
  public where(statement: string | ((qb: this) => this) | ObjectLiteral, params?: Params): this {
    if (typeof statement === 'object' && statement !== null) return this.addWhere(statement, 'simple')
    else if (typeof statement === 'function') return this.addWhere(statement as QueryBuilderCallback, 'simple', params)
    else return this.addWhere(statement as string, 'simple', params)
  }

  /**
   * Adds an AND WHERE clause to the delete query.
   * @param statement - A string, an object, or a function representing the AND WHERE condition.
   * @param params - Optional parameters to bind to the query.
   * @returns The current DeleteQueryBuilder instance for method chaining.
   */
  public andWhere(statement: string | ((qb: this) => this) | ObjectLiteral, params?: Params): this {
    if (typeof statement === 'object' && statement !== null) return this.addWhere(statement, 'and')
    else if (typeof statement === 'function') return this.addWhere(statement as QueryBuilderCallback, 'and', params)
    else return this.addWhere(statement as string, 'and', params)
  }

  /**
   * Adds an OR WHERE clause to the delete query.
   * @param statement - A string, an object, or a function representing the OR WHERE condition.
   * @param params - Optional parameters to bind to the query.
   * @returns The current DeleteQueryBuilder instance for method chaining.
   */
  public orWhere(statement: string | ((qb: this) => this) | ObjectLiteral, params?: Params): this {
    if (typeof statement === 'object' && statement !== null) return this.addWhere(statement, 'or')
    else if (typeof statement === 'function') return this.addWhere(statement as QueryBuilderCallback, 'or', params)
    else return this.addWhere(statement as string, 'or', params)
  }

  /**
   * Sets the limit for the number of rows to delete.
   * @param limit - The maximum number of rows to delete.
   * @returns The current DeleteQueryBuilder instance for method chaining.
   */
  public limit(limit: number): DeleteQueryBuilder {
    this.expressionMap.limit = limit
    return this
  }

  /**
   * Sets the offset for the delete query.
   * @param offset - The number of rows to skip before starting to delete.
   * @returns The current DeleteQueryBuilder instance for method chaining.
   */
  public offset(offset: number): DeleteQueryBuilder {
    this.expressionMap.offset = offset
    return this
  }

  /**
   * Converts the delete query builder into a SQL DELETE statement.
   * @returns The SQL DELETE statement as a string.
   */
  public toSql(): string {
    let sql = this.parseDelete() + this.parseTable() + this.parseWhere() + this.parseLimit() + this.parseOffset()
    if (this.expressionMap.whereClauses.length > 0) sql = this.processParams(sql, this.getParameters())

    return sql
  }

  /**
   * Executes the constructed DELETE SQL statement against the database.
   * @returns A promise that resolves when the command has been executed.
   */
  public async execute(): Promise<void> {
    await this.queryRunner.command(this.toSql(), this.getProcessedParameters())
  }

  /**
   * Parses the DELETE portion of the SQL statement.
   * @returns The DELETE keyword as a string.
   */
  protected parseDelete(): string {
    return `DELETE FROM`
  }

  /**
   * Parses the table name from the expression map.
   * @returns The full table path as a string.
   */
  protected parseTable(): string {
    return ` ${this.connection.getFullTablePath(this.expressionMap.table)}`
  }

  /**
   * Parses the LIMIT clause for the SQL statement, if set.
   * @returns The LIMIT clause as a string, or an empty string if not set.
   */
  protected parseLimit(): string {
    if (!this.expressionMap.limit) return ''
    return ` LIMIT ${this.expressionMap.limit}`
  }

  /**
   * Parses the OFFSET clause for the SQL statement, if set.
   * @returns The OFFSET clause as a string, or an empty string if not set.
   */
  protected parseOffset(): string {
    if (!this.expressionMap.offset) return ''
    return ` OFFSET ${this.expressionMap.offset}`
  }
}
