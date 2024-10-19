import { ObjectLiteral } from '../types/object-literal'
import { TableSchema } from '../types/table-schema'
import { QueryBuilder } from './query-builder'

/**
 * A builder class for creating INSERT SQL queries.
 * This class extends the QueryBuilder to provide methods specific to INSERT operations.
 */
export class InsertQueryBuilder extends QueryBuilder {
  readonly '@instanceof' = Symbol.for('InsertQueryBuilder')

  /**
   * Sets the values to insert into the target table.
   * @param callback - A function that modifies the query builder instance.
   * @returns The current InsertQueryBuilder instance for method chaining.
   */
  public values(callback: (qb: this) => this): InsertQueryBuilder
  /**
   * Sets a single value object to insert into the target table.
   * @param value - An object representing the values to insert.
   * @returns The current InsertQueryBuilder instance for method chaining.
   */
  public values(value: ObjectLiteral): InsertQueryBuilder
  /**
   * Sets multiple value objects to insert into the target table.
   * @param values - An array of objects representing the values to insert.
   * @returns The current InsertQueryBuilder instance for method chaining.
   */
  public values(values: ObjectLiteral[]): InsertQueryBuilder
  /**
   * Sets the values to insert into the target table.
   * @param value - A function, a single object, or an array of objects representing the values to insert.
   * @returns The current InsertQueryBuilder instance for method chaining.
   */
  public values(value: ((qb: this) => this) | ObjectLiteral | ObjectLiteral[]): InsertQueryBuilder {
    if (typeof value === 'function') {
      const [sql, processedParameters] = value(this.createQueryBuilder()).toSql()
      this.expressionMap.insertValues = sql
      this.setProcessedParameters(processedParameters)
    } else if (Array.isArray(value)) {
      this.expressionMap.insertValues = value
    } else {
      this.expressionMap.insertValues = [value]
    }

    return this
  }

  /**
   * Sets the target table for the insert operation.
   * @param target - The target table schema or table name as a string.
   * @param columns - Optional array of columns to insert values into.
   * @returns The current InsertQueryBuilder instance for method chaining.
   */
  public into(target: TableSchema | string, columns?: string[]): InsertQueryBuilder {
    if (typeof target === 'string') {
      this.expressionMap.table = target
    } else {
      this.expressionMap.table = target.name
    }

    if (columns) this.expressionMap.insertColumns = columns

    return this
  }

  /**
   * Executes the constructed INSERT SQL statement against the database.
   * @returns A promise that resolves when the command has been executed.
   */
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

  /**
   * Retrieves the values to insert, optionally filtering by specified columns.
   * @returns An array of objects or a string representing the values to insert.
   */
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
