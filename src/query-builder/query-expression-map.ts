import { SchemaMetadata } from '../metadata/schema-metadata'
import { ObjectLiteral } from '../types/object-literal'
import { Params } from '../types/params'
import { WhereClause } from '../types/where-clause'
import { JoinAttribute } from './join-attribute'

/**
 * Represents a map of query expression parameters used to construct SQL queries.
 */
export class QueryExpressionMap {
  /**
   * A symbol to identify instances of this class.
   * Used for type-checking with `instanceof`.
   */
  readonly '@instanceof' = Symbol.for('QueryExpressionMap')

  /** The name of the table associated with the query. */
  table: string

  /** An optional alias for the table. */
  tableAlias?: string

  /** An array of selected columns for the query. */
  selects: string[] = []

  /** An array of GROUP BY columns for the query. */
  groupBys: string[] = []

  /** An array of ORDER BY columns for the query. */
  orderBys: string[] = []

  /** The direction of sorting for the ORDER BY clause (default is 'desc'). */
  orderByDirection: string = 'desc'

  /** The maximum number of records to return. */
  limit?: number

  /** The offset for pagination, specifying how many records to skip. */
  offset?: number

  /** An array of parameters for the query. */
  parameters: Params[] = []

  /** A collection of processed parameters after they have been applied. */
  processedParameters: Params = {}

  /** Metadata associated with the schema of the table. */
  metadata?: SchemaMetadata

  /** Values to be inserted into the table. Can be an array of objects or a raw SQL string. */
  insertValues: ObjectLiteral[] | string = []

  /** An optional array of columns for insert operations. */
  insertColumns?: string[]

  /** An optional object representing the value(s) to be updated. */
  updateValue?: ObjectLiteral

  /** An optional array of columns for update operations. */
  updateColumns?: string[]

  /** An array of join attributes for the query. */
  joinAttributes: JoinAttribute[] = []

  /** An array of WHERE clauses for the query. */
  whereClauses: WhereClause[] = []

  /**
   * Creates a deep clone of the current QueryExpressionMap instance.
   *
   * @returns A new instance of QueryExpressionMap with the same property values as the original.
   */
  public clone(): QueryExpressionMap {
    const map = new QueryExpressionMap()
    map.table = this.table
    map.tableAlias = this.tableAlias
    map.selects = this.selects
    map.groupBys = this.groupBys
    map.orderBys = this.orderBys
    map.orderByDirection = this.orderByDirection
    map.limit = this.limit
    map.offset = this.offset
    map.parameters = this.parameters
    map.processedParameters = this.processedParameters
    map.metadata = this.metadata
    map.insertValues = this.insertValues
    map.insertColumns = this.insertColumns
    map.updateValue = this.updateValue
    map.joinAttributes = this.joinAttributes

    return map
  }
}
