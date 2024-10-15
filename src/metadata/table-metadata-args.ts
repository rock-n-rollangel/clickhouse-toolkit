import { SelectQueryBuilder } from '../query-builder/select-query-builder'
import { Engine } from '../types/engine'

/**
 * Represents metadata arguments for a database table.
 * This class holds information about the table's target, engine, name, and additional options.
 */
export class TableMetadataArgs {
  // The target function or string representing the schema or class associated with the table
  // eslint-disable-next-line @typescript-eslint/ban-types
  target: Function | string

  // The storage engine used for the table (e.g., MergeTree, Log, etc.)
  engine?: Engine

  // The name of the table in the database
  name?: string

  // Indicates if the table is a materialized view
  materialized?: boolean

  // The name of the table to which the materialized view is pointing
  materializedTo?: string

  // A query to be executed for the materialized view, can be a function or a string
  materializedQuery?: ((qb: SelectQueryBuilder) => SelectQueryBuilder) | string
}
