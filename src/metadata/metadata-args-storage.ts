import { ColumnMetadataArgs } from './column-metadata-args'
import { TableMetadataArgs } from './table-metadata-args'

/**
 * Storage for metadata arguments related to tables and columns.
 * This class holds the metadata for all tables and columns defined in the application.
 */
export class MetadataArgsStorage {
  // A unique symbol to ensure this is an instance of MetadataArgsStorage
  readonly '@instanceof' = Symbol.for('MetadataArgsStorage')

  // Array to store metadata about all defined tables
  tables: TableMetadataArgs[] = []

  // Array to store metadata about all defined columns
  columns: ColumnMetadataArgs[] = []
}
