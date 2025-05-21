import { Connection } from '../connection/connection'
import { ColumnMetadata } from '../common/column-metadata'
import { ColumnMetadataArgs } from './column-metadata-args'
import { TableMetadataArgs } from './table-metadata-args'

/**
 * Represents metadata for a database schema.
 * This class stores information about the connection, table metadata, and column metadata.
 */
export class SchemaMetadata {
  // A unique symbol to ensure this is an instance of SchemaMetadata
  readonly '@instanceof' = Symbol.for('SchemaMetadata')

  // The connection object associated with this schema
  connection: Connection

  // Metadata arguments for the table associated with this schema
  tableMetadataArgs?: TableMetadataArgs

  // Array of metadata arguments for columns in the schema
  columnMetadataArgs?: ColumnMetadataArgs[]

  /**
   * Constructs a SchemaMetadata object with a specific database connection.
   * @param connection - The connection to the database
   */
  constructor(connection: Connection) {
    this.connection = connection
  }

  /**
   * Returns metadata for each column in the schema.
   * Maps the column metadata arguments to a simplified ColumnMetadata array.
   * @returns An array of ColumnMetadata objects representing column details.
   */
  getColumnMetadatas(): ColumnMetadata[] {
    return this.columnMetadataArgs!.map((columnMetadataArg) => ({
      name: columnMetadataArg.options.name, // Column name
      type: columnMetadataArg.options.type, // Data type of the column
      nullable: columnMetadataArg.options.nullable, // Indicates if the column can be null
      unique: columnMetadataArg.options.unique, // Indicates if the column is unique
      primary: columnMetadataArg.options.primary, // Indicates if the column is a primary key
      orderBy: columnMetadataArg.options.orderBy, // Indicates if the column is used in ORDER BY
      function: columnMetadataArg.options.function, // A function to be applied to the column (e.g., aggregate function)
    }))
  }
}
