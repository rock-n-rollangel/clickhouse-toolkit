import { Connection } from '@/connection/connection'
import { ColumnMetadata } from './column-metadata'
import { ColumnMetadataArgs } from './column-metadata-args'
import { TableMetadataArgs } from './table-metadata-args'

export class SchemaMetadata {
  readonly '@instanceof' = Symbol.for('SchemaMetadata')

  connection: Connection

  tableMetadataArgs?: TableMetadataArgs

  columnMetadataArgs?: ColumnMetadataArgs[]

  constructor(connection: Connection) {
    this.connection = connection
  }

  getColumnMetadatas(): ColumnMetadata[] {
    return this.columnMetadataArgs!.map((columnMetadataArg) => ({
      name: columnMetadataArg.options.name,
      type: columnMetadataArg.options.type,
      nullable: columnMetadataArg.options.nullable,
      unique: columnMetadataArg.options.unique,
      primary: columnMetadataArg.options.primary,
      orderBy: columnMetadataArg.options.orderBy,
    }))
  }
}
