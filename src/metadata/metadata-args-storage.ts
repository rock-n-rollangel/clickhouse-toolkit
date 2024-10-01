import { ColumnMetadataArgs } from './column-metadata-args'
import { TableMetadataArgs } from './table-metadata-args'

export class MetadataArgsStorage {
  readonly '@instanceof' = Symbol.for('MetadataArgsStorage')
  tables: TableMetadataArgs[] = []
  columns: ColumnMetadataArgs[] = []
}
