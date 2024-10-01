import { ColumnSchema } from './column-schema'
import { TableSchema } from './table-schema'

export interface DatabaseSchema {
  tables: TableSchema[]
  columns: ColumnSchema[]
}
