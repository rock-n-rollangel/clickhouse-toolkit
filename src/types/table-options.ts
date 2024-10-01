import { ColumnMetadata } from '../metadata/column-metadata'
import { BaseTableOptions } from './base-table-options'
import { Engine } from './engine'

export interface TableOptions extends BaseTableOptions {
  name: string
  columns: ColumnMetadata[]
  engine: Engine
}
