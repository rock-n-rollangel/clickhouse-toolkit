import { ColumnType } from './data-types'

export interface ColumnSchema {
  name: string
  type: ColumnType
  table: string
  isPrimaryKey: boolean
  isPartitionKey: boolean
  isSortingKey: boolean
}
