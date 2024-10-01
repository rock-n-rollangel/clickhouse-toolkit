import { ColumnType } from '@/types/data-types'

export class ColumnOptions {
  type: ColumnType
  name?: string
  nullable?: boolean
  unique?: boolean
  primary?: boolean
  orderBy?: boolean
}
