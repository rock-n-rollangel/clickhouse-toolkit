import { SelectQueryBuilder } from '@/query-builder/select-query-builder'
import { BaseTableOptions } from './base-table-options'

export interface MaterializedViewOptions extends BaseTableOptions {
  name: string
  query: ((qb: SelectQueryBuilder) => SelectQueryBuilder) | string
  to: string
}
