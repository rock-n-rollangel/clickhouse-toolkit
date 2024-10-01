import { SelectQueryBuilder } from '../../../query-builder/select-query-builder'
import { SchemaOptionsBase } from './schema-options-base'

export interface MaterializedViewSchemaOptions extends SchemaOptionsBase {
  materialized?: boolean
  materializedTo?: string
  materializedQuery?: ((qb: SelectQueryBuilder) => SelectQueryBuilder) | string
}
