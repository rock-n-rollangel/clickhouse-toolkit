import { SelectQueryBuilder } from '../../../query-builder/select-query-builder'
import { SchemaOptionsBase } from './schema-options-base'

/**
 * Options specific to creating a materialized view schema.
 * Extends the base schema options and adds properties related to materialized views.
 */
export interface MaterializedViewSchemaOptions extends SchemaOptionsBase {
  materialized?: boolean // If true, defines the schema as a materialized view.
  materializedTo?: string // Specifies where the materialized data should be stored.
  materializedQuery?: ((qb: SelectQueryBuilder) => SelectQueryBuilder) | string // Query used to define the materialized view, either as a function or a raw query string.
}
