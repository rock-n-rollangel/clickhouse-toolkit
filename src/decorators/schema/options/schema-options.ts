import { Engine } from '../../../types/engine'
import { SchemaOptionsBase } from './schema-options-base'

/**
 * Options for creating a schema.
 * Extends the base schema options with additional properties specific to non-materialized views.
 */
export interface SchemaOptions extends SchemaOptionsBase {
  engine: Engine // The engine used for the schema, e.g., MergeTree, etc.
}
