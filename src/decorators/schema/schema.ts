import { getMetadataArgsStorage } from '../../globals'
import { SchemaOptions } from './options/schema-options'
import { snakeCase } from '../../util/string'
import { MaterializedViewSchemaOptions } from './options/materialized-options'

/**
 * Decorator to define a schema or materialized view.
 * Can be used to annotate classes as database tables or views.
 */
export function Schema(): ClassDecorator
export function Schema(mvOptions: MaterializedViewSchemaOptions): ClassDecorator
export function Schema(options: SchemaOptions): ClassDecorator
export function Schema(options?: SchemaOptions | MaterializedViewSchemaOptions): ClassDecorator {
  return function (constructor) {
    // Store metadata for the schema definition (either table or materialized view).
    getMetadataArgsStorage().tables.push({
      target: constructor, // Class being decorated.
      engine: options && 'engine' in options ? options.engine : null, // Engine type if specified (e.g., MergeTree).
      name: options?.name ?? snakeCase(constructor.name), // Table name or auto-generated snake_case from class name.
      materialized: options && 'materialized' in options ? options.materialized : null, // Whether the schema is a materialized view.
      materializedTo: options && 'materializedTo' in options ? options.materializedTo : null, // Target table for materialized view data.
      materializedQuery: options && 'materializedQuery' in options ? options.materializedQuery : null, // Query for the materialized view.
    })
  }
}
