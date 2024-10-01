import { getMetadataArgsStorage } from '../../globals'
import { SchemaOptions } from './options/schema-options'
import { snakeCase } from '../../util/string'
import { MaterializedViewSchemaOptions } from './options/materialized-options'

export function Schema(): ClassDecorator
export function Schema(mvOptions: MaterializedViewSchemaOptions): ClassDecorator
export function Schema(options: SchemaOptions): ClassDecorator
export function Schema(options?: SchemaOptions | MaterializedViewSchemaOptions): ClassDecorator {
  return function (constructor) {
    getMetadataArgsStorage().tables.push({
      target: constructor,
      engine: options && 'engine' in options ? options.engine : null,
      name: options?.name ?? snakeCase(constructor.name),
      materialized: options && 'materialized' in options ? options.materialized : null,
      materializedTo: options && 'materializedTo' in options ? options.materializedTo : null,
      materializedQuery: options && 'materializedQuery' in options ? options.materializedQuery : null,
    })
  }
}
