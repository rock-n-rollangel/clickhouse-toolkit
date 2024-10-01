import { SelectQueryBuilder } from '../query-builder/select-query-builder'
import { Engine } from '../types/engine'

export class TableMetadataArgs {
  // eslint-disable-next-line @typescript-eslint/ban-types
  target: Function | string
  engine?: Engine
  name?: string
  materialized?: boolean
  materializedTo?: string
  materializedQuery?: ((qb: SelectQueryBuilder) => SelectQueryBuilder) | string
}
