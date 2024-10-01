import { ColumnMetadata } from '../metadata/column-metadata'
import { SchemaMetadata } from '../metadata/schema-metadata'
import { QueryBuilder } from '../query-builder/query-builder'
import { SelectQueryBuilder } from '../query-builder/select-query-builder'
import { Engine } from '../types/engine'
import { MaterializedViewOptions } from '../types/materialized-view-options'
import { TableOptions } from '../types/table-options'

export class Table {
  readonly '@instanceof' = Symbol.for('Table')
  protected queryBuilder?: SelectQueryBuilder

  query?: ((qb: QueryBuilder) => QueryBuilder) | string
  to?: string
  engine?: Engine
  name: string
  columns: ColumnMetadata[]
  orderColumns?: ColumnMetadata[]
  primaryColumns?: ColumnMetadata[]

  // todo: separate view
  constructor(table: TableOptions)
  constructor(materializedView: MaterializedViewOptions)
  constructor(tableOrMaterializedView: TableOptions | MaterializedViewOptions) {
    if ('columns' in tableOrMaterializedView) {
      this.name = tableOrMaterializedView.name
      this.columns = tableOrMaterializedView.columns
      this.engine = tableOrMaterializedView.engine
      this.orderColumns = tableOrMaterializedView.columns.filter((column) => column.orderBy)
      this.primaryColumns = tableOrMaterializedView.columns.filter((column) => column.primary)
    } else if ('query' in tableOrMaterializedView) {
      this.query = tableOrMaterializedView.query
      this.to = tableOrMaterializedView.to
      this.name = tableOrMaterializedView.name
    }
  }

  public static create(entityMetadata: SchemaMetadata): Table {
    if (entityMetadata.tableMetadataArgs.materialized) {
      return new Table({
        name: entityMetadata.tableMetadataArgs.name,
        query: entityMetadata.tableMetadataArgs.materializedQuery,
        to: entityMetadata.tableMetadataArgs.materializedTo,
      })
    }

    return new Table({
      name: entityMetadata.tableMetadataArgs.name,
      columns: entityMetadata.getColumnMetadatas(),
      engine: entityMetadata.tableMetadataArgs.engine!,
    })
  }
}
