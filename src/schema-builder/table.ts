import { ColumnMetadata } from '../metadata/column-metadata'
import { SchemaMetadata } from '../metadata/schema-metadata'
import { QueryBuilder } from '../query-builder/query-builder'
import { SelectQueryBuilder } from '../query-builder/select-query-builder'
import { Engine } from '../types/engine'
import { MaterializedViewOptions } from '../types/materialized-view-options'
import { TableOptions } from '../types/table-options'

/**
 * The Table class represents both tables and materialized views.
 * It can handle standard tables with columns and order constraints, or materialized views with query definitions.
 */
export class Table {
  readonly '@instanceof' = Symbol.for('Table')

  /** The optional query builder used for building select queries. */
  protected queryBuilder?: SelectQueryBuilder

  /** Query string or function for materialized views. */
  query?: ((qb: QueryBuilder) => QueryBuilder) | string

  /** Optional target table or view to select data from (used for materialized views). */
  to?: string

  /** The engine used by the table, such as MergeTree or AggregatingMergeTree. */
  engine?: Engine

  /** Name of the table or view. */
  name: string

  /** Columns metadata for the table. */
  columns: ColumnMetadata[]

  /** Columns that are used for ordering (if any). */
  orderColumns?: ColumnMetadata[]

  /** Columns that are part of the primary key (if any). */
  primaryColumns?: ColumnMetadata[]

  /**
   * Constructor for creating a Table instance.
   * Handles both tables and materialized views based on provided options.
   *
   * @param tableOrMaterializedView - Either TableOptions or MaterializedViewOptions to define the structure.
   */
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

  /**
   * Creates a Table instance based on provided schema metadata.
   * Differentiates between standard tables and materialized views.
   *
   * @param schemaMetadata - Metadata about the schema, including columns, engine, and query details.
   * @returns A new Table instance.
   */
  public static create(schemaMetadata: SchemaMetadata): Table {
    if (schemaMetadata.tableMetadataArgs.materialized) {
      return new Table({
        name: schemaMetadata.tableMetadataArgs.name,
        query: schemaMetadata.tableMetadataArgs.materializedQuery,
        to: schemaMetadata.tableMetadataArgs.materializedTo,
      })
    }

    return new Table({
      name: schemaMetadata.tableMetadataArgs.name,
      columns: schemaMetadata.getColumnMetadatas(),
      engine: schemaMetadata.tableMetadataArgs.engine!,
    })
  }
}
