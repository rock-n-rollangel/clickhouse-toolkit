import { ColumnMetadata } from '../common/column-metadata'
import { SchemaMetadata } from '../metadata/schema-metadata'
import { Engine } from '../types/engine'
import { MaterializedViewOptions } from '../types/materialized-view-options'
import { TableOptions } from '../types/table-options'
import { ColumnType } from '../types/data-types'
import { CallbackFunction } from '../types/callback-function'
import { MaterializedViewCannotHaveColumnsError } from '../errors/materialized-view-cannot-have-columns'
import { MaterializedViewCannotHaveEngineError } from '../errors/materialized-view-cannot-have-engine'

/**
 * The Table class represents both tables and materialized views.
 * It can handle standard tables with columns and order constraints, or materialized views with query definitions.
 */
export class Table {
  readonly '@instanceof' = Symbol.for('Table')

  /** Query string or function for materialized views. */
  query?: CallbackFunction<any> | string

  /** Optional target table or view to select data from (used for materialized views). */
  to?: string

  /** The engine used by the table, such as MergeTree or AggregatingMergeTree. */
  engine?: Engine

  /** Name of the table or view. */
  name: string

  /** Columns metadata for the table. */
  columns: ColumnMetadata[]

  /** Columns that are part of the primary key (if any). */
  primaryColumns?: ColumnMetadata[]

  /**
   * Constructor for creating a Table instance.
   * Handles both tables and materialized views based on provided options.
   *
   * @param tableOrMaterializedView - Either TableOptions or MaterializedViewOptions to define the structure.
   */
  constructor()
  constructor(table: TableOptions)
  constructor(materializedView: MaterializedViewOptions)
  constructor(tableOrMaterializedView?: TableOptions | MaterializedViewOptions) {
    if (tableOrMaterializedView) {
      if ('columns' in tableOrMaterializedView) {
        this.name = tableOrMaterializedView.name
        this.columns = tableOrMaterializedView.columns
        this.engine = tableOrMaterializedView.engine
        this.primaryColumns = tableOrMaterializedView.columns.filter((column) => column.primary || column.orderBy)
      } else if ('query' in tableOrMaterializedView) {
        this.query = tableOrMaterializedView.query
        this.to = tableOrMaterializedView.to
        this.name = tableOrMaterializedView.name
      }
    }
  }

  /**
   * Adds a column to the table.
   *
   * @param options - The options for the column.
   * @returns The Table instance.
   */
  addColumn(options: ColumnMetadata): this
  /**
   * Adds a column to the table.
   *
   * @param name - The name of the column.
   * @param type - The type of the column.
   * @returns The Table instance.
   */
  addColumn(name: string, type: ColumnType): this
  /**
   * Adds a column to the table.
   *
   * @param nameOrOptions - The name of the column or the options for the column.
   * @param type - The type of the column.
   * @returns The Table instance.
   */
  addColumn(nameOrOptions: string | ColumnMetadata, type?: ColumnType): this {
    if (this.query) {
      throw new MaterializedViewCannotHaveColumnsError()
    }

    if (typeof nameOrOptions === 'string') {
      this.columns.push({ name: nameOrOptions, type })
    } else {
      this.columns.push(nameOrOptions)
    }

    return this
  }

  /**
   * Sets the engine for the table.
   *
   * @param engine - The engine to be set.
   * @returns The Table instance.
   */
  setEngine(engine: Engine): this {
    if (this.query) {
      throw new MaterializedViewCannotHaveEngineError()
    }

    this.engine = engine

    return this
  }

  /**
   * Sets the name of the table.
   *
   * @param name - The name of the table.
   * @returns The Table instance.
   */
  setName(name: string): this {
    this.name = name

    return this
  }

  /**
   * Sets the query for the table.
   *
   * @param query - The query to be set.
   * @returns The Table instance.
   */
  setQuery(query: string): this
  setQuery(query: CallbackFunction<any>): this
  setQuery(query: CallbackFunction<any> | string): this {
    this.query = query
    return this
  }

  /**
   * Sets the target table for the materialized view.
   *
   * @param to - The target table.
   * @returns The Table instance.
   */
  setTo(to: Table): this
  /**
   * Sets the target table for the materialized view.
   *
   * @param to - The target table.
   * @returns The Table instance.
   */
  setTo(to: string): this
  /**
   * Sets the target table for the materialized view.
   *
   * @param to - The target table.
   * @returns The Table instance.
   */
  setTo(to: Table | string): this {
    this.to = typeof to === 'string' ? to : to.name
    return this
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
