import { Connection } from '../connection/connection'
import { QueryRunner } from '../query-runner/query-runner'
import { ColumnSchema } from '../types/column-schema'
import { TableSchema } from '../types/table-schema'
import { Table } from './table'
import { SchemaMetadata } from '../metadata/schema-metadata'

/**
 * SchemaBuilder class for synchronizing database schemas.
 * It handles creating, updating, and dropping tables and materialized views.
 */
export class SchemaBuilder {
  readonly '@instanceof' = Symbol.for('SchemaBuilder')

  /** Temporary prefix used for renaming tables during updates. */
  static readonly tempPrefix = '_temp_'

  /** The QueryRunner instance for executing queries. */
  private queryRunner: QueryRunner

  /** Database table metadata. */
  public databaseTableMetadatas: {
    name: string
    columns: ColumnSchema[]
  }[] = []

  /**
   * Initializes a new instance of the SchemaBuilder class.
   * @param connection - The connection instance to the database.
   */
  constructor(private connection: Connection) {
    this.queryRunner = this.connection.queryRunner
  }

  /**
   * Synchronizes the database schema by dropping old materialized views,
   * creating new tables, updating changed tables, and restoring materialized views.
   */
  public async synchronize(): Promise<void> {
    await this.preload()

    // Drop materialized views
    const materializedMetadatas = this.connection.schemaMetadatas.filter((metadata) => {
      return metadata.tableMetadataArgs.materialized
    })
    for (const metadata of materializedMetadatas) {
      await this.dropMaterializedView(metadata)
    }

    // Create new tables if they do not exist yet
    const changedTables = await this.findChangedTables()
    const newTableMetadatas = this.connection.schemaMetadatas.filter((metadata) => {
      return (
        !changedTables.find((changed) => changed.tableMetadataArgs.name === metadata.tableMetadataArgs.name) &&
        !metadata.tableMetadataArgs.materialized
      )
    })
    for (const metadata of newTableMetadatas) {
      await this.createTable(metadata)
    }

    // Update changed tables
    for (const metadata of changedTables) {
      await this.updateTable(metadata)
    }

    // Restore materialized views
    for (const metadata of materializedMetadatas) {
      await this.createMaterializedView(metadata)
    }
  }

  /**
   * Retrieves a list of tables in the current database.
   * @returns A promise that resolves to an array of TableSchema objects.
   */
  public async getTables(): Promise<TableSchema[]> {
    return await this.connection
      .createQueryBuilder()
      .select('name')
      .from('system.tables')
      .where(`database = '${this.connection.database}'`)
      .execute<TableSchema>()
  }

  /**
   * Retrieves metadata about columns in the current database.
   * @returns A promise that resolves to an array of ColumnSchema objects.
   */
  public async getColumns(): Promise<ColumnSchema[]> {
    return await this.connection
      .createQueryBuilder()
      .select([
        'name',
        'type',
        'table',
        'is_in_primary_key AS isPrimaryKey',
        'is_in_sorting_key AS isSortingKey',
        'is_in_partition_key AS isPartitionKey',
      ])
      .from('system.columns')
      .where(`database = '${this.connection.database}'`)
      .execute<ColumnSchema>()
  }

  /**
   * Preloads table and column metadata into memory.
   */
  public async preload(): Promise<void> {
    const tables = await this.getTables()
    const columns = await this.getColumns()

    tables.forEach((table) => {
      this.databaseTableMetadatas.push({
        name: table.name,
        columns: columns.filter((column) => column.table === table.name),
      })
    })
  }

  /**
   * Drops a materialized view from the database.
   * @param metadata - The schema metadata for the materialized view to drop.
   */
  public async dropMaterializedView(metadata: SchemaMetadata): Promise<void> {
    await this.queryRunner.dropTable(metadata, true)
  }

  /**
   * Creates a materialized view in the database based on schema metadata.
   * @param metadata - The schema metadata for the materialized view to create.
   */
  public async createMaterializedView(metadata: SchemaMetadata): Promise<void> {
    await this.queryRunner.createTable(Table.create(metadata))
  }

  /**
   * Finds tables that have changed schemas.
   * @returns A promise that resolves to an array of changed SchemaMetadata objects.
   */
  public async findChangedTables(): Promise<SchemaMetadata[]> {
    const metadatas: SchemaMetadata[] = []

    for (const metadata of this.connection.schemaMetadatas) {
      const tableMetadata = this.databaseTableMetadatas.find(
        (tableMetadata) => tableMetadata.name === metadata.tableMetadataArgs.name,
      )

      if (metadata.tableMetadataArgs.materialized) continue

      if (!tableMetadata) continue

      const changedColumn = metadata.columnMetadataArgs.filter((metadataColumn) =>
        tableMetadata.columns.find(
          (tableColumn) =>
            tableColumn.name === metadataColumn.options.name && tableColumn.type === metadataColumn.options.type,
        ),
      )

      if (changedColumn) {
        metadatas.push(metadata)
      }
    }

    return metadatas
  }

  /**
   * Creates a new table in the database based on schema metadata.
   * @param metadata - The schema metadata for the table to create.
   */
  public async createTable(metadata: SchemaMetadata): Promise<void> {
    await this.queryRunner.createTable(Table.create(metadata), true)
  }

  /**
   * Updates an existing table by backing it up, creating a new version,
   * restoring data from the backup, and dropping the old table.
   * @param metadata - The schema metadata for the table to update.
   */
  public async updateTable(metadata: SchemaMetadata): Promise<void> {
    await this.backupTable(metadata)
    await this.createTable(metadata)
    await this.restoreBackup(metadata)
    await this.queryRunner.dropTable(SchemaBuilder.tempPrefix + metadata.tableMetadataArgs.name, true)
  }

  /**
   * Backs up an existing table by renaming it with a temporary prefix.
   * @param metadata - The schema metadata for the table to back up.
   */
  public async backupTable(metadata: SchemaMetadata): Promise<void> {
    await this.queryRunner.dropTable(SchemaBuilder.tempPrefix + metadata.tableMetadataArgs.name, true)
    await this.queryRunner.renameTable(
      metadata.tableMetadataArgs.name,
      SchemaBuilder.tempPrefix + metadata.tableMetadataArgs.name,
    )
  }

  /**
   * Restores data from a backup table to the original table.
   * @param metadata - The schema metadata for the table to restore.
   */
  public async restoreBackup(metadata: SchemaMetadata): Promise<void> {
    await this.connection
      .createQueryBuilder()
      .insert()
      .into(metadata.tableMetadataArgs.name)
      .values((qb) => qb.select().from(SchemaBuilder.tempPrefix + metadata.tableMetadataArgs.name))
      .execute()
  }
}
