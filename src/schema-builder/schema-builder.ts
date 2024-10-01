import { Connection } from '@/connection/connection'
import { QueryRunner } from '@/query-runner/query-runner'
import { ColumnSchema } from '@/types/column-schema'
import { TableSchema } from '@/types/table-schema'
import { Table } from './table'
import { SchemaMetadata } from '@/metadata/schema-metadata'

/*
  1. Remove materialized views
  2. Rename old tables with <prefix>_<name>
  3. Create tables with changed metadata
  4. Clone data from old tables into new tables
  5. Drop old tables
  6. Restore materialized views
*/
export class SchemaBuilder {
  readonly '@instanceof' = Symbol.for('SchemaBuilder')
  static readonly tempPrefix = '_temp_'
  private queryRunner: QueryRunner
  public databaseTableMetadatas: {
    name: string
    columns: ColumnSchema[]
  }[] = []

  constructor(private connection: Connection) {
    this.queryRunner = this.connection.queryRunner
  }

  public async synchronize(): Promise<void> {
    await this.preload()

    // first need to drop views
    const materializedMetadatas = this.connection.schemaMetadatas.filter((metadata) => {
      return metadata.tableMetadataArgs.materialized
    })
    for (const metadata of materializedMetadatas) {
      await this.dropMaterializedView(metadata)
    }

    // create tables that is not exists yet
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

    // find only changed schemas. we don't need to work all of tables, right?
    for (const metadata of changedTables) {
      // update table
      await this.updateTable(metadata)
    }

    // now we need to restore views
    for (const metadata of materializedMetadatas) {
      await this.createMaterializedView(metadata)
    }
  }

  public async getTables(): Promise<TableSchema[]> {
    return await this.connection
      .createQueryBuilder()
      .select('name')
      .from('system.tables')
      .where(`database = '${this.connection.database}'`)
      .execute<TableSchema>()
  }

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

  public async dropMaterializedView(metadata: SchemaMetadata): Promise<void> {
    await this.queryRunner.dropTable(metadata, true)
  }

  public async createMaterializedView(metadata: SchemaMetadata): Promise<void> {
    await this.queryRunner.createTable(Table.create(metadata))
  }

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

  public async createTable(metadata: SchemaMetadata): Promise<void> {
    await this.queryRunner.createTable(Table.create(metadata), true)
  }

  public async updateTable(metadata: SchemaMetadata): Promise<void> {
    await this.backupTable(metadata)
    await this.createTable(metadata)
    await this.restoreBackup(metadata)
    await this.queryRunner.dropTable(SchemaBuilder.tempPrefix + metadata.tableMetadataArgs.name, true)
  }

  public async backupTable(metadata: SchemaMetadata): Promise<void> {
    await this.queryRunner.dropTable(SchemaBuilder.tempPrefix + metadata.tableMetadataArgs.name, true)
    await this.queryRunner.renameTable(
      metadata.tableMetadataArgs.name,
      SchemaBuilder.tempPrefix + metadata.tableMetadataArgs.name,
    )
  }

  public async restoreBackup(metadata: SchemaMetadata): Promise<void> {
    await this.connection
      .createQueryBuilder()
      .insert()
      .into(metadata.tableMetadataArgs.name)
      .values((qb) => qb.select().from(SchemaBuilder.tempPrefix + metadata.tableMetadataArgs.name))
      .execute()
  }
}
