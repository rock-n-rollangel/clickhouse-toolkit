import { Params } from '../types/params'
import { Connection } from '../connection/connection'
import { SchemaMetadata } from '../metadata/schema-metadata'
import { InstanceChecker } from '../util/instance-checker'
import { Table } from '../schema-builder/table'
import { ColumnMetadata } from '../metadata/column-metadata'
import { Engine } from '../types/engine'
import { QueryBuilderCallback } from '../types/query-builder-callback'

export class QueryRunner {
  readonly '@instanceof' = Symbol.for('QueryRunner')
  constructor(
    protected connection: Connection,
    protected metadata?: SchemaMetadata,
  ) {}

  public setMetadata(metadata: SchemaMetadata): this {
    this.metadata = metadata
    return this
  }

  // todo: separate view
  public async createTable(table: Table, ifNotExists?: boolean): Promise<void> {
    await this.command(this.createTableSql(table, ifNotExists))
  }

  public async dropTable(table: string, ifExists?: boolean, isView?: boolean): Promise<void>
  public async dropTable(schema: SchemaMetadata, ifExists?: boolean): Promise<void>
  public async dropTable(table: string | SchemaMetadata, ifExists?: boolean, isView?: boolean): Promise<void> {
    let tableName: string
    let materialized: boolean = isView

    if (InstanceChecker.isSchemaMetadata(table)) {
      tableName = table.tableMetadataArgs.name
      materialized = table.tableMetadataArgs.materialized
    } else tableName = table

    await this.command(
      `DROP ${materialized ? 'VIEW' : 'TABLE'} ${ifExists ? 'IF EXISTS' : ''} ${this.connection.escape(tableName)}`,
    )
  }

  public async renameTable(from: string, to: string): Promise<void> {
    await this.command(`RENAME ${this.connection.getFullTablePath(from)} TO ${this.connection.getFullTablePath(to)}`)
  }

  public async query<T>(sql: string, params?: Params): Promise<T[]> {
    this.log(sql, params)
    const result = await this.connection.query<T>(sql, params)
    return result
  }

  public async command(sql: string, params?: Params): Promise<void> {
    this.log(sql, params)
    await this.connection.command(sql, params)
  }

  public async insert(values: any[], table: string) {
    await this.connection.insert(values, table)
  }

  protected createTableSql(table: Table, ifNotExists?: boolean): string {
    let sql: string

    if ('to' in table) {
      sql =
        `CREATE MATERIALIZED VIEW ${ifNotExists ? 'IF NOT EXISTS' : ''} ${table.name} TO ${table.to} AS ` +
        this.createViewQuerySql(table.query)
    } else {
      sql =
        `CREATE TABLE ${ifNotExists ? 'IF NOT EXISTS' : ''} ${this.connection.escape(table.name)} ` +
        this.createColumnsSql(table.columns) +
        this.createEngineSql(table.engine) +
        this.createPrimaryKeySql(table.primaryColumns) +
        this.createOrderBySql(table.orderColumns)
    }

    return sql
  }

  protected createViewQuerySql(query: QueryBuilderCallback | string): string {
    if (typeof query === 'string') return query
    return query(this.connection.createQueryBuilder()).toSql()
  }

  protected createColumnsSql(columns: ColumnMetadata[]): string {
    return `( ${columns
      .map(
        (column) =>
          `${this.connection.escape(column.name)} ${column.nullable ? `Nullable(${column.type})` : column.type}${column.unique ? ` COMMENT 'UNIQUE FIELD'` : ''}`,
      )
      .join(', ')} ) `
  }

  protected createEngineSql(engine: Engine): string {
    return `ENGINE ${engine} `
  }

  protected createPrimaryKeySql(columns: ColumnMetadata[]): string {
    return `PRIMARY KEY (${columns
      .map((column) =>
        column.function
          ? `${column.function}(${this.connection.escape(column.name)})`
          : this.connection.escape(column.name),
      )
      .join(', ')}) `
  }

  protected createOrderBySql(columns: ColumnMetadata[]): string {
    return `ORDER BY (${columns
      .map((column) =>
        column.function
          ? `${column.function}(${this.connection.escape(column.name)})`
          : this.connection.escape(column.name),
      )
      .join(', ')})`
  }

  protected log(sql: string, params?: Params): void {
    if (!this.connection.logging) return
    if (!params) console.log(`QUERY: ${sql.trim()}`)
    else {
      console.log(`QUERY: ${sql.trim()}`, `PARAMS: [ ${Object.values(params).join(', ')} ]`)
    }
  }
}
