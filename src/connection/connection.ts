import { ClickHouseClient, createClient, DataFormat, InsertResult } from '@clickhouse/client'
import { SchemaMetadataNotFoundError } from '../errors/schema-not-found'
import { Params } from '../types/params'
import { SelectQueryBuilder } from '../query-builder/select-query-builder'
import { QueryRunner } from '../query-runner/query-runner'
import { ConnectionOptions } from '../types/connection-options'
import { SchemaMetadata } from '../metadata/schema-metadata'
import { getMetadataArgsStorage } from '../globals'
import { registerQueryBuilders } from '../util/register-query-builders'
import { DatabaseSchema } from '../types/database-schema'
import { SchemaBuilder } from '../schema-builder/schema-builder'
import { ConnectionOptionsError } from '../errors/connection-options'

export class Connection {
  readonly '@instanceof' = Symbol.for('Connection')

  private client: ClickHouseClient
  private _queryRunner?: QueryRunner
  public readonly schemaMetadatas: SchemaMetadata[] = []
  public readonly schema?: DatabaseSchema
  public readonly database: string
  public readonly logging?: boolean

  private constructor(options: ConnectionOptions) {
    this.client = createClient({
      url: options.url,
      username: options.username,
      password: options.password,
      database: options.database,
      clickhouse_settings: options.settings,
      keep_alive: options.keepAlive,
    })

    this.database = options.database
    this.logging = options.logging

    registerQueryBuilders()

    const metadataArgsStorage = getMetadataArgsStorage()
    for (const tableMetadata of metadataArgsStorage.tables) {
      const entityMetadata = new SchemaMetadata(this)
      entityMetadata.tableMetadataArgs = tableMetadata
      entityMetadata.columnMetadataArgs = metadataArgsStorage.columns.filter(
        (column) => column.target === tableMetadata.target,
      )
      this.schemaMetadatas.push(entityMetadata)
      this._queryRunner = this.queryRunner
    }
  }

  // eslint-disable-next-line @typescript-eslint/ban-types
  protected findMetadata(target: Function | string): SchemaMetadata {
    const metadata = this.schemaMetadatas.find((metadata) => {
      return metadata.tableMetadataArgs.target === target || metadata.tableMetadataArgs.name === target
    })

    if (metadata) return metadata
    else throw new SchemaMetadataNotFoundError(typeof target === 'string' ? target : target.name)
  }

  public get queryRunner(): QueryRunner {
    if (!this._queryRunner) this._queryRunner = new QueryRunner(this)
    return this._queryRunner
  }

  public createQueryBuilder(): SelectQueryBuilder {
    return new SelectQueryBuilder(this, this.queryRunner)
  }

  public createSchemaBuilder(): SchemaBuilder {
    return new SchemaBuilder(this)
  }

  // eslint-disable-next-line @typescript-eslint/ban-types
  public getMetadata(target: Function | string): SchemaMetadata {
    return this.findMetadata(target)
  }

  public getFullTablePath(table: string): string {
    if (table.includes('.')) {
      return `${table.split('.').map(this.escape).join('.')}`
    }
    return `${this.escape(this.database)}.${this.escape(table)}`
  }

  public escape(target: string): string {
    return `"${target}"`
  }

  public async command(query: string, params?: Params): Promise<void> {
    await this.client.command({
      query: query,
      query_params: params,
    })
  }

  public async query<T>(query: string, params?: Params): Promise<T[]> {
    const result = await this.client.query({
      query: query,
      query_params: params,
    })

    const { data } = await result.json<T>()

    return data
  }

  public async insert(values: any[], table: string, format: DataFormat = 'JSONEachRow'): Promise<InsertResult> {
    const result = await this.client.insert({
      table: table,
      values: values,
      format: format,
    })

    return result
  }

  public static async initialize(options: ConnectionOptions): Promise<Connection> {
    if (!options.database || !options.password || !options.username || !options.url) {
      throw new ConnectionOptionsError()
    }

    const instance = new Connection(options)

    if (options.synchronize === true) {
      await instance.createSchemaBuilder().synchronize()
    }

    return instance
  }
}
