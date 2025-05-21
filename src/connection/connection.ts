import { ClickHouseClient, createClient, DataFormat, InsertResult } from '@clickhouse/client'
import { SchemaMetadataNotFoundError } from '../errors/schema-not-found'
import { Params } from '../types/params'
import { SelectQueryBuilder } from '../query-builder/select-query-builder'
import { QueryRunner } from '../query-runner/query-runner'
import { ConnectionOptions } from '../types/connection-options'
import { SchemaMetadata } from '../metadata/schema-metadata'
import { getMetadataArgsStorage } from '../globals'
import { registerQueryBuilders } from '../util/register-query-builders'
import { SchemaBuilder } from '../schema-builder/schema-builder'
import { ConnectionOptionsError } from '../errors/connection-options'
import { ObjectLiteral } from '../types/object-literal'
import { Migrator } from '../migrator/migrator'

/**
 * The Connection class manages the connection to a ClickHouse database.
 * It handles query execution, schema management, and metadata storage.
 */
export class Connection {
  readonly '@instanceof' = Symbol.for('Connection')

  /** The ClickHouse client instance used for executing commands. */
  private client: ClickHouseClient

  /** The optional query runner for executing queries. */
  private _queryRunner?: QueryRunner

  /** The migrator instance for running migrations. */
  private readonly _migrator: Migrator

  /** Array of schema metadata representing the database structure. */
  public readonly schemaMetadatas: SchemaMetadata[] = []

  /** The name of the connected database. */
  public readonly database: string

  /** Optional flag for logging SQL queries. */
  public readonly logging?: boolean

  /**
   * Private constructor to create a Connection instance.
   * Initializes the ClickHouse client and collects schema metadata.
   *
   * @param options - Connection options including URL, username, password, database, and settings.
   */
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
    this._migrator = new Migrator(
      this,
      options.migrationsTableName ?? 'clickhouse_toolkit_migrations',
      options.migrations,
    )

    registerQueryBuilders()

    const metadataArgsStorage = getMetadataArgsStorage()
    for (const tableMetadata of metadataArgsStorage.tables) {
      const schemaMetadata = new SchemaMetadata(this)
      schemaMetadata.tableMetadataArgs = tableMetadata
      schemaMetadata.columnMetadataArgs = metadataArgsStorage.columns.filter(
        (column) => column.target === tableMetadata.target,
      )
      this.schemaMetadatas.push(schemaMetadata)
      this._queryRunner = this.queryRunner
    }
  }

  /**
   * Finds metadata for a specified target (either a class or table name).
   *
   * @param target - The class or name of the table for which metadata is sought.
   * @returns The found SchemaMetadata.
   * @throws SchemaMetadataNotFoundError if no metadata is found for the target.
   */
  // eslint-disable-next-line @typescript-eslint/ban-types
  protected findMetadata(target: Function | string): SchemaMetadata {
    const metadata = this.schemaMetadatas.find((metadata) => {
      return metadata.tableMetadataArgs.target === target || metadata.tableMetadataArgs.name === target
    })

    if (metadata) return metadata
    else throw new SchemaMetadataNotFoundError(typeof target === 'string' ? target : target.name)
  }

  /**
   * Retrieves the query runner for executing queries.
   * Creates a new QueryRunner instance if one does not already exist.
   *
   * @returns The QueryRunner instance.
   */
  public get queryRunner(): QueryRunner {
    if (!this._queryRunner) this._queryRunner = new QueryRunner(this)
    return this._queryRunner
  }

  /**
   * Retrieves the migrator for running migrations.
   *
   * @returns The Migrator instance.
   */
  public get migrator(): Migrator {
    return this._migrator
  }

  /**
   * Creates a new SelectQueryBuilder for constructing SELECT queries.
   *
   * @returns A new instance of SelectQueryBuilder.
   */
  public createQueryBuilder<T extends ObjectLiteral>(): SelectQueryBuilder<T> {
    return new SelectQueryBuilder<T>(this, this.queryRunner)
  }

  /**
   * Creates a new SchemaBuilder for managing the database schema.
   *
   * @returns A new instance of SchemaBuilder.
   */
  public createSchemaBuilder(): SchemaBuilder {
    return new SchemaBuilder(this)
  }

  /**
   * Gets the metadata for a specified target (either a class or table name).
   *
   * @param target - The class or name of the table for which metadata is sought.
   * @returns The found SchemaMetadata.
   * @throws SchemaMetadataNotFoundError if no metadata is found for the target.
   */
  // eslint-disable-next-line @typescript-eslint/ban-types
  public getMetadata(target: Function | string): SchemaMetadata {
    return this.findMetadata(target)
  }

  /**
   * Returns the full path to a table, including the database name.
   *
   * @param table - The name of the table.
   * @returns The full table path in the format "database.table".
   */
  public getFullTablePath(table: string): string {
    if (table.includes('.')) {
      return `${table.split('.').map(this.escape).join('.')}`
    }
    return `${this.escape(this.database)}.${this.escape(table)}`
  }

  /**
   * Escapes a string for use in SQL queries by enclosing it in double quotes.
   *
   * @param target - The string to escape.
   * @returns The escaped string.
   */
  public escape(target: string): string {
    return `"${target}"`
  }

  /**
   * Executes a command on the ClickHouse database.
   *
   * @param query - The SQL query string to execute.
   * @param params - Optional parameters to be passed to the query.
   */
  public async command(query: string, params?: Params<any>): Promise<void> {
    await this.client.command({
      query: query,
      query_params: params,
    })
  }

  /**
   * Executes a query and returns the result as an array of typed objects.
   *
   * @param query - The SQL query string to execute.
   * @param params - Optional parameters to be passed to the query.
   * @returns An array of results of type T.
   */
  public async query<T>(query: string, params?: Params<any>): Promise<T[]> {
    const result = await this.client.query({
      query: query,
      query_params: params,
    })

    const { data } = await result.json<T>()

    return data
  }

  /**
   * Inserts an array of values into a specified table.
   *
   * @param values - The values to insert into the table.
   * @param table - The name of the target table.
   * @param format - The format of the input data (default is 'JSONEachRow').
   * @returns The result of the insert operation.
   */
  public async insert(values: any[], table: string, format: DataFormat = 'JSONEachRow'): Promise<InsertResult> {
    const result = await this.client.insert({
      table: table,
      values: values,
      format: format,
    })

    return result
  }

  /**
   * Initializes a new Connection instance with the specified options.
   * Validates the options and optionally synchronizes the schema if specified.
   *
   * @param options - Connection options including URL, username, password, database, and settings.
   * @returns A Promise that resolves to a new Connection instance.
   * @throws ConnectionOptionsError if any required options are missing.
   */
  public static async initialize(options: ConnectionOptions): Promise<Connection> {
    if (!options.database || !options.password || !options.username || !options.url) {
      throw new ConnectionOptionsError()
    }

    const instance = new Connection(options)

    if (options.synchronize === true) {
      await instance.createSchemaBuilder().synchronize()
    }

    if (options.runMigrations === true) {
      await instance._migrator.init()
      await instance._migrator.up()
    }

    return instance
  }
}
