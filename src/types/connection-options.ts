import { ClickHouseSettings } from '@clickhouse/client'

/**
 * ConnectionOptions interface defines the configuration options
 * for establishing a connection to a ClickHouse database.
 */
export interface ConnectionOptions {
  /**
   * The URL of the ClickHouse server, including the protocol (http or https).
   */
  url: string

  /**
   * The username used to authenticate with the ClickHouse server.
   */
  username: string

  /**
   * The password associated with the provided username.
   */
  password: string

  /**
   * The name of the database to connect to.
   */
  database: string

  /**
   * Optional settings to customize the ClickHouse client behavior.
   * See ClickHouseSettings for more details.
   */
  settings?: ClickHouseSettings

  /**
   * Optional configuration for keeping the connection alive.
   */
  keepAlive?: {
    /**
     * Enables or disables keep-alive functionality.
     */
    enabled?: boolean

    /**
     * Optional time in seconds for the idle socket's Time-To-Live (TTL).
     */
    idle_socket_ttl?: number
  } & {
    /**
     * Repeated definition of enabled flag for compatibility reasons.
     * Enables or disables keep-alive functionality.
     */
    enabled?: boolean
  }

  /**
   * Optional list of schemas (as classes) to load upon connection.
   * This can be used to register schemas or metadata relevant to the connection.
   */
  // eslint-disable-next-line @typescript-eslint/ban-types
  schemas?: Array<Function | string>

  /**
   * Optional flag to indicate whether to synchronize the database schema
   * with the defined schemas upon connection initialization.
   */
  synchronize?: boolean

  /**
   * Optional flag to enable or disable logging of SQL queries executed through the connection.
   */
  logging?: boolean
}
