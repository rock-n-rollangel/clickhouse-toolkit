import { ClickHouseSettings } from '@clickhouse/client'

export interface ConnectionOptions {
  url: string
  username: string
  password: string
  database: string
  settings?: ClickHouseSettings
  keepAlive?: {
    enabled?: boolean
  } & {
    enabled?: boolean
    idle_socket_ttl?: number
  }
  // eslint-disable-next-line @typescript-eslint/ban-types
  entities?: Array<Function | string>
  synchronize?: boolean
  logging?: boolean
}
