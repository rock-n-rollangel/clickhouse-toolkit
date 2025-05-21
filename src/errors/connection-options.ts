import { ClickHouseToolkitError } from './clickhouse-toolkit-error'

export class ConnectionOptionsError extends ClickHouseToolkitError {
  constructor() {
    super('required options was not provided')
  }
}
