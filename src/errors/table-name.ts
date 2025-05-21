import { ClickHouseToolkitError } from './clickhouse-toolkit-error'

export class TableNameError extends ClickHouseToolkitError {
  constructor() {
    super('Table name not provided')
  }
}
