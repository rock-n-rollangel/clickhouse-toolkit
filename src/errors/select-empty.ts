import { ClickHouseToolkitError } from './clickhouse-toolkit-error'

export class SelectStatementError extends ClickHouseToolkitError {
  constructor() {
    super('Select statement should not be empty')
  }
}
