import { ClickHouseToolkitError } from './clickhouse-toolkit-error'

export class WrongParameterColumnNameError extends ClickHouseToolkitError {
  constructor(columnName: string) {
    super(`Wrong column name -- ${columnName} -- provided in parameter`)
  }
}
