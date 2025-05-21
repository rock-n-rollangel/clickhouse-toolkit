import { ClickHouseToolkitError } from './clickhouse-toolkit-error'

export class MaterializedViewCannotHaveColumnsError extends ClickHouseToolkitError {
  constructor() {
    super('Materialized view cannot have columns')
  }
}
