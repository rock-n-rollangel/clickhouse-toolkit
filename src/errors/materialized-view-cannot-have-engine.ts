import { ClickHouseToolkitError } from './clickhouse-toolkit-error'

export class MaterializedViewCannotHaveEngineError extends ClickHouseToolkitError {
  constructor() {
    super('Materialized view cannot have engine')
  }
}
