import { ClickHouseToolkitError } from './clickhouse-toolkit-error'

export class UpdateValueNotSetError extends ClickHouseToolkitError {
  constructor() {
    super('Update values did not provided')
  }
}
