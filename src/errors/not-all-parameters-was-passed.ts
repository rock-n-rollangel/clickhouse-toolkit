import { ClickHouseToolkitError } from './clickhouse-toolkit-error'

export class NotAllParametersWasPassedError extends ClickHouseToolkitError {
  constructor() {
    super(`not enough parameters to bind`)
  }
}
