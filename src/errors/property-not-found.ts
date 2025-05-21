import { ClickHouseToolkitError } from './clickhouse-toolkit-error'

export class PropertyNotFoundError extends ClickHouseToolkitError {
  constructor(property: string, target: string) {
    super(
      `Property for column -- ${property} -- not found in the target -- ${target}. Cannot map the result to the target.`,
    )
  }
}
