import { ClickHouseToolkitError } from './clickhouse-toolkit-error'

export class SchemaMetadataNotFoundError extends ClickHouseToolkitError {
  constructor(target: string) {
    super(`Metadata for name -- ${target} -- not found`)
  }
}
