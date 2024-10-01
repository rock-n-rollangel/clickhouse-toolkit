import { ColumnOptions } from '@/decorators/column/options/column-options'

export class ColumnMetadataArgs {
  // eslint-disable-next-line @typescript-eslint/ban-types
  readonly target: Function | string

  readonly propertyName: string

  readonly options: ColumnOptions
}
