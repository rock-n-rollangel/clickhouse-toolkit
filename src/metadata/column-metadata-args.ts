import { ColumnOptions } from '../common/column-options'

/**
 * Represents metadata for a database column.
 * This class holds information about the target schema, the column's property name, and the column's configuration options.
 */
export class ColumnMetadataArgs {
  // The target schema (class) or a string representing the table name.
  // eslint-disable-next-line @typescript-eslint/ban-types
  readonly target: Function | string

  // The property name in the target schema to be mapped as a column.
  readonly propertyName: string

  // The options for the column (e.g., type, nullability, uniqueness).
  readonly options: ColumnOptions
}
