import { ColumnType } from '../../../types/data-types'

/**
 * Class representing the options for a database column.
 * It allows specifying the data type of the column along with various options
 * like uniqueness, primary key, nullable, sorting, and more.
 */
export class ColumnOptions {
  /**
   * The data type of the column. This field is required.
   */
  type: ColumnType

  /**
   * The name of the column. Optional.
   */
  name?: string

  /**
   * Specifies if the column can be null. Optional, defaults to false.
   */
  nullable?: boolean

  /**
   * Specifies if the column must have unique values. Optional, defaults to false.
   */
  unique?: boolean

  /**
   * Specifies if the column is a primary key. Optional, defaults to false.
   */
  primary?: boolean

  /**
   * Indicates if the column should be used for sorting (ORDER BY). Optional.
   */
  orderBy?: boolean

  /**
   * A function to be applied to the column (e.g., aggregate function). Optional.
   */
  function?: string
}
