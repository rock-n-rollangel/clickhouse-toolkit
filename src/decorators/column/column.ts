import { getMetadataArgsStorage } from '../../globals'
import { ColumnOptions } from '../../common/column-options'

/**
 * A decorator function for defining a column in the database.
 * It stores metadata for each column, including type, name, and other options.
 *
 * @param options - The options for configuring the column, such as type, uniqueness, and whether it's nullable.
 * @returns A property decorator that registers the column in the metadata storage.
 */
export function Column(options: ColumnOptions): PropertyDecorator {
  return function (object: object, propertyName: string) {
    // Pushes the column metadata into the global storage for future use.
    getMetadataArgsStorage().columns.push({
      target: object.constructor, // The target class to which the column belongs.
      propertyName: propertyName, // The name of the property in the class.
      options: {
        type: options.type, // The data type of the column.
        name: options?.name ?? propertyName, // Uses the specified name or defaults to the property name.
        nullable: options?.nullable, // Indicates if the column can be null.
        unique: options?.unique, // Indicates if the column must be unique.
        primary: options?.primary, // Marks the column as a primary key if true.
        orderBy: options?.orderBy, // Specifies if the column should be used for sorting.
        function: options?.function, // Any function (e.g., aggregate) to be applied to the column.
      },
    })
  }
}
