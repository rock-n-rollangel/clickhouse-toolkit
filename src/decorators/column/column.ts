import { getMetadataArgsStorage } from '@/globals'
import { ColumnOptions } from './options/column-options'

export function Column(options: ColumnOptions): PropertyDecorator {
  return function (object: object, propertyName: string) {
    getMetadataArgsStorage().columns.push({
      target: object.constructor,
      propertyName: propertyName,
      options: {
        type: options.type,
        name: options?.name ?? propertyName,
        nullable: options?.nullable,
        unique: options?.unique,
        primary: options?.primary,
        orderBy: options?.orderBy,
      },
    })
  }
}
