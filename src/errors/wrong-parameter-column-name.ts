export class WrongParameterColumnNameError extends Error {
  constructor(columnName: string) {
    super(`Wrong column name -- ${columnName} -- provided in parameter`)
  }
}
