export class SelectStatementError extends Error {
  constructor() {
    super('Select statement should not be empty')
  }
}
