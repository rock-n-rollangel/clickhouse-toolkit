export class UpdateValueNotSetError extends Error {
  constructor() {
    super('Update values did not provided')
  }
}
