export class NotAllParametersWasPassedError extends Error {
  constructor() {
    super(`not enough parameters to bind`)
  }
}
