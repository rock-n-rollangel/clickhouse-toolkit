export class ConnectionOptionsError extends Error {
  constructor() {
    super('required options was not provided')
  }
}
