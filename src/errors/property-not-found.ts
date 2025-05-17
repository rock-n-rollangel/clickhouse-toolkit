export class PropertyNotFoundError extends Error {
  constructor(property: string, target: string) {
    super(
      `Property for column -- ${property} -- not found in the target -- ${target}. Cannot map the result to the target.`,
    )
  }
}
