export class SchemaMetadataNotFoundError extends Error {
  constructor(target: string) {
    super(`Metadata for name -- ${target} -- not found`)
  }
}
