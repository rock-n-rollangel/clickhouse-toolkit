import { MetadataArgsStorage } from './metadata/metadata-args-storage'

export function getMetadataArgsStorage(): MetadataArgsStorage {
  if (!global.metadataStorage) global.metadataStorage = new MetadataArgsStorage()
  return global.metadataStorage
}
