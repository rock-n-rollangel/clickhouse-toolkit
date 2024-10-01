import { Engine } from '@/types/engine'
import { SchemaOptionsBase } from './schema-options-base'

export interface SchemaOptions extends SchemaOptionsBase {
  engine: Engine
}
