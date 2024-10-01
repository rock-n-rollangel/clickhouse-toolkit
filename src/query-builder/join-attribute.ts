import { SchemaMetadata } from '../metadata/schema-metadata'

export class JoinAttribute {
  readonly '@instanceof' = Symbol.for('JoinAttribute')

  constructor(
    public joinTable: SchemaMetadata | string,
    public direction: 'LEFT' | 'INNER',
    public alias: string,
    public condition: string,
  ) {}
}
