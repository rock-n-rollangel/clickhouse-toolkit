import { SchemaMetadata } from '../../../src/metadata/schema-metadata'
import { Connection } from '../../../src/connection/connection'
import { ColumnMetadataArgs } from '../../../src/metadata/column-metadata-args'

describe('SchemaMetadata', () => {
  let mockConnection: Connection
  let schemaMetadata: SchemaMetadata

  beforeEach(() => {
    mockConnection = {} as Connection
    schemaMetadata = new SchemaMetadata(mockConnection)
  })

  it('should initialize with the provided connection', () => {
    expect(schemaMetadata.connection).toBe(mockConnection)
  })

  it('should return column metadatas correctly from columnMetadataArgs', () => {
    const mockColumnMetadataArgs: ColumnMetadataArgs[] = [
      {
        target: 'blah',
        propertyName: 'property_1',
        options: {
          name: 'id',
          type: 'UUID',
          nullable: false,
          unique: true,
          primary: true,
          orderBy: true,
        },
      },
      {
        target: 'blah',
        propertyName: 'property_2',
        options: {
          name: 'name',
          type: 'String',
          nullable: true,
          unique: false,
          primary: false,
          orderBy: undefined,
        },
      },
    ]

    schemaMetadata.columnMetadataArgs = mockColumnMetadataArgs

    const result = schemaMetadata.getColumnMetadatas()

    expect(result).toEqual([
      {
        name: 'id',
        type: 'UUID',
        nullable: false,
        unique: true,
        primary: true,
        orderBy: true,
      },
      {
        name: 'name',
        type: 'String',
        nullable: true,
        unique: false,
        primary: false,
        orderBy: undefined,
      },
    ])
  })

  it('should handle empty columnMetadataArgs correctly', () => {
    schemaMetadata.columnMetadataArgs = []

    const result = schemaMetadata.getColumnMetadatas()

    expect(result).toEqual([]) // Expect an empty array when there are no columns
  })
})
