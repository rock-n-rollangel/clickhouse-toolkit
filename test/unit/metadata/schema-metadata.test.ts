import { SchemaMetadata } from '../../../src/metadata/schema-metadata'
import { Connection } from '../../../src/connection/connection'
import { ColumnMetadataArgs } from '../../../src/metadata/column-metadata-args'

describe('EntityMetadata', () => {
  let mockConnection: Connection
  let entityMetadata: SchemaMetadata

  beforeEach(() => {
    mockConnection = {} as Connection
    entityMetadata = new SchemaMetadata(mockConnection)
  })

  it('should initialize with the provided connection', () => {
    expect(entityMetadata.connection).toBe(mockConnection)
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

    entityMetadata.columnMetadataArgs = mockColumnMetadataArgs

    const result = entityMetadata.getColumnMetadatas()

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
    entityMetadata.columnMetadataArgs = []

    const result = entityMetadata.getColumnMetadatas()

    expect(result).toEqual([]) // Expect an empty array when there are no columns
  })
})
