import { Schema } from '@/decorators/schema/schema'
import { getMetadataArgsStorage } from '@/globals'
import { SchemaOptions } from '@/decorators/schema/options/schema-options'
import { MaterializedViewSchemaOptions } from '@/decorators/schema/options/materialized-options'

const tables = []
jest.mock('@/globals', () => ({
  getMetadataArgsStorage: jest.fn(() => ({
    tables: tables,
  })),
}))

describe('Schema decorator', () => {
  const mockMetadataArgsStorage = getMetadataArgsStorage()

  beforeEach(() => {
    tables.length = 0
    jest.clearAllMocks()
  })

  it('should parse class to snakecase correct', () => {
    @Schema()
    class MultiWordClassName {}

    expect(mockMetadataArgsStorage.tables).toHaveLength(1)
    expect(mockMetadataArgsStorage.tables[0]).toEqual({
      target: MultiWordClassName,
      engine: null,
      name: 'multi_word_class_name', // inferred name from class
      materialized: null,
      materializedTo: null,
      materializedQuery: null,
    })
  })

  it('should register an Schema with default options', () => {
    @Schema()
    class TestSchema {}

    expect(mockMetadataArgsStorage.tables).toHaveLength(1)
    expect(mockMetadataArgsStorage.tables[0]).toEqual({
      target: TestSchema,
      engine: null,
      name: 'test_schema', // inferred name from class
      materialized: null,
      materializedTo: null,
      materializedQuery: null,
    })
  })

  it('should register an Schema with custom options', () => {
    const options: SchemaOptions = { name: 'CustomSchemaName', engine: null }

    @Schema(options)
    class TestSchema {}

    expect(mockMetadataArgsStorage.tables).toHaveLength(1)
    expect(mockMetadataArgsStorage.tables[0]).toEqual({
      target: TestSchema,
      engine: null,
      name: 'CustomSchemaName',
      materialized: null,
      materializedTo: null,
      materializedQuery: null,
    })
  })

  it('should register a materialized view Schema with options', () => {
    const mvOptions: MaterializedViewSchemaOptions = {
      materialized: true,
      materializedTo: 'AnotherTable',
      materializedQuery: 'SELECT * FROM source_table',
    }

    @Schema(mvOptions)
    class TestMaterializedView {}

    expect(mockMetadataArgsStorage.tables).toHaveLength(1)
    expect(mockMetadataArgsStorage.tables[0]).toEqual({
      target: TestMaterializedView,
      engine: null,
      name: 'test_materialized_view', // inferred name
      materialized: true,
      materializedTo: 'AnotherTable',
      materializedQuery: 'SELECT * FROM source_table',
    })
  })
})
