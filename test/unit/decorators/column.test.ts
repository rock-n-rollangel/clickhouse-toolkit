import { getMetadataArgsStorage } from '../../../src/globals'
import { Column } from '../../../src/decorators/column/column'

const columns = []
jest.mock('../../../src/globals', () => ({
  getMetadataArgsStorage: jest.fn(() => ({
    columns: columns,
  })),
}))

describe('Column decorator', () => {
  const mockMetadataArgsStorage = getMetadataArgsStorage()

  beforeEach(() => {
    columns.length = 0
    jest.clearAllMocks()
  })

  it('should add string column', () => {
    class e {
      @Column({ type: 'String' })
      blegh = 1
    }

    expect(mockMetadataArgsStorage.columns).toHaveLength(1)
    expect(mockMetadataArgsStorage.columns[0]).toEqual({
      target: e,
      propertyName: 'blegh',
      options: {
        type: 'String',
        name: 'blegh',
      },
    })
  })

  it('should add full column', () => {
    class e {
      @Column({ type: 'String', name: 'e_column_1', unique: true, orderBy: true, primary: true, nullable: true })
      blegh: string
    }

    expect(mockMetadataArgsStorage.columns).toHaveLength(1)
    expect(mockMetadataArgsStorage.columns[0]).toEqual({
      target: e,
      propertyName: 'blegh',
      options: {
        type: 'String',
        name: 'e_column_1',
        unique: true,
        primary: true,
        orderBy: true,
        nullable: true,
      },
    })
  })
})
