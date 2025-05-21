import { QueryRunner } from '../../../src/query-runner/query-runner'
import { Connection } from '../../../src/connection/connection'
import { SchemaMetadata } from '../../../src/metadata/schema-metadata'
import { Table } from '../../../src/schema-builder/table'
import { ColumnMetadata } from '../../../src/common/column-metadata'
import { Engine } from '../../../src/types/engine'

describe('QueryRunner', () => {
  let mockConnection: jest.Mocked<Connection>
  let queryRunner: QueryRunner

  beforeEach(() => {
    mockConnection = {
      logging: true,
      query: jest.fn(),
      command: jest.fn(),
      insert: jest.fn(),
      escape: jest.fn().mockImplementation((name) => `\`${name}\``),
      getFullTablePath: jest.fn().mockImplementation((name) => `\`${name}\``),
      createQueryBuilder: jest.fn().mockReturnValue({ toSql: jest.fn().mockReturnValue('SELECT 1') }),
    } as unknown as jest.Mocked<Connection>

    queryRunner = new QueryRunner(mockConnection)
  })

  it('should initialize with connection and metadata', () => {
    const metadata = new SchemaMetadata(mockConnection)
    queryRunner.setMetadata(metadata)
    expect(queryRunner['metadata']).toBe(metadata)
    expect(queryRunner['connection']).toBe(mockConnection)
  })

  it('should create a table and call command method', async () => {
    const table: Table = new Table({
      name: 'test_table',
      columns: [{ name: 'id', type: 'UInt32', nullable: false, unique: true, primary: true }] as ColumnMetadata[],
      engine: 'MergeTree' as Engine,
    })

    await queryRunner.createTable(table, true)
    expect(mockConnection.command).toHaveBeenCalledWith(
      "CREATE TABLE IF NOT EXISTS `test_table` ( `id` UInt32 COMMENT 'UNIQUE FIELD' ) ENGINE MergeTree PRIMARY KEY (`id`)",
      undefined,
    )
  })

  it('should drop a table and call command method', async () => {
    const tableName = 'test_table'
    await queryRunner.dropTable(tableName, true)
    expect(mockConnection.command).toHaveBeenCalledWith('DROP TABLE IF EXISTS `test_table`', undefined)
  })

  it('should rename a table and call command method', async () => {
    await queryRunner.renameTable('old_table', 'new_table')
    expect(mockConnection.command).toHaveBeenCalledWith('RENAME `old_table` TO `new_table`', undefined)
  })

  it('should execute a query and return result', async () => {
    const mockResult = [{ id: 1 }]
    mockConnection.query.mockResolvedValue(mockResult)

    const result = await queryRunner.query('SELECT * FROM test')
    expect(mockConnection.query).toHaveBeenCalledWith('SELECT * FROM test', undefined)
    expect(result).toBe(mockResult)
  })

  it('should execute a command and log the SQL', async () => {
    const sql = 'CREATE TABLE test (id UInt32)'
    const logSpy = jest.spyOn(console, 'log').mockImplementation()

    await queryRunner.command(sql)
    expect(mockConnection.command).toHaveBeenCalledWith(sql, undefined)
    expect(logSpy).toHaveBeenCalledWith(`QUERY: ${sql}`)
  })

  it('should insert values into table', async () => {
    const values = [{ id: 1 }]
    await queryRunner.insert(values, 'test_table')
    expect(mockConnection.insert).toHaveBeenCalledWith(values, 'test_table')
  })
})
