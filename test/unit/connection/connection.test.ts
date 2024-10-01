import { createClient } from '@clickhouse/client'
import { Connection } from '../../../src/connection/connection'
import { QueryRunner } from '../../../src/query-runner/query-runner'
import { SelectQueryBuilder } from '../../../src/query-builder/select-query-builder'
import { ConnectionOptions } from '../../../src/types/connection-options'
import { SchemaBuilder } from '../../../src/schema-builder/schema-builder'
import { registerQueryBuilders } from '../../../src/util/register-query-builders'
import { ConnectionOptionsError } from '../../../src/errors/connection-options'

jest.mock('@clickhouse/client', () => {
  return {
    createClient: jest.fn().mockReturnValue({
      query: jest.fn().mockReturnValue(Promise),
      command: jest.fn().mockReturnValue(Promise),
      insert: jest.fn().mockReturnValue(Promise),
    }),
  }
})
jest.mock('../../../src/query-runner/query-runner')
jest.mock('../../../src/query-builder/select-query-builder')
jest.mock('../../../src/schema-builder/schema-builder')
jest.mock('../../../src/util/register-query-builders.ts')

describe('Connection', () => {
  let connectionOptions: ConnectionOptions

  beforeEach(() => {
    connectionOptions = {
      url: 'http://localhost:8123',
      username: 'default',
      password: 'password',
      database: 'test_db',
      settings: {},
      keepAlive: {
        enabled: true,
      },
      synchronize: false,
    }
  })

  it('should initialize a connection', async () => {
    const connection = await Connection.initialize(connectionOptions)

    expect(connection).toBeInstanceOf(Connection)
    expect(connection.database).toBe('test_db')
    expect(createClient).toHaveBeenCalledWith({
      url: connectionOptions.url,
      username: connectionOptions.username,
      password: connectionOptions.password,
      database: connectionOptions.database,
      clickhouse_settings: connectionOptions.settings,
      keep_alive: connectionOptions.keepAlive,
    })
    expect(registerQueryBuilders).toHaveBeenCalled()
    expect(connection['database']).toEqual(connectionOptions.database)
  })

  it('should create a query runner', async () => {
    const connection = await Connection.initialize(connectionOptions)
    const queryRunner = connection.queryRunner

    expect(queryRunner).toBeInstanceOf(QueryRunner)
    expect(queryRunner).toBe(connection.queryRunner) // Ensure the queryRunner is set on the connection
  })

  it('should create a query builder', async () => {
    const connection = await Connection.initialize(connectionOptions)
    const queryBuilder = connection.createQueryBuilder()

    expect(queryBuilder).toBeInstanceOf(SelectQueryBuilder)
    expect(queryBuilder['constructor']).toHaveBeenCalledWith(connection, connection.queryRunner)
  })

  it('should create a schema builder', async () => {
    const connection = await Connection.initialize(connectionOptions)
    const schemaBuilder = connection.createSchemaBuilder()

    expect(schemaBuilder).toBeInstanceOf(SchemaBuilder)
    expect(schemaBuilder['constructor']).toHaveBeenCalledWith(connection)
  })

  it('should execute a command', async () => {
    const connection = await Connection.initialize(connectionOptions)

    const result = await connection.command('CREATE TABLE test {id UInt32}')
    expect(result).toBeUndefined()
    expect(connection['client'].command).toHaveBeenCalledWith({
      query: 'CREATE TABLE test {id UInt32}',
      query_params: undefined,
    })
  })

  it('should execute a query', async () => {
    const connection = await Connection.initialize(connectionOptions)
    const mockQuery = jest.fn().mockResolvedValueOnce({
      json: jest.fn().mockResolvedValueOnce({ data: [{ id: 1 }] }),
    })
    connection['client'].query = mockQuery

    const result = await connection.query('SELECT * FROM test', {})
    expect(mockQuery).toHaveBeenCalledWith({
      query: 'SELECT * FROM test',
      query_params: {},
    })
    expect(result).toStrictEqual([{ id: 1 }])
  })

  it('should insert values into the table', async () => {
    const connection = await Connection.initialize(connectionOptions)

    await connection.insert([{ id: 1 }], 'test_table')
    expect(connection['client'].insert).toHaveBeenCalledWith({
      table: 'test_table',
      values: [{ id: 1 }],
      format: 'JSONEachRow',
    })
  })

  it('should run synchronize', async () => {
    const createSchemaBuilderSpy = jest.spyOn(Connection.prototype, 'createSchemaBuilder')
    const synchronizeSpy = jest.spyOn(SchemaBuilder.prototype, 'synchronize')
    await Connection.initialize({ ...connectionOptions, synchronize: true })

    expect(createSchemaBuilderSpy).toHaveBeenCalled()
    expect(synchronizeSpy).toHaveBeenCalled()
  })

  it('should fail coz of required options not provided', async () => {
    try {
      await Connection.initialize({
        url: '',
        username: '',
        password: '',
        database: '',
      })
    } catch (e) {
      expect(e).toBeInstanceOf(ConnectionOptionsError)
    }
  })
})
