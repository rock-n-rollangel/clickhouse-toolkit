import { Connection } from '../../../src/connection/connection'
import { ConnectionOptions } from '../../things/connection-options'

describe('Migrator (intergrational)', () => {
  let connection: Connection

  beforeAll(async () => {
    connection = await Connection.initialize({
      ...ConnectionOptions,
      migrations: ['test/migrations/*'],
    })
  })

  it('should migrate up', async () => {
    await connection.migrator.up()
    const tables = await connection.queryRunner.query<{ name: string }>('SHOW TABLES')
    expect(tables.find(({ name }) => name === 'users')).toBeDefined()
  })

  it('should migrate down', async () => {
    await connection.migrator.down()
    const tables = await connection.queryRunner.query<{ name: string }>('SHOW TABLES')
    expect(tables.find(({ name }) => name === 'users')).not.toBeDefined()
  })

  it('should show status', async () => {
    expect(await connection.migrator.status()).not.toBeDefined()
  })
})
