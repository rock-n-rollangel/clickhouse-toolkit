import { ConnectionOptions as Opts } from '../../src'

export const ConnectionOptions: Opts = {
  username: 'user',
  password: 'password',
  url: 'http://localhost:8123',
  database: 'test_db',
  settings: {
    mutations_sync: '2',
  },
}
