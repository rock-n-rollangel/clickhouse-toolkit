import { importMigrationFunctionsFromDirectories } from '../../../src/util/directory-loader'

describe('importMigrationFunctionsFromDirectories', () => {
  it('should load migrations from a directory', async () => {
    const migrations = await importMigrationFunctionsFromDirectories(['./test/migrations/*'])
    expect(migrations).toHaveLength(1)
    expect(migrations[0].name).toBe('test/migrations/create_test_users.ts')
    expect(migrations[0].up).toBeDefined()
    expect(migrations[0].down).toBeDefined()
  })
})
