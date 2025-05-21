import * as glob from 'glob'
import { pathResolve, pathExtname, pathNormalize } from './platform-tools'
import { MigrationModule } from '../types/migration-module'

export async function importMigrationFunctionsFromDirectories(
  directories: string[],
  formats = ['.ts', '.js'],
): Promise<MigrationModule[]> {
  const files = directories.flatMap((dir) => glob.sync(pathNormalize(dir)))

  const migrations: MigrationModule[] = []

  for (const file of files) {
    if (!formats.includes(pathExtname(file)) || file.endsWith('.d.ts')) continue

    const module = await import(pathResolve(file))

    const up = module.up
    if (typeof up !== 'function') continue

    migrations.push({ name: file, up, down: module.down })
  }

  return migrations
}
