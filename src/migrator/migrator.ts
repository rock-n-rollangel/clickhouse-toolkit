import { Connection } from '../connection/connection'
import { importMigrationFunctionsFromDirectories } from '../util/directory-loader'

export class Migrator {
  constructor(
    private readonly connection: Connection,
    private readonly migrationsTableName: string,
    private readonly migrations: string[],
  ) {}

  /**
   * Initializes the migrations table.
   */
  async init(): Promise<void> {
    await this.connection.command(`CREATE TABLE IF NOT EXISTS ${this.migrationsTableName} (
      name String,
      created_at DateTime DEFAULT now()
    )
    ENGINE = MergeTree()
    ORDER BY ( toYYYYMMDD(created_at), name )`)
  }

  async up(): Promise<void> {
    const all = await importMigrationFunctionsFromDirectories(this.migrations)
    const applied = await this.fetchAppliedNames()

    for (const m of all) {
      if (applied.has(m.name)) continue

      await m.up(this.connection.queryRunner)
      await this.connection.command(`INSERT INTO ${this.migrationsTableName} (name) VALUES ('${m.name}')`)
    }
  }

  async down(step: number = 1): Promise<void> {
    const applied = [...(await this.fetchApplied())]
    const toRollback = applied.slice(-step)

    if (toRollback.length === 0) {
      return
    }

    const all = await importMigrationFunctionsFromDirectories(this.migrations)
    const byName = new Map(all.map((m) => [m.name, m]))

    for (const m of toRollback.reverse()) {
      const mod = byName.get(m.name)
      if (!mod?.down) {
        continue
      }

      await mod.down(this.connection.queryRunner)
      await this.connection.command(`DELETE FROM ${this.migrationsTableName} WHERE name = '${m.name}'`)
    }
  }

  async status(): Promise<void> {
    const all = await importMigrationFunctionsFromDirectories(this.migrations)
    const applied = await this.fetchAppliedNames()

    for (const m of all) {
      console.log(m.name, applied.has(m.name) ? 'applied' : 'not applied')
    }
  }

  private async fetchAppliedNames(): Promise<Set<string>> {
    const result = await this.connection.queryRunner.query(
      `SELECT name FROM ${this.migrationsTableName} ORDER BY created_at`,
    )
    const rows = result as { name: string }[]
    return new Set(rows.map((r) => r.name))
  }

  private async fetchApplied(): Promise<{ name: string; created_at: string }[]> {
    const result = await this.connection.queryRunner.query(
      `SELECT name, created_at FROM ${this.migrationsTableName} ORDER BY created_at`,
    )
    return result as { name: string; created_at: string }[]
  }
}
