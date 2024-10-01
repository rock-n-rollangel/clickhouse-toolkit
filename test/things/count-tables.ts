import { Connection } from '../../src/connection/connection'

export async function countTables(connection: Connection, tableName: string): Promise<number> {
  const result = await connection.query<{ count: string }>(`
      SElECT COUNT(*) AS count FROM system.tables WHERE database = '${connection.database}' AND name = '${tableName}' 
    `)

  return +result[0].count
}
