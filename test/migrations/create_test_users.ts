import { QueryRunner } from '../../src/query-runner/query-runner'
import { Table } from '../../src/schema-builder/table'

export function up(queryRunner: QueryRunner) {
  queryRunner.createTable(
    new Table({
      name: 'users',
      columns: [{ name: 'id', type: 'UInt32', nullable: false, unique: true, primary: true }],
      engine: 'MergeTree',
    }),
    true,
  )
}

export function down(queryRunner: QueryRunner) {
  queryRunner.dropTable('users', true)
}
