import { QueryRunner } from 'src/query-runner/query-runner'

export interface MigrationModule {
  name: string
  up: (queryRunner: QueryRunner) => Promise<void>
  down?: (queryRunner: QueryRunner) => Promise<void>
}
