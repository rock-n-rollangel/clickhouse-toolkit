/**
 * E2E Test Setup
 * Provides utilities for setting up and managing ClickHouse test instances
 */

import { exec } from 'child_process'
import { promisify } from 'util'
import { QueryRunner } from '../../../src/runner/query-runner'
import { Logger, NoOpLogger } from '../../../src/core/logger'

const execAsync = promisify(exec)

export interface TestConfig {
  url: string
  username: string
  password: string
  database: string
  containerName: string
  useExternalClickHouse?: boolean // Skip Docker container management
  logger?: Logger
}

export const DEFAULT_TEST_CONFIG: TestConfig = {
  url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
  username: process.env.CLICKHOUSE_USER || 'user',
  password: process.env.CLICKHOUSE_PASSWORD || 'password',
  database: process.env.CLICKHOUSE_DATABASE || 'test_db',
  containerName: 'migrations',
  useExternalClickHouse: process.env.USE_EXTERNAL_CLICKHOUSE === 'true',
  logger: process.env.USE_NOOP_LOGGER === 'true' ? new NoOpLogger() : undefined,
}

export class ClickHouseTestSetup {
  private config: TestConfig
  private queryRunner: QueryRunner | null = null

  constructor(config: TestConfig = DEFAULT_TEST_CONFIG) {
    this.config = config
  }

  /**
   * Start ClickHouse container for testing
   */
  async startClickHouse(): Promise<void> {
    if (this.config.useExternalClickHouse) {
      // Just verify connection to external instance
      await this.waitForClickHouse(10)
      return
    }

    try {
      // Stop any existing container
      await this.stopClickHouse()

      // Start new container
      const dockerRunCmd = `docker run -d --name ${this.config.containerName} -e CLICKHOUSE_USER=${this.config.username} -e CLICKHOUSE_PASSWORD=${this.config.password} -e CLICKHOUSE_DB=${this.config.database} --ulimit nofile=262144:262144 -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server:latest`

      await execAsync(dockerRunCmd)

      // Wait for ClickHouse to be ready
      await this.waitForClickHouse()
    } catch (error) {
      throw new Error(`Failed to start ClickHouse container: ${error}`)
    }
  }

  /**
   * Stop ClickHouse container
   */
  async stopClickHouse(): Promise<void> {
    if (this.config.useExternalClickHouse) {
      return
    }

    try {
      await execAsync(`docker stop ${this.config.containerName}`)
      await execAsync(`docker rm ${this.config.containerName}`)
    } catch (error) {
      // Container might not exist, which is fine
    }
  }

  /**
   * Wait for ClickHouse to be ready
   */
  private async waitForClickHouse(maxRetries: number = 30): Promise<void> {
    for (let i = 0; i < maxRetries; i++) {
      try {
        const runner = new QueryRunner(this.config)
        const { stdout } = await execAsync(`curl localhost:8123`)
        const isReady = stdout.includes('Ok.')
        await runner.close()

        if (isReady) {
          return
        }
      } catch (error) {
        // Not ready yet
      }

      await new Promise((resolve) => setTimeout(resolve, 1000))
    }

    throw new Error('ClickHouse failed to start within timeout')
  }

  /**
   * Get a QueryRunner instance for testing
   */
  getQueryRunner(): QueryRunner {
    if (!this.queryRunner) {
      this.queryRunner = new QueryRunner(this.config)
    }
    return this.queryRunner
  }

  /**
   * Close the QueryRunner connection
   */
  async closeQueryRunner(): Promise<void> {
    if (this.queryRunner) {
      await this.queryRunner.close()
      this.queryRunner = null
    }
  }

  /**
   * Create test database and tables
   */
  async setupTestData(): Promise<void> {
    const runner = this.getQueryRunner()

    // Create test database
    await runner.command({
      sql: `CREATE DATABASE IF NOT EXISTS ${this.config.database}`,
    })

    // Create test tables
    await runner.command({
      sql: `
        CREATE TABLE IF NOT EXISTS ${this.config.database}.users (
          id UInt32,
          name String,
          email String,
          age UInt8,
          country String,
          status String DEFAULT 'active',
          created_at DateTime DEFAULT now(),
          updated_at DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree()
        ORDER BY id
      `,
    })

    await runner.command({
      sql: `
        CREATE TABLE IF NOT EXISTS ${this.config.database}.orders (
          id UInt32,
          user_id UInt32,
          product_name String,
          amount Decimal(10, 2),
          status String DEFAULT 'pending',
          created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY id
      `,
    })

    await runner.command({
      sql: `
        CREATE TABLE IF NOT EXISTS ${this.config.database}.products (
          id UInt32,
          name String,
          price Decimal(10, 2),
          category String,
          stock_quantity UInt32,
          created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY id
      `,
    })

    // Insert test data
    await runner.command({
      sql: `
        INSERT INTO ${this.config.database}.users VALUES
        (1, 'John Doe', 'john@example.com', 30, 'US', 'active', now(), now()),
        (2, 'Jane Smith', 'jane@example.com', 25, 'CA', 'active', now(), now()),
        (3, 'Bob Johnson', 'bob@example.com', 35, 'UK', 'inactive', now(), now()),
        (4, 'Alice Brown', 'alice@example.com', 28, 'US', 'active', now(), now()),
        (5, 'Charlie Wilson', 'charlie@example.com', 42, 'AU', 'active', now(), now())
      `,
    })

    await runner.command({
      sql: `
        INSERT INTO ${this.config.database}.products VALUES
        (1, 'Laptop', 999.99, 'Electronics', 50, now()),
        (2, 'Mouse', 29.99, 'Electronics', 200, now()),
        (3, 'Keyboard', 79.99, 'Electronics', 150, now()),
        (4, 'Book', 19.99, 'Education', 100, now()),
        (5, 'Pen', 2.99, 'Office', 500, now())
      `,
    })

    await runner.command({
      sql: `
        INSERT INTO ${this.config.database}.orders VALUES
        (1, 1, 'Laptop', 999.99, 'completed', now()),
        (2, 1, 'Mouse', 29.99, 'completed', now()),
        (3, 2, 'Keyboard', 79.99, 'pending', now()),
        (4, 3, 'Book', 19.99, 'completed', now()),
        (5, 4, 'Pen', 2.99, 'pending', now()),
        (6, 5, 'Laptop', 999.99, 'completed', now())
      `,
    })
  }

  /**
   * Clean up test data
   */
  async cleanupTestData(): Promise<void> {
    const runner = this.getQueryRunner()

    try {
      await runner.command({
        sql: `DROP TABLE IF EXISTS ${this.config.database}.orders`,
      })
      await runner.command({
        sql: `DROP TABLE IF EXISTS ${this.config.database}.products`,
      })
      await runner.command({
        sql: `DROP TABLE IF EXISTS ${this.config.database}.users`,
      })
    } catch {}
  }

  /**
   * Full setup: start container, create test data
   */
  async setup(): Promise<void> {
    await this.startClickHouse()
    await this.setupTestData()
  }

  /**
   * Full teardown: cleanup data, stop container
   */
  async teardown(): Promise<void> {
    if (this.config.useExternalClickHouse) {
      // When using external ClickHouse, optionally keep data for inspection
      const keepData = process.env.KEEP_TEST_DATA === 'true'
      if (!keepData) {
        await this.cleanupTestData()
      }
    } else {
      await this.cleanupTestData()
    }

    await this.closeQueryRunner()
    await this.stopClickHouse()
  }
}

/**
 * Global test setup instance
 */
export const testSetup = new ClickHouseTestSetup()
