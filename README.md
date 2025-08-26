# ClickHouse Toolkit

A comprehensive TypeScript ORM and toolkit for [ClickHouse](https://clickhouse.com/) databases. This library provides decorator-based schema definitions, automatic schema synchronization, migrations, and a fluent query builder API.

## Features

- **Decorator-based Schema Definition:** Define tables and columns using TypeScript decorators
- **Automatic Schema Synchronization:** Keep your database schema in sync with your TypeScript models
- **Migration System:** Version-controlled database schema changes
- **Fluent Query Builder:** Chain methods for building complex SQL queries
- **Materialized Views:** Support for ClickHouse materialized views
- **CLI Tool:** Command-line interface for running migrations
- **Type Safety:** Full TypeScript support with type inference

## Installation

Install the package via npm:

```bash
npm install clickhouse-toolkit
```

## Quick Start

### 1. Define Your Schema

```typescript
import { Schema } from 'clickhouse-toolkit'
import { Column } from 'clickhouse-toolkit'

@Schema({ engine: 'MergeTree' })
export class User {
  @Column({ type: 'UUID', primary: true })
  id: string

  @Column({ type: 'String' })
  name: string

  @Column({ type: 'DateTime', orderBy: true })
  createdAt: string

  @Column({ type: 'UInt32' })
  age: number
}
```

### 2. Initialize Connection

```typescript
import { Connection } from 'clickhouse-toolkit'

const connection = await Connection.initialize({
  url: 'http://localhost:8123',
  username: 'default',
  password: '',
  database: 'my_database',
  schemas: [User], // Register your schema classes
  logging: true
})
```

### 3. Synchronize Schema

```typescript
const schemaBuilder = connection.createSchemaBuilder()
await schemaBuilder.synchronize() // Creates/updates tables based on your schemas
```

### 4. Query Your Data

```typescript
// Simple query
const users = await connection
  .createQueryBuilder()
  .select(['id', 'name', 'age'])
  .from('user')
  .where('age > :age', { age: 18 })
  .execute<User>()

// Complex query with joins
const userOrders = await connection
  .createQueryBuilder()
  .select(['u.name', 'o.order_id', 'o.total'])
  .from('user', 'u')
  .innerJoin('order', 'o', 'u.id = o.user_id')
  .where('o.total > :total', { total: 100 })
  .execute<{ name: string; order_id: string; total: number }>()
```

## Schema Definition

### Table Decorators

```typescript
@Schema({ engine: 'MergeTree' })
export class MyTable {
  // columns...
}

// With custom table name
@Schema({ name: 'custom_table_name', engine: 'MergeTree' })
export class MyTable {
  // columns...
}
```

### Column Decorators

```typescript
export class Example {
  @Column({ type: 'String', primary: true })
  id: string

  @Column({ type: 'DateTime' })
  timestamp: string

  @Column({ type: 'Array(Int32)' })
  numbers: number[]

  @Column({ type: 'Nullable(String)' })
  optionalField?: string
}
```

### Materialized Views

```typescript
@Schema({ engine: 'MergeTree' })
export class UserStatsStorage {
  @Column({ type: 'String' })
  userId: string

  @Column({ type: 'UInt32' })
  orderCount: number
}

@Schema({
  materialized: true,
  materializedQuery: (qb) => 
    qb.select(['user_id', 'COUNT(*) as order_count'])
      .from('orders')
      .groupBy('user_id'),
  materializedTo: 'user_stats_storage'
})
export class UserStats {}
```

## Migrations

### Creating Migrations

Create migration files in your migrations directory:

```typescript
// migrations/create_users_table.ts
import { QueryRunner } from 'clickhouse-toolkit'
import { Table } from 'clickhouse-toolkit'

export function up(queryRunner: QueryRunner) {
  return queryRunner.createTable(
    new Table({
      name: 'users',
      columns: [
        { name: 'id', type: 'UInt32', primary: true },
        { name: 'name', type: 'String' },
        { name: 'email', type: 'String' }
      ],
      engine: 'MergeTree'
    }),
    true
  )
}

export function down(queryRunner: QueryRunner) {
  return queryRunner.dropTable('users', true)
}
```

### Running Migrations

#### Programmatically

```typescript
const connection = await Connection.initialize({
  // ... connection options
  migrations: ['migrations/*'] // Path to migration files
})

await connection.migrator.init()
await connection.migrator.up() // Apply pending migrations
await connection.migrator.down(1) // Rollback last migration
await connection.migrator.status() // Show migration status
```

#### Using CLI

```bash
# Set environment variables
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USERNAME="default"
export CLICKHOUSE_PASSWORD=""
export CLICKHOUSE_DATABASE="my_database"
export CLICKHOUSE_MIGRATIONS="migrations/*"

# Run migrations
npx clickhouse-toolkit up
npx clickhouse-toolkit down 1
npx clickhouse-toolkit status
```

## Query Builder

### Parameter Type Handling

The library automatically handles ClickHouse parameter type casting based on your schema definitions. When you use parameters in your queries, the library looks up the column type from your schema and applies the appropriate type casting automatically.

**Usage:**
```typescript
// The library automatically casts parameters based on your schema
.where('age >= :age', { age: 18 })        // Uses column type from schema
.where('total > :total', { total: 100.50 }) // Uses column type from schema
.where('name LIKE :name', { name: '%john%' }) // Uses column type from schema
```

### Select Queries

```typescript
const queryBuilder = connection.createQueryBuilder()

// Basic select
const results = await queryBuilder
  .select(['id', 'name'])
  .from('users')
  .execute<User>()

// With conditions
const adults = await queryBuilder
  .select(['id', 'name', 'age'])
  .from('users')
  .where('age >= :age', { age: 18 })
  .execute<User>()

// With joins
const userOrders = await queryBuilder
  .select(['u.name', 'o.order_id'])
  .from('users', 'u')
  .innerJoin('orders', 'o', 'u.id = o.user_id')
  .where('o.total > :total', { total: 100 })
  .execute<{ name: string; order_id: string }>()
```

### Insert Queries

```typescript
// Insert single record
await connection.insert(
  { id: '1', name: 'John', age: 25 },
  'users'
)

// Insert multiple records
await connection.insert([
  { id: '1', name: 'John', age: 25 },
  { id: '2', name: 'Jane', age: 30 }
], 'users')
```

### Update Queries

```typescript
await connection
  .createQueryBuilder()
  .update('users')
  .set({ age: 26 })
  .where('id = :id', { id: '1' })
  .execute()
```

### Delete Queries

```typescript
await connection
  .createQueryBuilder()
  .delete()
  .from('users')
  .where('age < :age', { age: 18 })
  .execute()
```

## Schema Management

### Synchronization

```typescript
const schemaBuilder = connection.createSchemaBuilder()

// Full synchronization (creates, updates, drops tables)
await schemaBuilder.synchronize()

// Individual operations
await schemaBuilder.createTable(metadata)
await schemaBuilder.updateTable(metadata)
await schemaBuilder.dropTable('table_name')
```

### Schema Inspection

```typescript
// Get all tables
const tables = await schemaBuilder.getTables()

// Get columns for a table
const columns = await schemaBuilder.getColumns()
```

## Configuration

### Connection Options

```typescript
const connection = await Connection.initialize({
  url: 'http://localhost:8123',
  username: 'default',
  password: '',
  database: 'my_database',
  schemas: [User, Order], // Your schema classes
  migrations: ['migrations/*'], // Migration file patterns
  migrationsTableName: 'clickhouse_toolkit_migrations',
  logging: true, // Enable SQL query logging
  settings: {
    // ClickHouse settings
    max_execution_time: 60
  }
})
```

## Contributing

Contributions, issues, and feature requests are welcome! Feel free to open a pull request or submit an issue.

## License

This project is licensed under the MIT License.
