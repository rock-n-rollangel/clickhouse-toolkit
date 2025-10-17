# ClickHouse Toolkit

A safety-first, composable TypeScript toolkit for building safe, ergonomic ClickHouse queries and migrations.

[![npm version](https://img.shields.io/npm/v/clickhouse-toolkit.svg)](https://www.npmjs.com/package/clickhouse-toolkit)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Query Builder](#query-builder)
  - [SELECT Queries](#select-queries)
  - [INSERT Queries](#insert-queries)
  - [UPDATE Queries](#update-queries)
  - [DELETE Queries](#delete-queries)
  - [Operators and Predicates](#operators-and-predicates)
  - [Joins](#joins)
  - [Aggregations](#aggregations)
  - [Subqueries](#subqueries)
  - [ClickHouse-Specific Features](#clickhouse-specific-features)
- [Streaming](#streaming)
- [Migrations](#migrations)
  - [CLI Usage](#cli-usage)
  - [Programmatic Usage](#programmatic-usage)
  - [Migration Files](#migration-files)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Examples](#examples)
- [License](#license)

## Features

- **Fluent Query Builder** - Chain methods for building complex SQL queries with full type safety
- **ClickHouse-Aware** - Built specifically for ClickHouse with support for FINAL, PREWHERE, arrays, maps, and more
- **Advanced Migrator** - Version-controlled migrations with drift detection, ON CLUSTER support, and dry-run capabilities
- **Multiple Insert Strategies** - Traditional SQL inserts, object-based inserts, and stream-based inserts for maximum flexibility
- **Streaming Support** - Built-in support for streaming large result sets with format validation and multiple formats
- **CLI Tool** - Command-line interface for running migrations and schema management
- **Type Safety** - Full TypeScript support with type inference and compile-time safety

## Installation

```bash
npm install clickhouse-toolkit
```

## Quick Start

```typescript
import { select, createQueryRunner, Eq, Gt } from 'clickhouse-toolkit'

// Create a query runner
const runner = createQueryRunner({
  url: 'http://localhost:8123',
  username: 'default',
  password: '',
  database: 'my_database'
})

// Build and execute a query
const users = await select(['id', 'name', 'email', 'age'])
  .from('users')
  .where({
    age: Gt(18),
    status: Eq('active')
  })
  .orderBy([{ column: 'created_at', direction: 'DESC' }])
  .limit(10)
  .run(runner)

console.log(users)
```

## Query Builder

### SELECT Queries

Build SELECT queries with a fluent, type-safe API:

```typescript
import { select, Eq, In, Between, Like, IsNotNull } from 'clickhouse-toolkit'

// Simple SELECT
const users = await select(['id', 'name', 'email'])
  .from('users')
  .run(runner)

// SELECT with aliases
const results = await select(['u.id', 'u.name'])
  .from('users', 'u')
  .run(runner)

// SELECT with WHERE
const activeUsers = await select(['*'])
  .from('users')
  .where({
    status: Eq('active'),
    age: Between(18, 65),
    country: In(['US', 'CA', 'UK']),
    name: Like('%John%'),
    email: IsNotNull()
  })
  .run(runner)

// SELECT with ORDER BY and LIMIT
const recentUsers = await select(['id', 'name', 'created_at'])
  .from('users')
  .orderBy([
    { column: 'created_at', direction: 'DESC' },
    { column: 'id', direction: 'ASC' }
  ])
  .limit(20)
  .run(runner)

// Object notation with aliases
const results = await select({
  userId: 'id',
  userName: 'name',
  userEmail: 'email'
})
  .from('users')
  .run(runner)
```

### INSERT Queries

Insert data safely with multiple strategies and automatic value formatting:

#### Traditional Value-Based Inserts

```typescript
import { insertInto } from 'clickhouse-toolkit'

// Insert single row
await insertInto('users')
  .columns(['id', 'name', 'email', 'age'])
  .values([[1, 'John Doe', 'john@example.com', 30]])
  .run(runner)

// Insert multiple rows
await insertInto('users')
  .columns(['id', 'name', 'email', 'age'])
  .values([
    [1, 'John Doe', 'john@example.com', 30],
    [2, 'Jane Smith', 'jane@example.com', 25],
    [3, 'Bob Johnson', 'bob@example.com', 35]
  ])
  .run(runner)

// Insert with complex data types
await insertInto('users')
  .columns(['id', 'name', 'tags', 'metadata', 'created_at'])
  .values([[
    1,
    'John Doe',
    ['admin', 'developer'],                    // Array
    { department: 'Engineering', level: 'Senior' },  // Map
    new Date('2024-01-15T10:30:00Z')           // DateTime
  ]])
  .run(runner)
```

#### Object-Based Inserts

Insert JavaScript objects directly using ClickHouse client's native insert method:

```typescript
import { insertInto } from 'clickhouse-toolkit'

// Insert single object
await insertInto('users')
  .objects([{ 
    id: 1, 
    name: 'John Doe', 
    email: 'john@example.com', 
    age: 30 
  }])
  .format('JSONEachRow')
  .run(runner)

// Insert multiple objects
await insertInto('users')
  .objects([
    { id: 1, name: 'John Doe', email: 'john@example.com', age: 30 },
    { id: 2, name: 'Jane Smith', email: 'jane@example.com', age: 25 },
    { id: 3, name: 'Bob Johnson', email: 'bob@example.com', age: 35 }
  ])
  .format('JSONEachRow')
  .run(runner)
```

#### Stream-Based Inserts

Insert data from Node.js streams for large datasets:

```typescript
import { insertInto } from 'clickhouse-toolkit'
import { Readable } from 'stream'

// Insert from CSV stream
const csvData = '1,John Doe,john@example.com,30\n2,Jane Smith,jane@example.com,25'
const csvStream = Readable.from([csvData], { objectMode: false })

await insertInto('users')
  .fromStream(csvStream)
  .format('CSV')
  .run(runner)

// Insert from JSONEachRow stream
const jsonData = [
  { id: 1, name: 'John', email: 'john@example.com' },
  { id: 2, name: 'Jane', email: 'jane@example.com' }
]
const jsonStream = Readable.from(jsonData, { objectMode: true })

await insertInto('users')
  .fromStream(jsonStream)
  .format('JSONEachRow')
  .run(runner)

// Insert large dataset efficiently
const largeDataset = generateLargeDataset() // Your data generator
const dataStream = Readable.from(largeDataset, { objectMode: false })

await insertInto('events')
  .fromStream(dataStream)
  .format('JSONEachRow')
  .run(runner)
```

### UPDATE Queries

**Note:** ClickHouse UPDATE operations are asynchronous mutations that process in the background.

```typescript
import { update, Eq, Gt, In } from 'clickhouse-toolkit'

// Update single column
await update('users')
  .set({ status: 'inactive' })
  .where({ id: Eq(1) })
  .run(runner)

// Update multiple columns
await update('users')
  .set({
    status: 'promoted',
    salary: 75000.00
  })
  .where({ id: Eq(2) })
  .run(runner)

// Update with complex conditions
await update('users')
  .set({ status: 'verified' })
  .where({
    age: Gt(18),
    country: In(['US', 'CA'])
  })
  .run(runner)

// Update with NULL
await update('users')
  .set({ middle_name: null })
  .where({ id: Eq(3) })
  .run(runner)
```

### DELETE Queries

**Note:** ClickHouse DELETE operations are asynchronous mutations that process in the background.

```typescript
import { deleteFrom, Eq, Gt, Between } from 'clickhouse-toolkit'

// Delete single row
await deleteFrom('users')
  .where({ id: Eq(1) })
  .run(runner)

// Delete with complex conditions
await deleteFrom('users')
  .where({
    status: Eq('inactive'),
    last_login: Lt(new Date('2023-01-01'))
  })
  .run(runner)

// Delete with range
await deleteFrom('users')
  .where({ age: Between(60, 100) })
  .run(runner)
```

### Operators and Predicates

Available operators for building WHERE clauses:

```typescript
import { 
  Eq, Neq, Gt, Gte, Lt, Lte,     // Comparison
  In, NotIn,                       // Membership
  Between,                         // Range
  Like, ILike,                     // Pattern matching
  IsNull, IsNotNull,               // NULL checks
  And, Or, Not,                    // Logical combinators
  EqCol                            // Column comparison
} from 'clickhouse-toolkit'

// Comparison operators
where({ age: Eq(25) })
where({ age: Gt(18) })
where({ age: Gte(21) })
where({ age: Lt(65) })
where({ age: Lte(60) })
where({ status: Neq('deleted') })

// Membership
where({ country: In(['US', 'CA', 'UK']) })
where({ role: NotIn(['guest', 'anonymous']) })

// Range
where({ age: Between(18, 65) })

// Pattern matching
where({ name: Like('%John%') })
where({ email: ILike('%@gmail.com') })  // Case-insensitive

// NULL checks
where({ deleted_at: IsNull() })
where({ email: IsNotNull() })

// Logical combinators
where({
  age: And(Gte(18), Lt(65)),
  status: Or(Eq('active'), Eq('pending'))
})

where({
  status: Not(Eq('deleted'))
})

// Column comparison (for JOINs)
.innerJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
```

### Joins

Build JOIN queries with type-safe conditions:

```typescript
import { select, EqCol } from 'clickhouse-toolkit'

// INNER JOIN
const results = await select(['u.id', 'u.name', 'o.product_name', 'o.amount'])
  .from('users', 'u')
  .innerJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
  .run(runner)

// LEFT JOIN
const results = await select(['u.id', 'u.name', 'o.product_name'])
  .from('users', 'u')
  .leftJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
  .run(runner)

// Multiple JOINs
const results = await select(['u.name', 'o.product_name', 'p.price'])
  .from('users', 'u')
  .innerJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
  .innerJoin('products', { 'p.name': EqCol('o.product_name') }, 'p')
  .run(runner)

// RIGHT JOIN
const results = await select(['*'])
  .from('users', 'u')
  .rightJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
  .run(runner)

// FULL JOIN
const results = await select(['*'])
  .from('users', 'u')
  .fullJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
  .run(runner)
```

### Aggregations

Perform aggregations with GROUP BY and HAVING:

```typescript
import { select, Count, Sum, Avg, Min, Max, Gt } from 'clickhouse-toolkit'

// Simple aggregation
const results = await select({
  country: 'country',
  userCount: Count()
})
  .from('users')
  .groupBy(['country'])
  .run(runner)

// Multiple aggregations
const results = await select({
  userId: 'user_id',
  totalAmount: Sum('amount'),
  avgAmount: Avg('amount'),
  minAmount: Min('amount'),
  maxAmount: Max('amount'),
  orderCount: Count()
})
  .from('orders')
  .groupBy(['user_id'])
  .run(runner)

// With HAVING clause
const results = await select(['user_id', 'sum(amount) as total'])
  .from('orders')
  .groupBy(['user_id'])
  .having({ total: Gt(1000) })
  .run(runner)
```

### Subqueries

Use subqueries in WHERE clauses and SELECT lists:

```typescript
import { select, In, Gt, Eq } from 'clickhouse-toolkit'

// Subquery in WHERE with IN
const userIdsWithOrders = select(['user_id']).from('orders')

const users = await select(['id', 'name'])
  .from('users')
  .where({ id: In(userIdsWithOrders) })
  .run(runner)

// Subquery with comparison operators
const avgAge = select(['avg(age)']).from('users')

const olderUsers = await select(['id', 'name', 'age'])
  .from('users')
  .where({ age: Gt(avgAge) })
  .run(runner)

// Scalar subquery in SELECT (non-correlated)
const results = await select({
  id: 'id',
  name: 'name',
  avgAge: select(['avg(age)']).from('users')
})
  .from('users')
  .limit(10)
  .run(runner)
```

**Note:** Correlated subqueries are not supported due to ClickHouse limitations with the experimental feature.

### ClickHouse-Specific Features

Take advantage of ClickHouse-specific optimizations:

```typescript
import { select } from 'clickhouse-toolkit'

// Use FINAL modifier for ReplacingMergeTree
const results = await select(['id', 'name'])
  .from('users')
  .final()
  .run(runner)

// Use PREWHERE for optimization
const results = await select(['*'])
  .from('events')
  .prewhere({ event_type: Eq('click') })
  .where({ user_id: Eq(123) })
  .run(runner)

// Apply query settings
const results = await select(['id', 'name'])
  .from('users')
  .settings({
    max_execution_time: 30,
    max_threads: 4,
    max_memory_usage: 10000000000
  })
  .run(runner)
```

## Streaming

Stream large datasets efficiently with format options and validation:

```typescript
import { select } from 'clickhouse-toolkit'

// Stream with default JSONEachRow format
const stream = await select(['id', 'name', 'email'])
  .from('users')
  .stream(runner)

const results: any[] = []
stream.on('data', (rows: any[]) => {
  results.push(...rows)
})

stream.on('end', () => {
  console.log(`Processed ${results.length} rows`)
})

stream.on('error', (error) => {
  console.error('Stream error:', error)
})

// Stream with explicit format using .format() method
const stream = await select(['id', 'name', 'email'])
  .from('users')
  .format('JSONEachRow')
  .stream(runner)

// Stream with CSV format
const csvStream = await select(['id', 'name', 'email'])
  .from('users')
  .format('CSV')
  .stream(runner)

// Stream with TabSeparated format
const tsvStream = await select(['id', 'name', 'email'])
  .from('users')
  .format('TabSeparated')
  .stream(runner)

// Stream with filters and settings
const stream = await select(['id', 'name', 'created_at'])
  .from('users')
  .where({ status: Eq('active') })
  .settings({ max_execution_time: 60 })
  .format('JSONEachRow')
  .stream(runner)
```

### Supported Streaming Formats

The following formats are supported for streaming operations:

- **JSONEachRow** - One JSON object per line (default)
- **JSONStringsEachRow** - JSON with string values
- **JSONCompactEachRow** - Compact JSON format
- **JSONCompactStringsEachRow** - Compact JSON with string values
- **JSONCompactEachRowWithNames** - Compact JSON with column names
- **JSONCompactEachRowWithNamesAndTypes** - Compact JSON with names and types
- **JSONCompactStringsEachRowWithNames** - Compact JSON strings with names
- **JSONCompactStringsEachRowWithNamesAndTypes** - Compact JSON strings with names and types
- **CSV** - Comma-separated values
- **CSVWithNames** - CSV with header row
- **CSVWithNamesAndTypes** - CSV with header and types
- **TabSeparated** - Tab-separated values
- **TabSeparatedRaw** - Raw tab-separated values
- **TabSeparatedWithNames** - Tab-separated with header
- **TabSeparatedWithNamesAndTypes** - Tab-separated with header and types

### Format Validation

The toolkit automatically validates that only streamable formats are used with the `.stream()` method:

```typescript
// This works - JSONEachRow is streamable
const stream = await select(['id', 'name'])
  .from('users')
  .format('JSONEachRow')
  .stream(runner)

// This throws an error - JSON is not streamable
const stream = await select(['id', 'name'])
  .from('users')
  .format('JSON')
  .stream(runner) // Throws: Format 'JSON' is not streamable
```

## Migrations

### CLI Usage

The toolkit includes a CLI for managing migrations:

#### Installation

```bash
# Install globally
npm install -g clickhouse-toolkit

# Or use with npx
npx clickhouse-toolkit
```

#### Configuration

Set environment variables:

```bash
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USERNAME="default"
export CLICKHOUSE_PASSWORD=""
export CLICKHOUSE_DATABASE="my_database"
export CLICKHOUSE_MIGRATIONS_TABLE_NAME="migrations"  # optional
```

#### Commands

```bash
# Show migration status
clickhouse-toolkit migrate:status

# Show migration plan (dry-run)
clickhouse-toolkit migrate:plan

# Apply pending migrations
clickhouse-toolkit migrate:up

# Rollback last migration
clickhouse-toolkit migrate:down

# Rollback 3 migrations
clickhouse-toolkit migrate:down 3
```

### Programmatic Usage

Use migrations in your code:

```typescript
import { Migrator, SimpleMigration, createQueryRunner } from 'clickhouse-toolkit'

const runner = createQueryRunner({
  url: 'http://localhost:8123',
  username: 'default',
  password: '',
  database: 'my_database'
})

const migrator = new Migrator(runner, {
  migrationsTableName: 'migrations',
  migrationsPath: './migrations',
  migrationsPattern: '*.sql'
})

// Initialize
await migrator.init()

// Check status
const status = await migrator.status()
console.log(status)

// Apply migrations
await migrator.up()

// Rollback migrations
await migrator.down(1)

// Get migration plan
const plan = await migrator.plan()
plan.print()
```

### Migration Files

#### SQL Migrations

Create SQL migration files in the `migrations` directory:

**migrations/001_create_users.sql:**
```sql
-- up
CREATE TABLE users (
  id UInt32,
  name String,
  email String,
  age UInt8,
  created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;

-- down
DROP TABLE users;
```

#### Class-based Migrations

Create programmatic migrations:

```typescript
import { SimpleMigration } from 'clickhouse-toolkit'

const createUsersTable = new SimpleMigration(
  '001_create_users',
  'Create users table',
  `CREATE TABLE users (
    id UInt32,
    name String,
    email String
  ) ENGINE = MergeTree() ORDER BY id`,
  `DROP TABLE users`
)

const migrator = new Migrator(runner, {
  migrationClasses: [createUsersTable]
})
```

## Configuration

### QueryRunner Options

```typescript
interface QueryRunnerConfig {
  url: string              // ClickHouse HTTP URL
  username: string         // Username
  password: string         // Password
  database: string         // Database name
  timeout?: number         // Request timeout in ms (default: 30000)
  retries?: number         // Number of retries (default: 3)
  settings?: Record<string, any>  // Default ClickHouse settings
}
```

### Migrator Options

```typescript
interface MigratorOptions {
  migrationsTableName?: string      // Default: 'clickhouse_toolkit_migrations'
  migrationsPath?: string           // Default: './migrations'
  migrationsPattern?: string        // Default: '*.sql'
  tsMigrationsPath?: string         // Default: './migrations'
  tsMigrationsPattern?: string      // Default: '*.ts'
  migrationClasses?: Migration[]    // Programmatic migrations
  cluster?: string | null           // Cluster name for ON CLUSTER
  allowMutations?: boolean          // Allow mutations (default: false)
  dryRun?: boolean                  // Dry run mode (default: false)
}
```

## API Reference

### Query Builders

- `select(columns?: string[] | Record<string, string | SelectBuilder>)` - Create SELECT query
- `insertInto(table: string)` - Create INSERT query
- `update(table: string)` - Create UPDATE query
- `deleteFrom(table: string)` - Create DELETE query

### InsertBuilder Methods

- `.columns(cols: string[])` - Specify column names
- `.values(vals: any[][])` - Insert using value arrays (traditional SQL)
- `.value(row: any[])` - Add single row to values
- `.objects(data: Array<Record<string, any>>)` - Insert JavaScript objects
- `.fromStream(stream: NodeJS.ReadableStream)` - Insert from Node.js stream
- `.format(fmt: DataFormat)` - Specify data format
- `.run(runner?: QueryRunner)` - Execute the insert

### SelectBuilder Methods

- `.from(table: string, alias?: string)` - Specify source table
- `.where(conditions: Record<string, any>)` - Add WHERE conditions
- `.format(fmt: DataFormat)` - Specify output format
- `.stream(runner?: QueryRunner)` - Create streaming query
- `.run(runner?: QueryRunner)` - Execute query and return results

### Operators

- `Eq(value)` - Equal to
- `Neq(value)` - Not equal to
- `Gt(value)` - Greater than
- `Gte(value)` - Greater than or equal
- `Lt(value)` - Less than
- `Lte(value)` - Less than or equal
- `In(values)` - IN clause
- `NotIn(values)` - NOT IN clause
- `Between(start, end)` - BETWEEN clause
- `Like(pattern)` - LIKE pattern
- `ILike(pattern)` - Case-insensitive LIKE
- `IsNull()` - IS NULL
- `IsNotNull()` - IS NOT NULL
- `And(...conditions)` - AND combinator
- `Or(...conditions)` - OR combinator
- `Not(condition)` - NOT operator
- `EqCol(column)` - Column equality (for JOINs)

### Aggregation Functions

- `Count(column?)` - COUNT aggregate
- `Sum(column)` - SUM aggregate
- `Avg(column)` - AVG aggregate
- `Min(column)` - MIN aggregate
- `Max(column)` - MAX aggregate

### ClickHouse Functions

- `arrayElement(arr, index)` - Access array element
- `arrayLength(arr)` - Get array length
- `arrayConcat(arr1, arr2)` - Concatenate arrays
- `now()` - Current timestamp
- `today()` - Current date
- `toDate(value)` - Convert to date
- `formatDateTime(date, format)` - Format datetime

And many more! See the source code for complete function list.

## Examples

### Complex Query with Everything

```typescript
import { select, Eq, Gt, In, Between, And, Count, Sum } from 'clickhouse-toolkit'

const report = await select({
  country: 'u.country',
  userCount: Count(),
  totalRevenue: Sum('o.amount'),
  avgOrderValue: select(['avg(amount)']).from('orders')
})
  .from('users', 'u')
  .innerJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
  .where({
    'o.status': Eq('completed'),
    'u.age': Between(18, 65),
    'u.country': In(['US', 'CA', 'UK'])
  })
  .groupBy(['u.country'])
  .having({ totalRevenue: Gt(10000) })
  .orderBy([{ column: 'totalRevenue', direction: 'DESC' }])
  .limit(10)
  .settings({
    max_execution_time: 30,
    max_threads: 4
  })
  .run(runner)
```

### Complete Migration Workflow

```typescript
import { Migrator, SimpleMigration, createQueryRunner } from 'clickhouse-toolkit'

const runner = createQueryRunner({
  url: process.env.CLICKHOUSE_URL,
  username: process.env.CLICKHOUSE_USERNAME,
  password: process.env.CLICKHOUSE_PASSWORD,
  database: process.env.CLICKHOUSE_DATABASE
})

// Create migrations
const migrations = [
  new SimpleMigration(
    '001_create_users',
    'Create users table',
    `CREATE TABLE users (
      id UInt32,
      name String,
      email String
    ) ENGINE = MergeTree() ORDER BY id`,
    `DROP TABLE users`
  ),
  new SimpleMigration(
    '002_create_orders',
    'Create orders table',
    `CREATE TABLE orders (
      id UInt32,
      user_id UInt32,
      amount Decimal(10, 2)
    ) ENGINE = MergeTree() ORDER BY id`,
    `DROP TABLE orders`
  )
]

const migrator = new Migrator(runner, {
  migrationClasses: migrations,
  migrationsPath: './migrations'
})

// Initialize and run
await migrator.init()
await migrator.up()

// Check status
const status = await migrator.status()
console.log(status)
```

### Streaming Large Datasets

```typescript
import { select } from 'clickhouse-toolkit'

// Stream millions of rows efficiently
const stream = await select(['id', 'event_type', 'timestamp', 'data'])
  .from('events')
  .where({ event_type: Eq('click') })
  .orderBy([{ column: 'timestamp', direction: 'DESC' }])
  .settings({ max_execution_time: 300 })
  .format('JSONEachRow')
  .stream(runner)

let processedCount = 0

stream.on('data', (rows: any[]) => {
  rows.forEach(row => {
    // Process each row
    console.log(`Processing event ${row.id}`)
    processedCount++
  })
})

stream.on('end', () => {
  console.log(`Processed ${processedCount} events`)
})

stream.on('error', (error) => {
  console.error('Stream error:', error)
})
```

### Object-Based Data Insertion

```typescript
import { insertInto } from 'clickhouse-toolkit'

// Insert user data as objects
const users = [
  { id: 1, name: 'John Doe', email: 'john@example.com', age: 30, active: true },
  { id: 2, name: 'Jane Smith', email: 'jane@example.com', age: 25, active: true },
  { id: 3, name: 'Bob Johnson', email: 'bob@example.com', age: 35, active: false }
]

await insertInto('users')
  .objects(users)
  .format('JSONEachRow')
  .run(runner)

// Insert complex nested data
const events = [
  {
    id: 1,
    user_id: 123,
    event_type: 'click',
    properties: { page: '/home', element: 'button' },
    tags: ['web', 'mobile'],
    timestamp: new Date('2024-01-15T10:30:00Z')
  }
]

await insertInto('events')
  .objects(events)
  .format('JSONEachRow')
  .run(runner)
```

### Stream-Based Data Insertion

```typescript
import { insertInto } from 'clickhouse-toolkit'
import { Readable } from 'stream'

// Insert from CSV file stream
const csvData = '1,John Doe,john@example.com,30\n2,Jane Smith,jane@example.com,25'
const csvStream = Readable.from([csvData], { objectMode: false })

await insertInto('users')
  .fromStream(csvStream)
  .format('CSV')
  .run(runner)

// Insert large JSON dataset from stream
const jsonData = [
  { id: 1, name: 'User1', value: 100 },
  { id: 2, name: 'User2', value: 200 },
  { id: 3, name: 'User3', value: 300 }
]

const jsonStream = Readable.from(jsonData, { objectMode: true })

await insertInto('large_dataset')
  .fromStream(jsonStream)
  .format('JSONEachRow')
  .run(runner)
```

## Data Type Support

ClickHouse Toolkit automatically handles formatting for various data types:

- **Primitives:** String, Number, Boolean, NULL
- **Dates:** Date objects → ClickHouse DateTime format
- **Arrays:** JavaScript arrays → ClickHouse Array syntax `[...]`
- **Maps:** Plain objects → ClickHouse Map syntax `{'key': 'value'}`
- **Special Numbers:** Infinity, -Infinity, NaN

## Limitations

### Known Limitations

1. **Correlated Subqueries** - Not supported due to ClickHouse experimental feature limitations
2. **SQL Expressions in UPDATE** - Only literal values supported in SET clause (no `salary * 1.1`)
3. **Async Mutations** - UPDATE and DELETE are async in ClickHouse; immediate verification may not show changes
4. **Date Objects in UPDATE** - Use string literals or raw SQL for complex datetime expressions

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License

## Repository

[https://github.com/rock-n-rollangel/clickhouse-toolkit](https://github.com/rock-n-rollangel/clickhouse-toolkit)

