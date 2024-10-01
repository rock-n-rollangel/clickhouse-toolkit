# ClickHouse Toolkit

A customizable SQL query builder for [ClickHouse](https://clickhouse.com/) databases. This library provides a flexible and intuitive API to construct complex SQL queries with ease, supporting `SELECT`, `JOIN`, `WHERE`, and more.

## Features

- **Fluent API:** Chain methods for building queries.
- **Support for JOINs:** Easily build `INNER JOIN` and `LEFT JOIN` clauses.
- **Parameterized Queries:** Automatically escape parameters to prevent SQL injection.
- **SELECT statements:** Supports basic and advanced `SELECT` queries.

## Installation

Install the package via npm:

```bash
npm install @your-package-name/clickhouse-query-builder
```

## Usage

```typescript
import { SelectQueryBuilder } from '@your-package-name/clickhouse-query-builder'

// Example: Select with WHERE and JOIN
const qb = new SelectQueryBuilder()
const query = await qb
  .select(['id', 'name'])
  .from('users', 'u')
  .innerJoin('orders', 'o', 'u.id = o.user_id')
  .where('u.age > :age', { age: 18 })
  .execute<{ id: number; name: string }>()
```

## API

### `select(fields: string[] | string): SelectQueryBuilder`

Specifies the `SELECT` fields for the query.

```typescript
qb.select(['id', 'name'])
```

### `from(table: string, alias?: string): SelectQueryBuilder`

Defines the `FROM` clause of the query.

```typescript
qb.from('users', 'u')
```

### `where(statement: string, params?: Object): SelectQueryBuilder`

Adds a `WHERE` clause.

```typescript
qb.where('u.age > :age', { age: 18 })
```

### `innerJoin(table: string, alias: string, condition: string, params?: Object): SelectQueryBuilder`

Adds an `INNER JOIN`.

```typescript
qb.innerJoin('orders', 'o', 'u.id = o.user_id')
```

### `execute<T>(): T[]`

Builds the final SQL query string and executes it.

```typescript
const result = qb.execute<{ id: number; name: string }>()
```

## Customization

You can extend the `SelectQueryBuilder` class to add custom logic or override default behavior as per your needs.

## Contributing

Contributions, issues, and feature requests are welcome! Feel free to open a pull request or submit an issue.

## License

This project is licensed under the MIT License.
