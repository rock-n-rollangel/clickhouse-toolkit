/**
 * ClickHouse Toolkit - Main exports
 * Safety-first, composable TypeScript toolkit for ClickHouse
 */

// Core exports
export * from './core/ast'
export * from './core/ir'
export * from './core/normalizer'
export * from './core/operators'
export * from './core/errors'
export * from './core/logger'
export * from './core/validator'
export * from './core/predicate-builder'
export * from './core/case-builder'

// Builder exports
export * from './builder/select-builder'
export * from './builder/insert-builder'
export * from './builder/update-builder'
export * from './builder/delete-builder'
export * from './builder/base-builder'

// Dialect exports
export * from './dialect-ch/renderer'

// Runner exports
export * from './runner/query-runner'

// Migration exports
export * from './migrate/migrator'
export * from './migrate/migration'
export * from './migrate/migration-plan'
export * from './migrate/drift-detection'

// Re-export commonly used operators for convenience
export {
  Eq,
  EqCol,
  Ne,
  Gt,
  Gte,
  Lt,
  Lte,
  In,
  NotIn,
  Between,
  Like,
  ILike,
  RegExp,
  IsNull,
  IsNotNull,
  HasAny,
  HasAll,
  InTuple,
  Exists,
  NotExists,
  And,
  Or,
  Not,
  startsWith,
  endsWith,
  contains,
} from './core/operators'

// Re-export builder factory functions
export { select } from './builder/select-builder'
export { insertInto } from './builder/insert-builder'
export { update } from './builder/update-builder'
export { deleteFrom } from './builder/delete-builder'

// Re-export SQL helper functions
export {
  // Expression helpers
  Column,
  Value,
  // Raw SQL function
  Raw,
  // CASE builder
  Case,
  // Aggregate functions
  Count,
  Sum,
  Avg,
  Min,
  Max,
  CountDistinct,
  // String functions
  Concat,
  Upper,
  Lower,
  Trim,
  Substring,
  // Math functions
  Round,
  Floor,
  Ceil,
  Abs,
  // Conditional functions
  If,
  Coalesce,
  // Date/Time functions
  Now,
  Today,
  ToDate,
  ToDateTime,
  FormatDateTime,
  // Array functions
  ArrayElement,
  ArrayLength,
  ArrayJoin,
  // Type conversion functions
  Cast,
  ToString,
  ToInt,
  ToFloat,
  // ClickHouse-specific functions
  Distinct,
  GroupArray,
  GroupUniqArray,
  UniqExact,
  Quantile,
  Median,
} from './core/sql-functions'
