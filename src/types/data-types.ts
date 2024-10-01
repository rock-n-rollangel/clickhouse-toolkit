type StandaloneTypes =
  | 'UInt8'
  | 'UInt16'
  | 'UInt32'
  | 'UInt64'
  | 'UInt128'
  | 'Int8'
  | 'Int16'
  | 'Int32'
  | 'Int64'
  | 'Int128'
  | 'Float32'
  | 'Float64'
  | 'Boolean'
  | 'String'
  | 'UUID'
  | 'Date32'
  | 'Date64'
  | 'DateTime'
  | 'Enum'
  | 'JSON'
  | 'IPv4'
  | 'IPv6'
  | 'Point'
  | 'Ring'
  | 'Polygon'
  | 'MultiPolygon'
  | `FixedString(${number})`
  | `Decimal32(${number})`
  | `Decimal64(${number})`
  | `Decimal128(${number})`
  | `Decimal256(${number})`

type InheritingTypes = `Nullable(${StandaloneTypes})` | `LowCardinality(${StandaloneTypes})`

type MapType = `Map(${StandaloneTypes}, ${StandaloneTypes | InheritingTypes})`

type TupleType = `Tuple(${StandaloneTypes | InheritingTypes})`

type ArrayType = `Array(${StandaloneTypes | InheritingTypes})`

type NestedType = `Nested(${string} ${StandaloneTypes | InheritingTypes})`

type AggregateFunctionType = `AggregateFunction(${string} ${string})`

export type ColumnType =
  | StandaloneTypes
  | InheritingTypes
  | NestedType
  | ArrayType
  | TupleType
  | MapType
  | AggregateFunctionType
