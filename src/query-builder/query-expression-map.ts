import { SchemaMetadata } from '@/metadata/schema-metadata'
import { ObjectLiteral } from '@/types/object-literal'
import { Params } from '@/types/params'
import { WhereClause } from '../types/where-clause'
import { JoinAttribute } from './join-attribute'

export class QueryExpressionMap {
  readonly '@instanceof' = Symbol.for('QueryExpressionMap')

  table: string
  tableAlias?: string
  selects: string[] = []
  groupBys: string[] = []
  orderBys: string[] = []
  orderByDirection: string = 'desc'
  limit?: number
  offset?: number
  parameters: Params[] = []
  processedParameters: Params = {}
  metadata?: SchemaMetadata
  insertValues: ObjectLiteral[] | string = []
  insertColumns?: string[]
  updateValue?: ObjectLiteral
  updateColumns?: string[]
  joinAttributes: JoinAttribute[] = []
  whereClauses: WhereClause[] = []

  public clone(): QueryExpressionMap {
    const map = new QueryExpressionMap()
    map.table = this.table
    map.tableAlias = this.tableAlias
    map.selects = this.selects
    map.groupBys = this.groupBys
    map.orderBys = this.orderBys
    map.orderByDirection = this.orderByDirection
    map.limit = this.limit
    map.offset = this.offset
    map.parameters = this.parameters
    map.processedParameters = this.processedParameters
    map.metadata = this.metadata
    map.insertValues = this.insertValues
    map.insertColumns = this.insertColumns
    map.updateValue = this.updateValue
    map.joinAttributes = this.joinAttributes

    return map
  }
}
