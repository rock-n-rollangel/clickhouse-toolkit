import { Connection } from '../connection/connection'
import { SchemaMetadata } from '../metadata/schema-metadata'
import { MetadataArgsStorage } from '../metadata/metadata-args-storage'
import { DeleteQueryBuilder } from '../query-builder/delete-query-builder'
import { InsertQueryBuilder } from '../query-builder/insert-query-builder'
import { QueryBuilder } from '../query-builder/query-builder'
import { QueryExpressionMap } from '../query-builder/query-expression-map'
import { SelectQueryBuilder } from '../query-builder/select-query-builder'
import { UpdateQueryBuilder } from '../query-builder/update-query-builder'
import { QueryRunner } from '../query-runner/query-runner'
import { SchemaBuilder } from '../schema-builder/schema-builder'
import { Table } from '../schema-builder/table'

export class InstanceChecker {
  public static isSelectQueryBuilder(obj: unknown): obj is SelectQueryBuilder<any> {
    return this.check(obj, 'SelectQueryBuilder')
  }

  public static isInsertQueryBuilder(obj: unknown): obj is InsertQueryBuilder<any> {
    return this.check(obj, 'InsertQueryBuilder')
  }

  public static isUpdateQueryBuilder(obj: unknown): obj is UpdateQueryBuilder<any> {
    return this.check(obj, 'UpdateQueryBuilder')
  }

  public static isDeleteQueryBuilder(obj: unknown): obj is DeleteQueryBuilder<any> {
    return this.check(obj, 'DeleteQueryBuilder')
  }

  public static isSchemaMetadata(obj: unknown): obj is SchemaMetadata {
    return this.check(obj, 'SchemaMetadata')
  }

  public static isTable(obj: unknown): obj is Table {
    return this.check(obj, 'Table')
  }

  public static isQueryRunner(obj: unknown): obj is QueryRunner {
    return this.check(obj, 'QueryRunner')
  }

  public static isQueryBuilder(obj: unknown): obj is QueryBuilder<any> {
    return this.check(obj, 'QueryBuilder')
  }

  public static isSchemaBuilder(obj: unknown): obj is SchemaBuilder {
    return this.check(obj, 'SchemaBuilder')
  }

  public static isMetadataArgsStorage(obj: unknown): obj is MetadataArgsStorage {
    return this.check(obj, 'MetadataArgsStorage')
  }

  public static isQueryExpressinMap(obj: unknown): obj is QueryExpressionMap<any> {
    return this.check(obj, 'QueryExpressionMap')
  }

  public static isConnection(obj: unknown): obj is Connection {
    return this.check(obj, 'Connection')
  }

  private static check(obj: unknown, name: string) {
    return (
      typeof obj === 'object' && obj !== null && (obj as { '@instanceof': symbol })['@instanceof'] === Symbol.for(name)
    )
  }
}
