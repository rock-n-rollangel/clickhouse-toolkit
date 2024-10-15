import { SchemaMetadata } from '../metadata/schema-metadata'

/**
 * Represents a join attribute used in query building.
 * This class encapsulates the details of a join operation between tables.
 */
export class JoinAttribute {
  readonly '@instanceof' = Symbol.for('JoinAttribute')

  /**
   * Creates an instance of JoinAttribute.
   * @param joinTable - The schema metadata of the table to join or the table name as a string.
   * @param direction - The type of join, either 'LEFT' or 'INNER'.
   * @param alias - An alias for the joined table.
   * @param condition - The condition for the join, typically expressed as a SQL WHERE clause.
   */
  constructor(
    public joinTable: SchemaMetadata | string,
    public direction: 'LEFT' | 'INNER',
    public alias: string,
    public condition: string,
  ) {}
}
