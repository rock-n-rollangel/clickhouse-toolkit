/**
 * Abstract base class for all query builders
 */

import { QueryNode } from '../core/ast'
import { ValidationResult } from '../core/ir'
import { Logger, LoggingComponent } from '../core/logger'
import { QueryValidator } from '../core/validator'
import { QueryNormalizer } from '../core/normalizer'
import { createValidationError } from '../core/errors'

export abstract class QueryBuilder<T extends QueryNode> extends LoggingComponent {
  protected validator: QueryValidator
  protected normalizer: QueryNormalizer

  constructor(logger?: Logger, componentName?: string) {
    super(logger, componentName)
    this.validator = new QueryValidator(this.logger)
    this.normalizer = new QueryNormalizer(this.logger)
  }

  /**
   * Validate the current query and throw if invalid
   */
  protected validateAndThrow(result: ValidationResult, context: string): void {
    if (!result.valid) {
      this.logger.error('Validation failed', { context, errors: result.errors })
      throw createValidationError(`Validation failed in ${context}: ${result.errors.join(', ')}`, undefined, context)
    }
  }

  /**
   * Abstract method to generate SQL from the query
   */
  abstract toSQL(): { sql: string; params: any[] }

  /**
   * Validate the current query
   */
  validate(): ValidationResult {
    return this.validator.validateQuery(this.getQueryNode())
  }

  /**
   * Abstract method to get the underlying query node
   */
  protected abstract getQueryNode(): T
}
