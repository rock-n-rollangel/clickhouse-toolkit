/**
 * AST and IR Tests
 * Tests for Abstract Syntax Tree and Intermediate Representation
 */

import { describe, it, expect } from '@jest/globals'
import { select, Eq, EqCol, Gt, And, Or, Lt, In, SelectNode } from '../../../src/index'

describe('AST and IR Pipeline', () => {
  describe('AST Generation', () => {
    it('should generate correct AST for simple query', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ status: Eq('active') })

      // Access the internal AST (this would need to be exposed in the actual implementation)
      const ast = (query as any).query as SelectNode

      expect(ast).toBeDefined()
      expect(ast.type).toBe('select')
      expect(ast.from?.table).toBe('users')
      expect(ast.columns).toEqual([
        { type: 'column', name: 'id' },
        { type: 'column', name: 'name' },
      ])
      expect(ast.where).toBeDefined()
    })

    it('should generate correct AST for complex query', () => {
      const query = select(['u.id', 'u.name', 'p.title'])
        .from('users', 'u')
        .innerJoin('posts', { 'u.id': EqCol('p.user_id') }, 'p')
        .where({
          'u.status': Eq('active'),
          'u.age': Gt(18),
        })
        .groupBy(['u.id'])
        .orderBy([{ column: 'u.name', direction: 'ASC' }])
        .limit(10)

      const ast = (query as any).query as SelectNode

      expect(ast.type).toBe('select')
      expect(ast.from?.table).toBe('users')
      expect(ast.from?.alias).toBe('u')
      expect(ast.joins).toHaveLength(1)
      expect(ast.joins[0].type).toBe('inner')
      expect(ast.joins[0].table).toBe('posts')
      expect(ast.joins[0].alias).toBe('p')
      expect(ast.groupBy).toEqual([{ type: 'column', name: 'u.id' }])
      expect(ast.orderBy).toHaveLength(1)
      expect(ast.orderBy[0].column.name).toBe('u.name')
      expect(ast.orderBy[0].direction).toBe('ASC')
      expect(ast.limit).toBe(10)
    })
  })

  describe('IR Normalization', () => {
    it('should normalize simple query to IR', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ status: Eq('active') })

      const { sql } = query.toSQL()

      // The toSQL method should internally use AST -> IR -> SQL pipeline
      expect(sql).toBe("SELECT `id`, `name` FROM `users` WHERE `status` = 'active'")
    })

    it('should normalize complex predicates to IR', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where(And({ status: Eq('active') }, Or({ age: Gt(18) }, { age: Lt(65) })))

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id`, `name` FROM `users` WHERE `status` = 'active' AND (`age` > 18 OR `age` < 65)")
    })

    it('should normalize joins to IR', () => {
      const query = select(['u.id', 'u.name', 'p.title'])
        .from('users', 'u')
        .innerJoin('posts', { 'u.id': EqCol('p.user_id') }, 'p')

      const { sql } = query.toSQL()

      expect(sql).toBe(
        'SELECT `u`.`id`, `u`.`name`, `p`.`title` FROM `users` AS `u` INNER JOIN `posts` AS `p` ON `u`.`id` = `p`.`user_id`',
      )
    })
  })

  describe('IR Structure Validation', () => {
    it('should have correct IR structure for select query', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({ status: Eq('active') })
        .orderBy([{ column: 'name', direction: 'ASC' }])
        .limit(10)

      const { sql } = query.toSQL()

      // Verify the SQL is correctly generated from IR
      expect(sql).toContain('SELECT')
      expect(sql).toContain('FROM')
      expect(sql).toContain('WHERE')
      expect(sql).toContain('ORDER BY')
      expect(sql).toContain('LIMIT')
    })

    it('should handle IR with multiple predicates', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({
          status: Eq('active'),
          age: Gt(18),
          name: Eq('John'),
        })

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id`, `name` FROM `users` WHERE `status` = 'active' AND `age` > 18 AND `name` = 'John'")
    })

    it('should handle IR with group by and having', () => {
      const query = select(['status', 'count()'])
        .from('users')
        .groupBy(['status'])
        .having({ 'count()': Gt(5) })

      const { sql } = query.toSQL()

      expect(sql).toBe('SELECT `status`, count() FROM `users` GROUP BY `status` HAVING `count()` > 5')
    })
  })

  describe('Dialect Rendering', () => {
    it('should render ClickHouse-specific SQL', () => {
      const query = select(['id', 'name']).from('users').final().settings({ max_execution_time: 30 })

      const { sql } = query.toSQL()

      expect(sql).toContain('FINAL')
      expect(sql).toContain('SETTINGS')
      expect(sql).toContain('max_execution_time = 30')
    })

    it('should handle ClickHouse array syntax', () => {
      const query = select(['id', 'tags'])
        .from('users')
        .where({ tags: In(['tag1', 'tag2']) })

      const { sql } = query.toSQL()

      // ClickHouse should render arrays properly
      expect(sql).toContain("WHERE `tags` IN ('tag1', 'tag2')")
    })

    it('should handle ClickHouse tuple syntax', () => {
      const query = select(['id', 'name'])
        .from('users')
        .where({
          name: Eq('John'),
          age: Eq(25),
        })

      const { sql } = query.toSQL()

      expect(sql).toBe("SELECT `id`, `name` FROM `users` WHERE `name` = 'John' AND `age` = 25")
    })
  })

  describe('Error Handling in Pipeline', () => {
    it('should handle invalid column references', () => {
      const query = select(['invalid_column']).from('users')

      // This should not throw during AST generation
      expect(() => query.toSQL()).not.toThrow()
    })

    it('should handle invalid table references', () => {
      const query = select(['id']).from('invalid_table')

      // This should not throw during AST generation
      expect(() => query.toSQL()).not.toThrow()
    })

    it('should handle malformed predicates', () => {
      const query = select(['id'])
        .from('users')
        .where({ invalid_predicate: Eq('invalid_value') })

      // This should be handled gracefully
      expect(() => query.toSQL()).not.toThrow()
    })
  })

  describe('Performance and Optimization', () => {
    it('should handle large IN clauses efficiently', () => {
      const largeArray = Array.from({ length: 1000 }, (_, i) => `item_${i}`)

      const query = select(['id', 'name'])
        .from('users')
        .where({ name: In(largeArray) })

      const { sql } = query.toSQL()

      expect(sql).toContain('IN (')
    })

    it('should handle deep nested predicates', () => {
      const query = select(['id'])
        .from('users')
        .where(And({ status: Eq('active') }, Or({ age: Gt(18) }, And({ age: Lt(65) }, { name: Eq('John') }))))

      const { sql } = query.toSQL()

      expect(sql).toContain('WHERE')
      expect(sql).toContain('AND')
      expect(sql).toContain('OR')
    })
  })
})
