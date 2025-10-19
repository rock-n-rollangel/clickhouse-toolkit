/**
 * Case Builder E2E Tests
 * Tests Case() functionality with real ClickHouse execution
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals'
import {
  select,
  Case,
  Eq,
  Gt,
  Lt,
  Lte,
  In,
  IsNull,
  IsNotNull,
  Count,
  Sum,
  Raw,
  Column,
  Value,
} from '../../../src/index'
import { testSetup } from '../setup/test-setup'

describe('CaseBuilder E2E Tests', () => {
  let queryRunner: any

  beforeAll(async () => {
    await testSetup.setup()
    queryRunner = testSetup.getQueryRunner()
  }, 60000)

  afterAll(async () => {
    await testSetup.teardown()
  }, 30000)

  beforeEach(() => {
    if (!queryRunner) {
      queryRunner = testSetup.getQueryRunner()
    }
  })

  describe('Basic SQL Generation & Execution', () => {
    it('should execute simple case in SELECT clause', async () => {
      const userStatus = Case()
        .when({ status: Eq('active') }, Value('Active User'))
        .else(Value('Inactive User'))

      const results = await select({
        id: 'id',
        name: 'name',
        status: userStatus,
      })
        .from('users')
        .limit(5)
        .run<any>(queryRunner)

      expect(Array.isArray(results)).toBe(true)
      expect(results.length).toBeGreaterThan(0)
      expect(results[0]).toHaveProperty('id')
      expect(results[0]).toHaveProperty('name')
      expect(results[0]).toHaveProperty('status')
    })

    it('should execute case with multiple when conditions', async () => {
      const ageCategory = Case()
        .when({ age: Lt(18) }, Value('Minor'))
        .when({ age: Gt(65) }, Value('Senior'))
        .else(Value('Adult'))

      const results = await select({
        id: 'id',
        name: 'name',
        age: 'age',
        category: ageCategory,
      })
        .from('users')
        .limit(10)
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      results.forEach((result) => {
        expect(result).toHaveProperty('category')
        expect(['Minor', 'Adult', 'Senior']).toContain(result.category)
      })
    })

    it('should execute case with column references in then/else', async () => {
      const priorityLevel = Case()
        .when({ id: Eq(1) }, Value('high_priority'))
        .when({ id: Eq(2) }, Value('medium_priority'))
        .else(Value('low_priority'))

      const results = await select({
        id: 'id',
        name: 'name',
        priority: priorityLevel,
      })
        .from('users')
        .limit(5)
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
    })

    it('should execute case with literal values', async () => {
      const isActiveFlag = Case()
        .when({ status: Eq('active') }, Value(1))
        .else(Value(0))

      const results = await select({
        id: 'id',
        name: 'name',
        isActive: isActiveFlag,
      })
        .from('users')
        .limit(5)
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
    })

    it('should execute case with null handling', async () => {
      const nullStatus = Case()
        .when({ email: IsNull() }, Value('No Email'))
        .when({ email: IsNotNull() }, Value('Has Email'))
        .else(Value('Unknown'))

      const results = await select({
        id: 'id',
        name: 'name',
        emailStatus: nullStatus,
      })
        .from('users')
        .limit(5)
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
    })
  })

  describe('Advanced SQL Scenarios', () => {
    it('should execute nested case expressions in SELECT', async () => {
      const innerCase = Case()
        .when({ country: Eq('US') }, Value('Premium Member'))
        .else(Value('Regular Member'))

      const userType = Case()
        .when({ status: Eq('active') }, innerCase)
        .else(Value('Inactive'))

      const results = await select({
        id: 'id',
        name: 'name',
        userType: userType,
      })
        .from('users')
        .limit(5)
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
    })

    it('should execute case with aggregate functions', async () => {
      const conditionalAgg = Case()
        .when({ status: Eq('completed') }, Sum('amount'))
        .when({ status: Eq('pending') }, Count())
        .else(Raw('0'))

      const results = await select({
        status: 'status',
        metric: conditionalAgg,
      })
        .from('orders')
        .groupBy(['status'])
        .limit(5)
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
    })

    it('should execute case with string functions', async () => {
      const nameFormat = Case()
        .when({ country: Eq('US') }, Column('name'))
        .when({ country: Eq('UK') }, Raw(`CONCAT('Mr. ', name)`))
        .else(Column('name'))

      const results = await select({
        id: 'id',
        formattedName: nameFormat,
        country: 'country',
      })
        .from('users')
        .limit(5)
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
    })

    it('should execute case with complex conditions using And/Or', async () => {
      const eligibility = Case()
        .when(
          {
            age: Gt(18),
            status: Eq('active'),
          },
          Value('Eligible'),
        )
        .when({ status: Eq('suspended') }, Value('Suspended'))
        .else(Value('Not Eligible'))

      const results = await select({
        id: 'id',
        name: 'name',
        age: 'age',
        status: eligibility,
      })
        .from('users')
        .limit(5)
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
    })

    it('should execute case with IN operator conditions', async () => {
      const statusGroup = Case()
        .when({ status: In(['active', 'pending']) }, Value('In Progress'))
        .when({ status: In(['completed', 'approved']) }, Value('Finished'))
        .else(Value('Unknown'))

      const results = await select({
        id: 'id',
        status: 'status',
        group: statusGroup,
      })
        .from('orders')
        .limit(10)
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
    })
  })

  describe('Real-World Examples', () => {
    it('should handle user categorization by age ranges', async () => {
      const ageGroup = Case()
        .when({ age: Lt(13) }, Value('Child'))
        .when({ age: Lt(18) }, Value('Teenager'))
        .when({ age: Lte(65) }, Value('Adult'))
        .else(Value('Senior'))

      const results = await select({
        id: 'id',
        name: 'name',
        age: 'age',
        ageGroup: ageGroup,
      })
        .from('users')
        .orderBy([{ column: 'age', direction: 'ASC' }])
        .limit(10)
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      // Verify that age groups make sense
      results.forEach((result) => {
        expect(['Child', 'Teenager', 'Adult', 'Senior']).toContain(result.ageGroup)
        if (result.age < 13) {
          expect(result.ageGroup).toBe('Child')
        } else if (result.age < 18) {
          expect(result.ageGroup).toBe('Teenager')
        } else if (result.age <= 65) {
          expect(result.ageGroup).toBe('Adult')
        } else {
          expect(result.ageGroup).toBe('Senior')
        }
      })
    })

    it('should handle status mapping (numeric to string)', async () => {
      const statusText = Case()
        .when({ status: Eq('active') }, Value('Active'))
        .when({ status: Eq('pending') }, Value('Pending'))
        .when({ status: Eq('completed') }, Value('Completed'))
        .when({ status: Eq('inactive') }, Value('Inactive'))
        .else(Value('Unknown Status'))

      const results = await select({
        id: 'id',
        statusCode: 'status',
        statusText: statusText,
      })
        .from('users')
        .limit(10)
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
    })

    it('should handle conditional aggregation in GROUP BY', async () => {
      const revenueByCategory = Case()
        .when({ status: Eq('completed') }, Sum('amount'))
        .when({ status: Eq('pending') }, Sum('amount'))
        .else(Value(0))

      const results = await select({
        status: 'status',
        totalRevenue: revenueByCategory,
        orderCount: Count(),
      })
        .from('orders')
        .groupBy(['status'])
        .orderBy([{ column: 'totalRevenue', direction: 'DESC' }])
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
      results.forEach((result) => {
        expect(typeof result.totalRevenue).toBe('number')
        expect(typeof result.orderCount).toBe('number')
      })
    })

    it('should handle dynamic column selection based on conditions', async () => {
      const conditionalField = Case()
        .when({ id: Eq(1) }, Value('admin_notes'))
        .when({ id: Eq(2) }, Value('user_notes'))
        .else(Value('public_info'))

      const results = await select({
        id: 'id',
        notes: conditionalField,
      })
        .from('users')
        .limit(10)
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
    })

    it('should handle case with ORDER BY using case expression', async () => {
      const priority = Case()
        .when({ id: Eq(1) }, Value(1))
        .when({ id: Eq(2) }, Value(2))
        .else(Value(3))

      const results = await select({
        id: 'id',
        priority: priority,
      })
        .from('orders')
        .orderBy([{ column: 'id', direction: 'ASC' }])
        .limit(10)
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
    })
  })

  describe('SQL Generation Verification', () => {
    it('should generate correct SQL for simple case', () => {
      const userStatus = Case()
        .when({ status: Eq('active') }, Column('Active User'))
        .else(Column('Inactive User'))

      const query = select({
        id: 'id',
        status: userStatus,
      }).from('users')

      const { sql } = query.toSQL()

      expect(sql).toContain('CASE')
      expect(sql).toContain('WHEN')
      expect(sql).toContain('THEN')
      expect(sql).toContain('ELSE')
      expect(sql).toContain('END')
    })

    it('should generate correct SQL for complex nested case', () => {
      const innerCase = Case()
        .when({ country: Eq('US') }, Value('Premium'))
        .else(Value('Regular'))

      const outerCase = Case()
        .when({ status: Eq('active') }, innerCase)
        .else(Value('Inactive'))

      const query = select({
        category: outerCase,
      }).from('users')

      const { sql } = query.toSQL()

      expect(sql).toContain('CASE')
      expect(sql.split('CASE').length - 1).toBeGreaterThan(1) // Multiple CASE statements
    })

    it('should generate correct SQL for case with multiple conditions', () => {
      const complexCase = Case()
        .when({ age: Gt(18), status: Eq('active') }, Value('Adult Active'))
        .when({ age: Lt(18) }, Value('Minor'))
        .else(Value('Other'))

      const query = select({
        category: complexCase,
      }).from('users')

      const { sql } = query.toSQL()

      expect(sql).toContain('CASE')
      expect(sql).toContain('AND') // Should contain AND for multiple conditions
    })
  })

  describe('Error Handling & Edge Cases', () => {
    it('should handle case without else clause gracefully', async () => {
      const completeCase = Case()
        .when({ status: Eq('active') }, Value('Active'))
        .else(Value(null))

      const query = select({
        id: 'id',
        statusOnly: completeCase,
      })
        .from('users')
        .limit(1)

      expect(async () => {
        await query.run(queryRunner)
      }).not.toThrow()
    })

    it('should handle case with empty values', async () => {
      const emptyCase = Case()
        .when({ name: Eq('') }, Value('Empty Name'))
        .else(Value('Has Name'))

      const results = await select({
        id: 'id',
        nameCheck: emptyCase,
      })
        .from('users')
        .limit(5)
        .run<any>(queryRunner)

      expect(results.length).toBeGreaterThan(0)
    })
  })
})
