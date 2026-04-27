import { describe, it, expect } from '@jest/globals'
import { select, EqCol } from '../../../src/index'

describe('Advanced JOINs', () => {
  describe('ASOF', () => {
    it('renders ASOF INNER JOIN with ON', () => {
      const { sql } = select(['t.id', 'q.price'])
        .from('trades', 't')
        .asofInnerJoin('quotes', { 'q.symbol': EqCol('t.symbol') }, 'q')
        .toSQL()
      expect(sql).toBe(
        'SELECT `t`.`id`, `q`.`price` FROM `trades` AS `t` ASOF JOIN `quotes` AS `q` ON `q`.`symbol` = `t`.`symbol`',
      )
    })

    it('renders ASOF LEFT JOIN with ON', () => {
      const { sql } = select(['t.id'])
        .from('trades', 't')
        .asofLeftJoin('quotes', { 'q.symbol': EqCol('t.symbol') }, 'q')
        .toSQL()
      expect(sql).toBe(
        'SELECT `t`.`id` FROM `trades` AS `t` ASOF LEFT JOIN `quotes` AS `q` ON `q`.`symbol` = `t`.`symbol`',
      )
    })
  })

  describe('SEMI / ANTI / ANY', () => {
    it('renders LEFT SEMI JOIN', () => {
      const { sql } = select(['u.id'])
        .from('users', 'u')
        .leftSemiJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
        .toSQL()
      expect(sql).toBe(
        'SELECT `u`.`id` FROM `users` AS `u` LEFT SEMI JOIN `orders` AS `o` ON `o`.`user_id` = `u`.`id`',
      )
    })

    it('renders LEFT ANTI JOIN', () => {
      const { sql } = select(['u.id'])
        .from('users', 'u')
        .leftAntiJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
        .toSQL()
      expect(sql).toBe(
        'SELECT `u`.`id` FROM `users` AS `u` LEFT ANTI JOIN `orders` AS `o` ON `o`.`user_id` = `u`.`id`',
      )
    })

    it('renders LEFT ANY JOIN', () => {
      const { sql } = select(['u.id'])
        .from('users', 'u')
        .leftAnyJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
        .toSQL()
      expect(sql).toBe(
        'SELECT `u`.`id` FROM `users` AS `u` LEFT ANY JOIN `orders` AS `o` ON `o`.`user_id` = `u`.`id`',
      )
    })
  })

  describe('CROSS JOIN', () => {
    it('renders CROSS JOIN with no ON', () => {
      const { sql } = select(['u.id', 'p.id']).from('users', 'u').crossJoin('products', 'p').toSQL()
      expect(sql).toBe('SELECT `u`.`id`, `p`.`id` FROM `users` AS `u` CROSS JOIN `products` AS `p`')
    })

    it('rejects CROSS JOIN with USING', () => {
      expect(() =>
        select(['*']).from('users', 'u').crossJoin('products', 'p', { using: ['id'] }),
      ).toThrow(/CROSS JOIN cannot use USING/)
    })
  })

  describe('USING clause', () => {
    it('renders INNER JOIN with USING via options', () => {
      const { sql } = select(['*'])
        .from('users', 'u')
        .innerJoin('orders', {}, 'o', { using: ['user_id'] })
        .toSQL()
      expect(sql).toBe('SELECT * FROM `users` AS `u` INNER JOIN `orders` AS `o` USING (`user_id`)')
    })

    it('rejects passing both ON and USING', () => {
      expect(() =>
        select(['*'])
          .from('users', 'u')
          .innerJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o', { using: ['user_id'] }),
      ).toThrow(/cannot specify both an ON condition and options.using/)
    })

    it('rejects empty ON without USING', () => {
      expect(() => select(['*']).from('users', 'u').innerJoin('orders', {}, 'o')).toThrow(
        /requires either an ON condition or options.using/,
      )
    })
  })

  describe('GLOBAL', () => {
    it('renders GLOBAL INNER JOIN', () => {
      const { sql } = select(['*'])
        .from('users', 'u')
        .innerJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o', { global: true })
        .toSQL()
      expect(sql).toBe(
        'SELECT * FROM `users` AS `u` GLOBAL INNER JOIN `orders` AS `o` ON `o`.`user_id` = `u`.`id`',
      )
    })

    it('renders GLOBAL ASOF LEFT JOIN', () => {
      const { sql } = select(['*'])
        .from('trades', 't')
        .asofLeftJoin('quotes', { 'q.symbol': EqCol('t.symbol') }, 'q', { global: true })
        .toSQL()
      expect(sql).toBe(
        'SELECT * FROM `trades` AS `t` GLOBAL ASOF LEFT JOIN `quotes` AS `q` ON `q`.`symbol` = `t`.`symbol`',
      )
    })
  })

  describe('back-compat — existing JOIN methods unchanged', () => {
    it('innerJoin still renders the same SQL as before', () => {
      const { sql } = select(['u.id', 'o.amount'])
        .from('users', 'u')
        .innerJoin('orders', { 'o.user_id': EqCol('u.id') }, 'o')
        .toSQL()
      expect(sql).toBe(
        'SELECT `u`.`id`, `o`.`amount` FROM `users` AS `u` INNER JOIN `orders` AS `o` ON `o`.`user_id` = `u`.`id`',
      )
    })
  })
})
