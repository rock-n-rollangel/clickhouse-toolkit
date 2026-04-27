import { describe, it, expect } from '@jest/globals'
import { select, Eq } from '../../../src/index'

describe('ARRAY JOIN', () => {
  it('renders ARRAY JOIN with single column array', () => {
    const { sql } = select(['user_id', 'tag']).from('users').arrayJoin(['tags']).toSQL()
    expect(sql).toBe('SELECT `user_id`, `tag` FROM `users` ARRAY JOIN `tags`')
  })

  it('renders ARRAY JOIN with aliased single column', () => {
    const { sql } = select(['user_id', 'tag']).from('users').arrayJoin({ tag: 'tags' }).toSQL()
    expect(sql).toBe('SELECT `user_id`, `tag` FROM `users` ARRAY JOIN `tags` AS `tag`')
  })

  it('renders zipped multi-column ARRAY JOIN', () => {
    const { sql } = select(['id', 'tag', 'score']).from('items').arrayJoin(['tags', 'scores']).toSQL()
    expect(sql).toBe('SELECT `id`, `tag`, `score` FROM `items` ARRAY JOIN `tags`, `scores`')
  })

  it('renders LEFT ARRAY JOIN', () => {
    const { sql } = select(['user_id', 'tag']).from('users').leftArrayJoin(['tags']).toSQL()
    expect(sql).toBe('SELECT `user_id`, `tag` FROM `users` LEFT ARRAY JOIN `tags`')
  })

  it('places ARRAY JOIN between JOIN and WHERE', () => {
    const { sql } = select(['id'])
      .from('events')
      .arrayJoin(['tags'])
      .where({ id: Eq(1) })
      .toSQL()
    expect(sql).toBe('SELECT `id` FROM `events` ARRAY JOIN `tags` WHERE `id` = 1')
  })

  it('replaces previous ARRAY JOIN when called twice', () => {
    const { sql } = select(['id']).from('events').arrayJoin(['tags']).leftArrayJoin(['scores']).toSQL()
    expect(sql).toBe('SELECT `id` FROM `events` LEFT ARRAY JOIN `scores`')
  })

  it('rejects empty array', () => {
    expect(() => select(['*']).from('t').arrayJoin([])).toThrow(/at least one array/)
  })

  it('rejects empty record', () => {
    expect(() => select(['*']).from('t').arrayJoin({})).toThrow(/at least one array/)
  })
})
