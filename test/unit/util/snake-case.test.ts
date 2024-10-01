import { snakeCase } from '@/util/string'

describe('snakeCase', () => {
  it('should return lowercase snake case string', () => {
    expect(snakeCase('PascalCaseString')).toEqual('pascal_case_string')
    expect(snakeCase('camelCase')).toEqual('camel_case')
  })
})
