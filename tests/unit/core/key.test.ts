import { key, indexKey } from '@/core'
import { describe, it, expect } from 'vitest'

describe('key', () => {
  it('should create dynamo key strings', () => {
    expect(key('user', '123')).toBe('user#123')
    expect(key('order', '456')).toBe('order#456')
    expect(key('product', 789)).toBe('product#789')
    expect(key('USER', 123, 'TYPE', 'ADMIN')).toBe('USER#123#TYPE#ADMIN')
  })

  it('should throw an error if no parts are provided', () => {
    expect(() => key()).toThrowError('At least one key part must be provided')
  })
})

describe('indexKey', () => {
  it('should create dynamo index key strings', () => {
    expect(indexKey('user', '123')).toBe('user#123')
    expect(indexKey('order', '456')).toBe('order#456')
    expect(indexKey('product', 789)).toBe('product#789')
    expect(indexKey('USER', 123, 'TYPE', 'ADMIN')).toBe('USER#123#TYPE#ADMIN')
  })

  it('should return undefined if any part is undefined', () => {
    expect(indexKey('user', undefined)).toBeUndefined()
    expect(indexKey(undefined, '123')).toBeUndefined()
    expect(indexKey('order', '456', undefined)).toBeUndefined()
    expect(indexKey(undefined)).toBeUndefined()
  })
})
