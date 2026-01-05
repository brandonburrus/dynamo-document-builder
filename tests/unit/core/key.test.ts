import { key } from '@/core'
import { describe, it, expect } from 'vitest'

describe('key', () => {
  it('should create dynamo key strings', () => {
    expect(key('user', '123')).toBe('user#123')
    expect(key('order', '456')).toBe('order#456')
    expect(key('product', 789)).toBe('product#789')
    expect(key('USER', 123, 'TYPE', 'ADMIN')).toBe('USER#123#TYPE#ADMIN')
  })
})
