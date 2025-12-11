import { describe, expect, it, beforeEach } from 'vitest'
import { AttributeExpressionMap } from '@/attributes/attribute-map'

describe('AttributeExpressionMap', () => {
  let map: AttributeExpressionMap

  beforeEach(() => {
    map = new AttributeExpressionMap()
  })

  describe('constructor', () => {
    it('should initialize with empty maps and sets', () => {
      expect(map.getNameCount()).toBe(0)
      expect(map.getValueCount()).toBe(0)
      expect(map.toDynamoAttributeExpression()).toEqual({})
    })
  })

  describe('addName', () => {
    it('should add a new attribute name', () => {
      map.addName('userId')
      expect(map.hasName('userId')).toBe(true)
      expect(map.getNameCount()).toBe(1)
    })

    it('should create correct placeholder for name', () => {
      map.addName('userId')
      expect(map.getPlaceholderFromName('userId')).toBe('#userId')
    })

    it('should not add duplicate names', () => {
      map.addName('userId')
      map.addName('userId')
      expect(map.getNameCount()).toBe(1)
    })

    it('should add multiple different names', () => {
      map.addName('userId')
      map.addName('email')
      map.addName('status')
      expect(map.getNameCount()).toBe(3)
      expect(map.hasName('userId')).toBe(true)
      expect(map.hasName('email')).toBe(true)
      expect(map.hasName('status')).toBe(true)
    })
  })

  describe('addValue', () => {
    it('should add a new attribute value', () => {
      map.addValue('test-value')
      expect(map.hasValue('test-value')).toBe(true)
      expect(map.getValueCount()).toBe(1)
    })

    it('should create sequential placeholders for values', () => {
      map.addValue('first')
      map.addValue('second')
      map.addValue('third')
      expect(map.getPlaceholderFromValue('first')).toBe(':v1')
      expect(map.getPlaceholderFromValue('second')).toBe(':v2')
      expect(map.getPlaceholderFromValue('third')).toBe(':v3')
    })

    it('should not add duplicate values', () => {
      map.addValue('test-value')
      map.addValue('test-value')
      expect(map.getValueCount()).toBe(1)
    })

    it('should handle different value types', () => {
      map.addValue('string-value')
      map.addValue(42)
      map.addValue(true)
      map.addValue(null)
      expect(map.getValueCount()).toBe(4)
    })

    it('should handle array values', () => {
      const arrayValue = ['a', 'b', 'c']
      map.addValue(arrayValue)
      expect(map.hasValue(arrayValue)).toBe(true)
      expect(map.getPlaceholderFromValue(arrayValue)).toBe(':v1')
    })

    it('should handle object values', () => {
      const objValue = { key: 'value' }
      map.addValue(objValue)
      expect(map.hasValue(objValue)).toBe(true)
      expect(map.getPlaceholderFromValue(objValue)).toBe(':v1')
    })
  })

  describe('add', () => {
    it('should add both name and value', () => {
      map.add('userId', '12345')
      expect(map.hasName('userId')).toBe(true)
      expect(map.hasValue('12345')).toBe(true)
      expect(map.getNameCount()).toBe(1)
      expect(map.getValueCount()).toBe(1)
    })

    it('should add multiple name-value pairs', () => {
      map.add('userId', '12345')
      map.add('email', 'test@example.com')
      map.add('status', 'active')
      expect(map.getNameCount()).toBe(3)
      expect(map.getValueCount()).toBe(3)
    })
  })

  describe('hasName', () => {
    it('should return true for existing names', () => {
      map.addName('userId')
      expect(map.hasName('userId')).toBe(true)
    })

    it('should return false for non-existing names', () => {
      expect(map.hasName('userId')).toBe(false)
    })
  })

  describe('hasValue', () => {
    it('should return true for existing values', () => {
      map.addValue('test-value')
      expect(map.hasValue('test-value')).toBe(true)
    })

    it('should return false for non-existing values', () => {
      expect(map.hasValue('test-value')).toBe(false)
    })
  })

  describe('getPlaceholderFromName', () => {
    it('should return placeholder for existing name', () => {
      map.addName('userId')
      expect(map.getPlaceholderFromName('userId')).toBe('#userId')
    })

    it('should return undefined for non-existing name', () => {
      expect(map.getPlaceholderFromName('userId')).toBeUndefined()
    })
  })

  describe('getPlaceholderFromValue', () => {
    it('should return placeholder for existing value', () => {
      map.addValue('test-value')
      expect(map.getPlaceholderFromValue('test-value')).toBe(':v1')
    })

    it('should return undefined for non-existing value', () => {
      expect(map.getPlaceholderFromValue('test-value')).toBeUndefined()
    })
  })

  describe('getNameFromPlaceholder', () => {
    it('should return name for valid placeholder', () => {
      map.addName('userId')
      expect(map.getNameFromPlaceholder('#userId')).toBe('userId')
    })

    it('should return undefined for non-existing placeholder', () => {
      expect(map.getNameFromPlaceholder('#userId')).toBeUndefined()
    })
  })

  describe('getValueFromPlaceholder', () => {
    it('should return value for valid placeholder', () => {
      map.addValue('test-value')
      expect(map.getValueFromPlaceholder(':v1')).toBe('test-value')
    })

    it('should return undefined for non-existing placeholder', () => {
      expect(map.getValueFromPlaceholder(':v1')).toBeUndefined()
    })
  })

  describe('getNameCount', () => {
    it('should return 0 for empty map', () => {
      expect(map.getNameCount()).toBe(0)
    })

    it('should return correct count', () => {
      map.addName('userId')
      map.addName('email')
      expect(map.getNameCount()).toBe(2)
    })
  })

  describe('getValueCount', () => {
    it('should return 0 for empty map', () => {
      expect(map.getValueCount()).toBe(0)
    })

    it('should return correct count', () => {
      map.addValue('value1')
      map.addValue('value2')
      expect(map.getValueCount()).toBe(2)
    })
  })

  describe('toDynamoAttributeExpression', () => {
    it('should return empty object when no attributes added', () => {
      expect(map.toDynamoAttributeExpression()).toEqual({})
    })

    it('should return only ExpressionAttributeNames when only names added', () => {
      map.addName('userId')
      map.addName('email')
      const result = map.toDynamoAttributeExpression()
      expect(result).toEqual({
        ExpressionAttributeNames: {
          '#userId': 'userId',
          '#email': 'email',
        },
      })
      expect(result.ExpressionAttributeValues).toBeUndefined()
    })

    it('should return only ExpressionAttributeValues when only values added', () => {
      map.addValue('test-value')
      map.addValue(42)
      const result = map.toDynamoAttributeExpression()
      expect(result).toEqual({
        ExpressionAttributeValues: {
          ':v1': 'test-value',
          ':v2': 42,
        },
      })
      expect(result.ExpressionAttributeNames).toBeUndefined()
    })

    it('should return both ExpressionAttributeNames and ExpressionAttributeValues', () => {
      map.add('userId', '12345')
      map.add('email', 'test@example.com')
      const result = map.toDynamoAttributeExpression()
      expect(result).toEqual({
        ExpressionAttributeNames: {
          '#userId': 'userId',
          '#email': 'email',
        },
        ExpressionAttributeValues: {
          ':v1': '12345',
          ':v2': 'test@example.com',
        },
      })
    })

    it('should handle complex values in expression', () => {
      map.add('items', ['a', 'b', 'c'])
      map.add('metadata', { key: 'value' })
      const result = map.toDynamoAttributeExpression()
      expect(result.ExpressionAttributeNames).toEqual({
        '#items': 'items',
        '#metadata': 'metadata',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':v1': ['a', 'b', 'c'],
        ':v2': { key: 'value' },
      })
    })
  })

  describe('integration scenarios', () => {
    it('should handle a typical update expression scenario', () => {
      map.add('userId', '12345')
      map.add('status', 'active')
      map.addName('updatedAt')
      map.addValue(Date.now())

      expect(map.getNameCount()).toBe(3)
      expect(map.getValueCount()).toBe(3)

      const expression = map.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toBeDefined()
      expect(expression.ExpressionAttributeValues).toBeDefined()
      expect(Object.keys(expression.ExpressionAttributeNames!)).toHaveLength(3)
      expect(Object.keys(expression.ExpressionAttributeValues!)).toHaveLength(3)
    })

    it('should handle reverse lookups correctly', () => {
      map.add('userId', '12345')
      map.add('email', 'test@example.com')

      const userIdPlaceholder = map.getPlaceholderFromName('userId')
      const emailPlaceholder = map.getPlaceholderFromName('email')

      expect(userIdPlaceholder).toBe('#userId')
      expect(emailPlaceholder).toBe('#email')

      expect(map.getNameFromPlaceholder(userIdPlaceholder!)).toBe('userId')
      expect(map.getNameFromPlaceholder(emailPlaceholder!)).toBe('email')

      const value1Placeholder = map.getPlaceholderFromValue('12345')
      const value2Placeholder = map.getPlaceholderFromValue('test@example.com')

      expect(value1Placeholder).toBe(':v1')
      expect(value2Placeholder).toBe(':v2')

      expect(map.getValueFromPlaceholder(value1Placeholder!)).toBe('12345')
      expect(map.getValueFromPlaceholder(value2Placeholder!)).toBe('test@example.com')
    })

    it('should maintain consistency when adding duplicate names and values', () => {
      map.add('userId', '12345')
      map.add('userId', '12345')
      map.add('userId', '67890')

      expect(map.getNameCount()).toBe(1)
      expect(map.getValueCount()).toBe(2)

      const expression = map.toDynamoAttributeExpression()
      expect(Object.keys(expression.ExpressionAttributeNames!)).toHaveLength(1)
      expect(Object.keys(expression.ExpressionAttributeValues!)).toHaveLength(2)
    })
  })
})
