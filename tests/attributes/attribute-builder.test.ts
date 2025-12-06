import { describe, expect, it } from 'vitest'
import { buildAttributeExpression } from '@/attributes/attribute-builder'

describe('buildAttributeExpression', () => {
  it('returns empty object when no names or values provided', () => {
    const result = buildAttributeExpression({})

    expect(result).toEqual({})
  })

  it('returns empty object when names and values are empty', () => {
    const result = buildAttributeExpression({
      names: {},
      values: {},
    })

    expect(result).toEqual({})
  })

  it('includes ExpressionAttributeNames when names provided', () => {
    const result = buildAttributeExpression({
      names: {
        '#name': 'name',
        '#id': 'id',
      },
    })

    expect(result).toEqual({
      ExpressionAttributeNames: {
        '#name': 'name',
        '#id': 'id',
      },
    })
  })

  it('includes ExpressionAttributeValues when values provided', () => {
    const result = buildAttributeExpression({
      values: {
        ':name': 'Test',
        ':id': '123',
      },
    })

    expect(result).toEqual({
      ExpressionAttributeValues: {
        ':name': 'Test',
        ':id': '123',
      },
    })
  })

  it('includes both ExpressionAttributeNames and ExpressionAttributeValues', () => {
    const result = buildAttributeExpression({
      names: {
        '#name': 'name',
        '#id': 'id',
      },
      values: {
        ':name': 'Test',
        ':id': '123',
      },
    })

    expect(result).toEqual({
      ExpressionAttributeNames: {
        '#name': 'name',
        '#id': 'id',
      },
      ExpressionAttributeValues: {
        ':name': 'Test',
        ':id': '123',
      },
    })
  })

  it('handles numeric values', () => {
    const result = buildAttributeExpression({
      names: {
        '#count': 'count',
      },
      values: {
        ':count': 42,
        ':price': 19.99,
      },
    })

    expect(result).toEqual({
      ExpressionAttributeNames: {
        '#count': 'count',
      },
      ExpressionAttributeValues: {
        ':count': 42,
        ':price': 19.99,
      },
    })
  })

  it('handles boolean values', () => {
    const result = buildAttributeExpression({
      values: {
        ':active': true,
        ':deleted': false,
      },
    })

    expect(result).toEqual({
      ExpressionAttributeValues: {
        ':active': true,
        ':deleted': false,
      },
    })
  })

  it('handles null values', () => {
    const result = buildAttributeExpression({
      values: {
        ':data': null,
      },
    })

    expect(result).toEqual({
      ExpressionAttributeValues: {
        ':data': null,
      },
    })
  })

  it('handles array values', () => {
    const result = buildAttributeExpression({
      values: {
        ':tags': ['tag1', 'tag2', 'tag3'],
      },
    })

    expect(result).toEqual({
      ExpressionAttributeValues: {
        ':tags': ['tag1', 'tag2', 'tag3'],
      },
    })
  })

  it('handles object values', () => {
    const result = buildAttributeExpression({
      values: {
        ':user': { name: 'John', age: 30 },
      },
    })

    expect(result).toEqual({
      ExpressionAttributeValues: {
        ':user': { name: 'John', age: 30 },
      },
    })
  })

  it('handles nested attribute names', () => {
    const result = buildAttributeExpression({
      names: {
        '#user': 'user',
        '#profile': 'profile',
        '#email': 'email',
      },
    })

    expect(result).toEqual({
      ExpressionAttributeNames: {
        '#user': 'user',
        '#profile': 'profile',
        '#email': 'email',
      },
    })
  })

  it('handles multiple value placeholders with same base name', () => {
    const result = buildAttributeExpression({
      names: {
        '#name': 'name',
      },
      values: {
        ':name': 'value1',
        ':name1': 'value2',
        ':name2': 'value3',
      },
    })

    expect(result).toEqual({
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':name': 'value1',
        ':name1': 'value2',
        ':name2': 'value3',
      },
    })
  })

  it('handles Date values', () => {
    const date = new Date('2024-01-01T00:00:00Z')
    const result = buildAttributeExpression({
      values: {
        ':timestamp': date,
      },
    })

    expect(result).toEqual({
      ExpressionAttributeValues: {
        ':timestamp': date,
      },
    })
  })

  it('handles Set values', () => {
    const stringSet = new Set(['a', 'b', 'c'])
    const result = buildAttributeExpression({
      values: {
        ':tags': stringSet,
      },
    })

    expect(result).toEqual({
      ExpressionAttributeValues: {
        ':tags': stringSet,
      },
    })
  })

  it('omits ExpressionAttributeNames when only empty names provided', () => {
    const result = buildAttributeExpression({
      names: {},
      values: {
        ':value': 'test',
      },
    })

    expect(result).toEqual({
      ExpressionAttributeValues: {
        ':value': 'test',
      },
    })
    expect(result).not.toHaveProperty('ExpressionAttributeNames')
  })

  it('omits ExpressionAttributeValues when only empty values provided', () => {
    const result = buildAttributeExpression({
      names: {
        '#name': 'name',
      },
      values: {},
    })

    expect(result).toEqual({
      ExpressionAttributeNames: {
        '#name': 'name',
      },
    })
    expect(result).not.toHaveProperty('ExpressionAttributeValues')
  })
})
