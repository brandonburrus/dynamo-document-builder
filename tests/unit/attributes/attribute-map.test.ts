import { describe, it, expect } from 'vitest'
import { AttributeExpressionMap } from '@/attributes/attribute-map'

describe('Attribute Expression Map', () => {
  it('should instantiate', () => {
    const attrMap = new AttributeExpressionMap()
    expect(attrMap).toBeInstanceOf(AttributeExpressionMap)
  })

  it('should add names', () => {
    const attrMap = new AttributeExpressionMap()

    expect(attrMap.addName('name')).toBe('#name')
    expect(attrMap.addName('value')).toBe('#value')
  })

  it('should add values', () => {
    const attrMap = new AttributeExpressionMap()

    expect(attrMap.addValue('John')).toBe(':v1')
    expect(attrMap.addValue(42)).toBe(':v2')
  })

  it('should add names and values at the same time', () => {
    const attrMap = new AttributeExpressionMap()

    expect(attrMap.add('age', 30)).toEqual(['#age', ':v1'])
    expect(attrMap.add('status', 'active')).toEqual(['#status', ':v2'])
  })

  it('should retrieve names', () => {
    const attrMap = new AttributeExpressionMap()
    attrMap.addName('category')

    expect(attrMap.getNameFromPlaceholder('#category')).toBe('category')
    expect(attrMap.getNameFromPlaceholder('#unknown')).toBeUndefined()
    expect(attrMap.getPlaceholderFromName('category')).toBe('#category')
  })

  it('should retrieve values', () => {
    const attrMap = new AttributeExpressionMap()
    attrMap.addValue(100)

    expect(attrMap.getValueFromPlaceholder(':v1')).toBe(100)
    expect(attrMap.getValueFromPlaceholder(':v2')).toBeUndefined()
    expect(attrMap.getPlaceholderFromValue(100)).toBe(':v1')
  })

  it('should check existence of names and values', () => {
    const attrMap = new AttributeExpressionMap()

    attrMap.add('status', 'active')

    expect(attrMap.hasName('status')).toBe(true)
    expect(attrMap.hasName('unknown')).toBe(false)
    expect(attrMap.hasValue('active')).toBe(true)
    expect(attrMap.hasValue('inactive')).toBe(false)
  })

  it('should counts names and values', () => {
    const attrMap = new AttributeExpressionMap()

    expect(attrMap.getNameCount()).toBe(0)
    expect(attrMap.getValueCount()).toBe(0)

    attrMap.addName('name')
    attrMap.addValue('John')

    expect(attrMap.getNameCount()).toBe(1)
    expect(attrMap.getValueCount()).toBe(1)

    attrMap.add('age', 30)

    expect(attrMap.getNameCount()).toBe(2)
    expect(attrMap.getValueCount()).toBe(2)
  })

  it('should converts to DynamoDB format', () => {
    const attrMap = new AttributeExpressionMap()

    attrMap.add('name', 'Alice')
    attrMap.add('age', 28)

    expect(attrMap.toDynamoAttributeNames()).toEqual({
      '#name': 'name',
      '#age': 'age',
    })

    expect(attrMap.toDynamoAttributeValues()).toEqual({
      ':v1': 'Alice',
      ':v2': 28,
    })

    expect(attrMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#name': 'name',
        '#age': 'age',
      },
      ExpressionAttributeValues: {
        ':v1': 'Alice',
        ':v2': 28,
      },
    })
  })

  it('should dedupe names and values', () => {
    const attrMap = new AttributeExpressionMap()

    attrMap.addName('status')
    attrMap.addName('status')
    attrMap.addValue('active')
    attrMap.addValue('active')

    expect(attrMap.getNameCount()).toBe(1)
    expect(attrMap.getValueCount()).toBe(1)

    expect(attrMap.toDynamoAttributeNames()).toEqual({
      '#status': 'status',
    })

    expect(attrMap.toDynamoAttributeValues()).toEqual({
      ':v1': 'active',
    })
  })
})
