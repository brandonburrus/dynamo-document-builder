import { describe, it, expect } from 'vitest'
import {
  type Condition,
  parseCondition,
  equals,
  notEquals,
  and,
  or,
  not,
  greaterThan,
  lessThan,
  greaterThanOrEqual,
  lessThanOrEqual,
  beginsWith,
  contains,
  between,
  isIn,
  exists,
  notExists,
  size,
  typeIs,
} from '@/conditions'

describe('Condition Parser', () => {
  it('should parse simple equality condition', () => {
    const condition: Condition = { age: 30 }
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('#age = :v1')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#age': 'age' },
      ExpressionAttributeValues: { ':v1': 30 },
    })
  })

  it('should parse multiple equality checks', () => {
    const condition: Condition = {
      name: 'John',
      age: 25,
    }
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('#name = :v1 AND #age = :v2')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#name': 'name',
        '#age': 'age',
      },
      ExpressionAttributeValues: {
        ':v1': 'John',
        ':v2': 25,
      },
    })
  })

  it('should parse equals() condition', () => {
    const condition: Condition = {
      status: equals('active'),
    }
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('#status = :v1')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':v1': 'active' },
    })
  })

  it('should parse notEquals() condition', () => {
    const condition: Condition = {
      status: notEquals('inactive'),
    }
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('#status <> :v1')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':v1': 'inactive' },
    })
  })

  it('should parse and() conditions', () => {
    const condition: Condition = and({ age: notEquals(25) }, { age: notEquals(30) })
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('#age <> :v1 AND #age <> :v2')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#age': 'age' },
      ExpressionAttributeValues: { ':v1': 25, ':v2': 30 },
    })
  })

  it('should parse implicit and conditions', () => {
    const condition: Condition = [{ age: greaterThan(18) }, { age: lessThan(65) }]
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('#age > :v1 AND #age < :v2')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#age': 'age' },
      ExpressionAttributeValues: { ':v1': 18, ':v2': 65 },
    })
  })

  it('should parse or() conditions', () => {
    const condition: Condition = or({ status: equals('active') }, { status: equals('pending') })
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('#status = :v1 OR #status = :v2')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':v1': 'active', ':v2': 'pending' },
    })
  })

  it('should parse not() condition', () => {
    const condition: Condition = not({ status: equals('inactive') })
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('NOT (#status = :v1)')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':v1': 'inactive' },
    })
  })

  it('should parse nested not() condition', () => {
    const condition: Condition = not(or({ status: equals('inactive') }, not({ age: lessThan(18) })))
    const result = parseCondition(condition)
    expect(result.conditionExpression).toBe('NOT (#status = :v1 OR NOT (#age < :v2))')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#status': 'status', '#age': 'age' },
      ExpressionAttributeValues: { ':v1': 'inactive', ':v2': 18 },
    })
  })

  it('should parse beginsWith() condition', () => {
    const condition: Condition = {
      name: beginsWith('A'),
    }
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('begins_with(#name, :v1)')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#name': 'name' },
      ExpressionAttributeValues: { ':v1': 'A' },
    })
  })

  it('should parse greaterThan() condition', () => {
    const condition: Condition = {
      age: greaterThan(21),
    }
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('#age > :v1')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#age': 'age' },
      ExpressionAttributeValues: { ':v1': 21 },
    })
  })

  it('should parse lessThan() condition', () => {
    const condition: Condition = {
      age: lessThan(65),
    }
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('#age < :v1')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#age': 'age' },
      ExpressionAttributeValues: { ':v1': 65 },
    })
  })

  it('should parse greaterThanOrEqual() condition', () => {
    const condition: Condition = {
      age: greaterThanOrEqual(18),
    }
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('#age >= :v1')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#age': 'age' },
      ExpressionAttributeValues: { ':v1': 18 },
    })
  })

  it('should parse lessThanOrEqual() condition', () => {
    const condition: Condition = {
      age: lessThanOrEqual(65),
    }
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('#age <= :v1')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#age': 'age' },
      ExpressionAttributeValues: { ':v1': 65 },
    })
  })

  it('should parse contains() condition', () => {
    const condition: Condition = {
      tags: contains('urgent'),
    }
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('contains(#tags, :v1)')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#tags': 'tags' },
      ExpressionAttributeValues: { ':v1': 'urgent' },
    })
  })

  it('should parse between() condition', () => {
    const condition: Condition = {
      age: between(18, 30),
    }
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('#age BETWEEN :v1 AND :v2')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#age': 'age' },
      ExpressionAttributeValues: { ':v1': 18, ':v2': 30 },
    })
  })

  it('should parse isIn() condition', () => {
    const condition: Condition = {
      status: isIn('active', 'pending', 'suspended'),
    }
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('#status IN (:v1, :v2, :v3)')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':v1': 'active', ':v2': 'pending', ':v3': 'suspended' },
    })
  })

  it('should parse exists() condition', () => {
    const condition: Condition = {
      profile: exists(),
    }
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('attribute_exists(#profile)')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#profile': 'profile' },
    })
  })

  it('should parse notExists() condition', () => {
    const condition: Condition = {
      profile: notExists(),
    }
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('attribute_not_exists(#profile)')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#profile': 'profile' },
    })
  })

  it('should parse size() condition', () => {
    const condition: Condition = {
      tags: size(5),
      count: size(greaterThan(10)),
    }
    const result = parseCondition(condition)
    expect(result.conditionExpression).toBe('size(#tags) = :v1 AND size(#count) > :v2')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#tags': 'tags',
        '#count': 'count',
      },
      ExpressionAttributeValues: {
        ':v1': 5,
        ':v2': 10,
      },
    })
  })

  it('should parse size() condition with various comparison operators', () => {
    const condition: Condition = {
      username: size(5),
      tags: size(lessThan(3)),
      items: size(greaterThanOrEqual(1)),
      description: size(lessThanOrEqual(100)),
      categories: size(notEquals(0)),
    }
    const result = parseCondition(condition)
    expect(result.conditionExpression).toBe(
      'size(#username) = :v1 AND size(#tags) < :v2 AND size(#items) >= :v3 AND size(#description) <= :v4 AND size(#categories) <> :v5',
    )
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#username': 'username',
        '#tags': 'tags',
        '#items': 'items',
        '#description': 'description',
        '#categories': 'categories',
      },
      ExpressionAttributeValues: {
        ':v1': 5,
        ':v2': 3,
        ':v3': 1,
        ':v4': 100,
        ':v5': 0,
      },
    })
  })

  it('should parse typeIs() condition', () => {
    const condition: Condition = {
      data: typeIs('S'),
    }
    const result = parseCondition(condition)

    expect(result.conditionExpression).toBe('attribute_type(#data, :v1)')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#data': 'data' },
      ExpressionAttributeValues: { ':v1': 'S' },
    })
  })
})
