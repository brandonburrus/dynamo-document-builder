import { describe, it, expect } from 'vitest'
import { parseProjection } from '@/projections/projection-parser'

describe('Projection Parser', () => {
  it('should parse simple projection list', () => {
    const projection = ['name', 'age', 'email']
    const result = parseProjection(projection)

    expect(result.projectionExpression).toBe('#name, #age, #email')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#name': 'name',
        '#age': 'age',
        '#email': 'email',
      },
    })
  })

  it('should parse projection with nested attributes', () => {
    const projection = ['address.street', 'address.city', 'profile.picture']
    const result = parseProjection(projection)

    expect(result.projectionExpression).toBe('#address.#street, #address.#city, #profile.#picture')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#address': 'address',
        '#street': 'street',
        '#city': 'city',
        '#profile': 'profile',
        '#picture': 'picture',
      },
    })
  })

  it('should dedupe attribute names', () => {
    const projection = ['name', 'age', 'name', 'email', 'age']
    const result = parseProjection(projection)

    expect(result.projectionExpression).toBe('#name, #age, #email')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#name': 'name',
        '#age': 'age',
        '#email': 'email',
      },
    })
  })

  it('should handle indexed attributes', () => {
    const projection = ['items[0]', 'items[1]', 'details.info[2]']
    const result = parseProjection(projection)

    expect(result.projectionExpression).toBe('#items[0], #items[1], #details.#info[2]')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#items': 'items',
        '#details': 'details',
        '#info': 'info',
      },
    })
  })

  it('should handle mixed simple and nested attributes', () => {
    const projection = ['name', 'address.city', 'profile.picture', 'age']
    const result = parseProjection(projection)

    expect(result.projectionExpression).toBe('#name, #address.#city, #profile.#picture, #age')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#name': 'name',
        '#address': 'address',
        '#city': 'city',
        '#profile': 'profile',
        '#picture': 'picture',
        '#age': 'age',
      },
    })
  })

  it('should return empty projection for empty input', () => {
    const projection: string[] = []
    const result = parseProjection(projection)

    expect(result.projectionExpression).toBe('')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({})
  })

  it('should handle single attribute projection', () => {
    const projection = ['username']
    const result = parseProjection(projection)

    expect(result.projectionExpression).toBe('#username')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#username': 'username',
      },
    })
  })
})
