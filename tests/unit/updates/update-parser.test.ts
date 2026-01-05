import { describe, it, expect } from 'vitest'
import { parseUpdate } from '@/updates/update-parser'
import {
  type UpdateValues,
  ref,
  remove,
  add,
  subtract,
  append,
  prepend,
  addToSet,
  removeFromSet,
} from '@/updates'

describe('Update Parser', () => {
  it('should parse simple set update', () => {
    const update: UpdateValues = {
      age: 31,
    }
    const result = parseUpdate(update)

    expect(result.updateExpression).toBe('SET #age = :v1')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: { '#age': 'age' },
      ExpressionAttributeValues: { ':v1': 31 },
    })
  })

  it('should parse multiple set updates', () => {
    const update: UpdateValues = {
      name: 'Jane',
      age: 28,
    }
    const result = parseUpdate(update)

    expect(result.updateExpression).toBe('SET #name = :v1, #age = :v2')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#name': 'name',
        '#age': 'age',
      },
      ExpressionAttributeValues: {
        ':v1': 'Jane',
        ':v2': 28,
      },
    })
  })

  it('should parse set update with reference', () => {
    const update: UpdateValues = {
      lastLogin: ref('currentTimestamp'),
    }
    const result = parseUpdate(update)

    expect(result.updateExpression).toBe('SET #lastLogin = #currentTimestamp')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#lastLogin': 'lastLogin',
        '#currentTimestamp': 'currentTimestamp',
      },
    })
  })

  it('should parse nested attribute update', () => {
    const update: UpdateValues = {
      'address.city': 'New York',
    }
    const result = parseUpdate(update)

    expect(result.updateExpression).toBe('SET #address.#city = :v1')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#address': 'address',
        '#city': 'city',
      },
      ExpressionAttributeValues: {
        ':v1': 'New York',
      },
    })
  })

  it('should parse update with nested attribute ref', () => {
    const update: UpdateValues = {
      'profile.lastUpdated': ref('timestamp.current'),
    }
    const result = parseUpdate(update)

    expect(result.updateExpression).toBe('SET #profile.#lastUpdated = #timestamp.#current')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#profile': 'profile',
        '#lastUpdated': 'lastUpdated',
        '#timestamp': 'timestamp',
        '#current': 'current',
      },
    })
  })

  it('should parse update ref with default value', () => {
    const update: UpdateValues = {
      score: ref('newScore', 0),
    }
    const result = parseUpdate(update)

    expect(result.updateExpression).toBe('SET #score = if_not_exists(#newScore, :v1)')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#score': 'score',
        '#newScore': 'newScore',
      },
      ExpressionAttributeValues: {
        ':v1': 0,
      },
    })
  })

  it('should parse multiple updates with refs and values', () => {
    const update: UpdateValues = {
      status: 'active',
      lastLogin: ref('currentTimestamp'),
      loginCount: ref('loginCount', 0),
    }
    const result = parseUpdate(update)

    expect(result.updateExpression).toBe(
      'SET #status = :v1, #lastLogin = #currentTimestamp, #loginCount = if_not_exists(#loginCount, :v2)',
    )
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#status': 'status',
        '#lastLogin': 'lastLogin',
        '#currentTimestamp': 'currentTimestamp',
        '#loginCount': 'loginCount',
      },
      ExpressionAttributeValues: {
        ':v1': 'active',
        ':v2': 0,
      },
    })
  })

  it('should throw an error on empty update', () => {
    const update: UpdateValues = {}
    expect(() => parseUpdate(update)).toThrowError()
  })

  it('should parse updates with a remove operation', () => {
    const update: UpdateValues = {
      name: 'Alice',
      age: remove(),
      address: remove(),
    }
    const result = parseUpdate(update)

    expect(result.updateExpression).toBe('SET #name = :v1 REMOVE #age, #address')
  })

  it('should parse updates with add and subtract operations', () => {
    const update: UpdateValues = {
      score: add(10),
      points: subtract(5),
    }
    const result = parseUpdate(update)

    expect(result.updateExpression).toBe('SET #score = #score + :v1, #points = #points - :v2')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#score': 'score',
        '#points': 'points',
      },
      ExpressionAttributeValues: {
        ':v1': 10,
        ':v2': 5,
      },
    })
  })

  it('should parse updates with append operations', () => {
    const update: UpdateValues = {
      tags: append(['newTag']),
    }
    const result = parseUpdate(update)

    expect(result.updateExpression).toBe('SET #tags = list_append(#tags, :v1)')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#tags': 'tags',
      },
      ExpressionAttributeValues: {
        ':v1': ['newTag'],
      },
    })
  })

  it('should parse updates with prepend operations', () => {
    const update: UpdateValues = {
      tags: prepend(['firstTag']),
    }
    const result = parseUpdate(update)

    expect(result.updateExpression).toBe('SET #tags = list_append(:v1, #tags)')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#tags': 'tags',
      },
      ExpressionAttributeValues: {
        ':v1': ['firstTag'],
      },
    })
  })

  it('should parse updates with addToSet operations', () => {
    const update: UpdateValues = {
      roles: addToSet(['admin']),
    }
    const result = parseUpdate(update)

    expect(result.updateExpression).toBe('ADD #roles :v1')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#roles': 'roles',
      },
      ExpressionAttributeValues: {
        ':v1': new Set(['admin']),
      },
    })
  })

  it('should parse updates with removeFromSet operations', () => {
    const update: UpdateValues = {
      roles: removeFromSet(['guest']),
    }
    const result = parseUpdate(update)

    expect(result.updateExpression).toBe('DELETE #roles :v1')
    expect(result.attributeExpressionMap.toDynamoAttributeExpression()).toEqual({
      ExpressionAttributeNames: {
        '#roles': 'roles',
      },
      ExpressionAttributeValues: {
        ':v1': new Set(['guest']),
      },
    })
  })
})
