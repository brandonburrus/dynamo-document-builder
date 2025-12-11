import { describe, expect, it } from 'vitest'
import { parseUpdate, InvalidUpdateDocumentBuilderError } from '@/updates/update-parser'
import { add } from '@/updates/add'
import { subtract } from '@/updates/subtract'
import { append } from '@/updates/append'
import { prepend } from '@/updates/prepend'
import { addToSet } from '@/updates/add-to-set'
import { removeFromSet } from '@/updates/delete'
import { remove } from '@/updates/remove'
import { ref } from '@/updates/ref'
import { $add, $set } from '@/updates/update-symbols'

describe('parseUpdate', () => {
  describe('SET operations', () => {
    it('should handle simple SET with primitive value', () => {
      const result = parseUpdate({
        name: 'John',
        age: 30,
      })

      expect(result.updateExpression).toBe('SET #name = :v1, #age = :v2')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#name': 'name',
        '#age': 'age',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': 'John',
        ':v2': 30,
      })
    })

    it('should handle SET with boolean values', () => {
      const result = parseUpdate({
        active: true,
        deleted: false,
      })

      expect(result.updateExpression).toBe('SET #active = :v1, #deleted = :v2')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#active': 'active',
        '#deleted': 'deleted',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': true,
        ':v2': false,
      })
    })

    it('should handle SET with null value', () => {
      const result = parseUpdate({
        data: null,
      })

      expect(result.updateExpression).toBe('SET #data = :v1')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#data': 'data',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': null,
      })
    })

    it('should handle SET with array value', () => {
      const result = parseUpdate({
        tags: ['tag1', 'tag2'],
      })

      expect(result.updateExpression).toBe('SET #tags = :v1')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#tags': 'tags',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': ['tag1', 'tag2'],
      })
    })

    it('should handle SET with object value', () => {
      const result = parseUpdate({
        metadata: { key: 'value' },
      })

      expect(result.updateExpression).toBe('SET #metadata = :v1')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#metadata': 'metadata',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': { key: 'value' },
      })
    })

    it('should handle SET with add expression', () => {
      const result = parseUpdate({
        count: add(5),
      })

      expect(result.updateExpression).toBe('SET #count = #count + :v1')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#count': 'count',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': 5,
      })
    })

    it('should handle SET with subtract expression', () => {
      const result = parseUpdate({
        balance: subtract(100),
      })

      expect(result.updateExpression).toBe('SET #balance = #balance - :v1')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#balance': 'balance',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': 100,
      })
    })

    it('should handle SET with append expression', () => {
      const result = parseUpdate({
        items: append(['item1', 'item2']),
      })

      expect(result.updateExpression).toBe('SET #items = list_append(#items, :v1)')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#items': 'items',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': ['item1', 'item2'],
      })
    })

    it('should handle SET with prepend expression', () => {
      const result = parseUpdate({
        items: prepend(['item1', 'item2']),
      })

      expect(result.updateExpression).toBe('SET #items = list_append(:v1, #items)')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#items': 'items',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': ['item1', 'item2'],
      })
    })

    it('should handle SET with nested attribute paths', () => {
      const result = parseUpdate({
        'user.profile.email': 'test@example.com',
      })

      expect(result.updateExpression).toBe('SET #user.#profile.#email = :v1')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#user': 'user',
        '#profile': 'profile',
        '#email': 'email',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': 'test@example.com',
      })
    })

    it('should handle SET with array index notation', () => {
      const result = parseUpdate({
        'items[0]': 'first-item',
      })

      expect(result.updateExpression).toBe('SET #items[0] = :v1')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#items': 'items',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': 'first-item',
      })
    })

    it('should handle SET with reference to another attribute', () => {
      const result = parseUpdate({
        newName: ref('oldName'),
      })

      expect(result.updateExpression).toBe('SET #newName = #oldName')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#newName': 'newName',
        '#oldName': 'oldName',
      })
      expect(expression.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle SET with reference with default value', () => {
      const result = parseUpdate({
        counter: ref('oldCounter', 0),
      })

      expect(result.updateExpression).toBe('SET #counter = if_not_exists(#oldCounter, :v1)')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#counter': 'counter',
        '#oldCounter': 'oldCounter',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': 0,
      })
    })

    it('should handle SET with add expression using reference', () => {
      const result = parseUpdate({
        count: add(ref('increment')),
      })

      expect(result.updateExpression).toBe('SET #count = #count + #increment')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#count': 'count',
        '#increment': 'increment',
      })
      expect(expression.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle SET with subtract expression using reference', () => {
      const result = parseUpdate({
        balance: subtract(ref('deduction')),
      })

      expect(result.updateExpression).toBe('SET #balance = #balance - #deduction')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#balance': 'balance',
        '#deduction': 'deduction',
      })
      expect(expression.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle SET with append expression using reference', () => {
      const result = parseUpdate({
        items: append(ref('newItems')),
      })

      expect(result.updateExpression).toBe('SET #items = list_append(#items, #newItems)')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#items': 'items',
        '#newItems': 'newItems',
      })
      expect(expression.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle SET with prepend expression using reference', () => {
      const result = parseUpdate({
        items: prepend(ref('newItems')),
      })

      expect(result.updateExpression).toBe('SET #items = list_append(#newItems, #items)')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#items': 'items',
        '#newItems': 'newItems',
      })
      expect(expression.ExpressionAttributeValues).toBeUndefined()
    })
  })

  describe('REMOVE operations', () => {
    it('should handle REMOVE single attribute', () => {
      const result = parseUpdate({
        oldField: remove(),
      })

      expect(result.updateExpression).toBe('REMOVE #oldField')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#oldField': 'oldField',
      })
      expect(expression.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle REMOVE multiple attributes', () => {
      const result = parseUpdate({
        field1: remove(),
        field2: remove(),
        field3: remove(),
      })

      expect(result.updateExpression).toBe('REMOVE #field1, #field2, #field3')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#field1': 'field1',
        '#field2': 'field2',
        '#field3': 'field3',
      })
      expect(expression.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle REMOVE with nested attribute paths', () => {
      const result = parseUpdate({
        'user.profile.email': remove(),
      })

      expect(result.updateExpression).toBe('REMOVE #user.#profile.#email')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#user': 'user',
        '#profile': 'profile',
        '#email': 'email',
      })
      expect(expression.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle REMOVE with array index notation', () => {
      const result = parseUpdate({
        'items[2]': remove(),
      })

      expect(result.updateExpression).toBe('REMOVE #items[2]')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#items': 'items',
      })
      expect(expression.ExpressionAttributeValues).toBeUndefined()
    })
  })

  describe('ADD operations', () => {
    it('should handle ADD with addToSet', () => {
      const result = parseUpdate({
        tags: addToSet(['tag1', 'tag2']),
      })

      expect(result.updateExpression).toBe('ADD #tags :v1')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#tags': 'tags',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': ['tag1', 'tag2'],
      })
    })

    it('should handle ADD with addToSet using reference', () => {
      const result = parseUpdate({
        tags: addToSet(ref('newTags')),
      })

      expect(result.updateExpression).toBe('ADD #tags #newTags')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#tags': 'tags',
        '#newTags': 'newTags',
      })
      expect(expression.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle ADD with multiple addToSet expressions', () => {
      const result = parseUpdate({
        tags: addToSet(['tag1']),
        categories: addToSet(['cat1']),
      })

      expect(result.updateExpression).toBe('ADD #tags :v1, #categories :v2')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#tags': 'tags',
        '#categories': 'categories',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': ['tag1'],
        ':v2': ['cat1'],
      })
    })
  })

  describe('DELETE operations', () => {
    it('should handle DELETE from set', () => {
      const result = parseUpdate({
        tags: removeFromSet(['tag1', 'tag2']),
      })

      expect(result.updateExpression).toBe('DELETE #tags :v1')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#tags': 'tags',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': ['tag1', 'tag2'],
      })
    })

    it('should handle DELETE using reference', () => {
      const result = parseUpdate({
        tags: removeFromSet(ref('tagsToRemove')),
      })

      expect(result.updateExpression).toBe('DELETE #tags #tagsToRemove')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#tags': 'tags',
        '#tagsToRemove': 'tagsToRemove',
      })
      expect(expression.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle DELETE with multiple attributes', () => {
      const result = parseUpdate({
        tags: removeFromSet(['tag1']),
        categories: removeFromSet(['cat1']),
      })

      expect(result.updateExpression).toBe('DELETE #tags :v1, #categories :v2')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#tags': 'tags',
        '#categories': 'categories',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': ['tag1'],
        ':v2': ['cat1'],
      })
    })
  })

  describe('Mixed operations', () => {
    it('should handle SET and REMOVE together', () => {
      const result = parseUpdate({
        name: 'John',
        oldField: remove(),
      })

      expect(result.updateExpression).toBe('SET #name = :v1 REMOVE #oldField')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#name': 'name',
        '#oldField': 'oldField',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': 'John',
      })
    })

    it('should handle SET, REMOVE, ADD, and DELETE together', () => {
      const result = parseUpdate({
        name: 'John',
        count: add(5),
        oldField: remove(),
        tags: addToSet(['tag1']),
        categories: removeFromSet(['cat1']),
      })

      expect(result.updateExpression).toBe(
        'SET #name = :v1, #count = #count + :v2 REMOVE #oldField ADD #tags :v3 DELETE #categories :v4',
      )
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#name': 'name',
        '#count': 'count',
        '#oldField': 'oldField',
        '#tags': 'tags',
        '#categories': 'categories',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': 'John',
        ':v2': 5,
        ':v3': ['tag1'],
        ':v4': ['cat1'],
      })
    })

    it('should handle multiple operations of each type', () => {
      const result = parseUpdate({
        name: 'John',
        age: 30,
        field1: remove(),
        field2: remove(),
        tags1: addToSet(['tag1']),
        tags2: addToSet(['tag2']),
        cats1: removeFromSet(['cat1']),
        cats2: removeFromSet(['cat2']),
      })

      expect(result.updateExpression).toBe(
        'SET #name = :v1, #age = :v2 REMOVE #field1, #field2 ADD #tags1 :v3, #tags2 :v4 DELETE #cats1 :v5, #cats2 :v6',
      )
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#name': 'name',
        '#age': 'age',
        '#field1': 'field1',
        '#field2': 'field2',
        '#tags1': 'tags1',
        '#tags2': 'tags2',
        '#cats1': 'cats1',
        '#cats2': 'cats2',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': 'John',
        ':v2': 30,
        ':v3': ['tag1'],
        ':v4': ['tag2'],
        ':v5': ['cat1'],
        ':v6': ['cat2'],
      })
    })
  })

  describe('Edge cases', () => {
    it('should throw error for empty update object', () => {
      expect(() => parseUpdate({})).toThrow(InvalidUpdateDocumentBuilderError)
      expect(() => parseUpdate({})).toThrow('Update expression cannot be empty')
    })

    it('should handle reserved keywords in attribute names', () => {
      const result = parseUpdate({
        name: 'John',
        status: 'active',
      })

      expect(result.updateExpression).toBe('SET #name = :v1, #status = :v2')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#name': 'name',
        '#status': 'status',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': 'John',
        ':v2': 'active',
      })
    })

    it('should handle attributes with special characters', () => {
      const result = parseUpdate({
        'user-name': 'John',
      })

      expect(result.updateExpression).toBe('SET #user-name = :v1')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#user-name': 'user-name',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': 'John',
      })
    })

    it('should handle single update value', () => {
      const result = parseUpdate({
        name: 'John',
      })

      expect(result.updateExpression).toBe('SET #name = :v1')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#name': 'name',
      })
      expect(expression.ExpressionAttributeValues).toEqual({
        ':v1': 'John',
      })
    })
  })

  describe('Internal defensive branches', () => {
    it('should handle malformed ADD expression without addToSet op', () => {
      // This tests a defensive branch that shouldn't normally occur with proper typing
      // $add type without $addToSet op should be ignored (doesn't add to any expression list)
      const result = parseUpdate({
        field: 'test',
        // @ts-expect-error - Testing defensive branch for malformed ADD expression
        malformed: { type: $add },
      })

      // Should not crash and only process the valid field
      expect(result.updateExpression).toBe('SET #field = :v1')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#field': 'field',
        '#malformed': 'malformed',
      })
    })

    it('should handle malformed SET expression without op property', () => {
      // This tests a defensive branch that shouldn't normally occur with proper typing
      // $set type without op property should be ignored (doesn't add to SET expression list)
      const result = parseUpdate({
        field: 'test',
        // @ts-expect-error - Testing defensive branch for malformed SET expression
        malformed: { type: $set },
      })

      // Should not crash and only process the valid field
      expect(result.updateExpression).toBe('SET #field = :v1')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#field': 'field',
        '#malformed': 'malformed',
      })
    })

    it('should handle $set expressions with unknown op values', () => {
      // This tests branches for $set expressions with unrecognized op values
      // $set with unknown op should be ignored (doesn't match any known op)
      const result = parseUpdate({
        field: 'test',
        // @ts-expect-error - Testing defensive branch for unknown op
        malformed: { type: $set, op: Symbol('unknown'), value: 5 },
      })

      // Should not crash and only process the valid field
      expect(result.updateExpression).toBe('SET #field = :v1')
      const expression = result.attributeExpressionMap.toDynamoAttributeExpression()
      expect(expression.ExpressionAttributeNames).toEqual({
        '#field': 'field',
        '#malformed': 'malformed',
      })
    })
  })
})
