import { describe, expect, it } from 'vitest'
import { parseUpdate, InvalidDynamoDBUpdateExpressionError } from '@/updates/update-parser'
import { add } from '@/updates/add'
import { subtract } from '@/updates/subtract'
import { append } from '@/updates/append'
import { prepend } from '@/updates/prepend'
import { addToSet } from '@/updates/add-to-set'
import { removeFromSet } from '@/updates/delete'
import { remove } from '@/updates/remove'
import { ref } from '@/updates/ref'

describe('parseUpdate', () => {
  describe('SET operations', () => {
    it('should handle simple SET with primitive value', () => {
      const result = parseUpdate({
        name: 'John',
        age: 30,
      })

      expect(result.UpdateExpression).toBe('SET #name = :name, #age = :age')
      expect(result.ExpressionAttributeNames).toEqual({
        '#name': 'name',
        '#age': 'age',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':name': 'John',
        ':age': 30,
      })
    })

    it('should handle SET with boolean values', () => {
      const result = parseUpdate({
        active: true,
        deleted: false,
      })

      expect(result.UpdateExpression).toBe('SET #active = :active, #deleted = :deleted')
      expect(result.ExpressionAttributeNames).toEqual({
        '#active': 'active',
        '#deleted': 'deleted',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':active': true,
        ':deleted': false,
      })
    })

    it('should handle SET with null value', () => {
      const result = parseUpdate({
        data: null,
      })

      expect(result.UpdateExpression).toBe('SET #data = :data')
      expect(result.ExpressionAttributeNames).toEqual({
        '#data': 'data',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':data': null,
      })
    })

    it('should handle SET with array value', () => {
      const result = parseUpdate({
        tags: ['tag1', 'tag2'],
      })

      expect(result.UpdateExpression).toBe('SET #tags = :tags')
      expect(result.ExpressionAttributeNames).toEqual({
        '#tags': 'tags',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':tags': ['tag1', 'tag2'],
      })
    })

    it('should handle SET with object value', () => {
      const result = parseUpdate({
        metadata: { key: 'value' },
      })

      expect(result.UpdateExpression).toBe('SET #metadata = :metadata')
      expect(result.ExpressionAttributeNames).toEqual({
        '#metadata': 'metadata',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':metadata': { key: 'value' },
      })
    })

    it('should handle SET with add expression', () => {
      const result = parseUpdate({
        count: add(5),
      })

      expect(result.UpdateExpression).toBe('SET #count = #count + :count')
      expect(result.ExpressionAttributeNames).toEqual({
        '#count': 'count',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':count': 5,
      })
    })

    it('should handle SET with subtract expression', () => {
      const result = parseUpdate({
        balance: subtract(100),
      })

      expect(result.UpdateExpression).toBe('SET #balance = #balance - :balance')
      expect(result.ExpressionAttributeNames).toEqual({
        '#balance': 'balance',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':balance': 100,
      })
    })

    it('should handle SET with append expression', () => {
      const result = parseUpdate({
        items: append(['item1', 'item2']),
      })

      expect(result.UpdateExpression).toBe('SET #items = list_append(#items, :items)')
      expect(result.ExpressionAttributeNames).toEqual({
        '#items': 'items',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':items': ['item1', 'item2'],
      })
    })

    it('should handle SET with prepend expression', () => {
      const result = parseUpdate({
        items: prepend(['item1', 'item2']),
      })

      expect(result.UpdateExpression).toBe('SET #items = list_append(:items, #items)')
      expect(result.ExpressionAttributeNames).toEqual({
        '#items': 'items',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':items': ['item1', 'item2'],
      })
    })

    it('should handle SET with nested attribute paths', () => {
      const result = parseUpdate({
        'user.profile.email': 'test@example.com',
      })

      expect(result.UpdateExpression).toBe('SET #user.#profile.#email = :user_profile_email')
      expect(result.ExpressionAttributeNames).toEqual({
        '#user': 'user',
        '#profile': 'profile',
        '#email': 'email',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':user_profile_email': 'test@example.com',
      })
    })

    it('should handle SET with array index notation', () => {
      const result = parseUpdate({
        'items[0]': 'first-item',
      })

      expect(result.UpdateExpression).toBe('SET #items[0] = :items_0')
      expect(result.ExpressionAttributeNames).toEqual({
        '#items': 'items',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':items_0': 'first-item',
      })
    })

    it('should handle SET with reference to another attribute', () => {
      const result = parseUpdate({
        newName: ref('oldName'),
      })

      expect(result.UpdateExpression).toBe('SET #newName = #oldName')
      expect(result.ExpressionAttributeNames).toEqual({
        '#newName': 'newName',
        '#oldName': 'oldName',
      })
      expect(result.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle SET with reference with default value', () => {
      const result = parseUpdate({
        counter: ref('oldCounter', 0),
      })

      expect(result.UpdateExpression).toBe(
        'SET #counter = if_not_exists(#oldCounter, :counter_default)',
      )
      expect(result.ExpressionAttributeNames).toEqual({
        '#counter': 'counter',
        '#oldCounter': 'oldCounter',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':counter_default': 0,
      })
    })

    it('should handle SET with add expression using reference', () => {
      const result = parseUpdate({
        count: add(ref('increment')),
      })

      expect(result.UpdateExpression).toBe('SET #count = #count + #increment')
      expect(result.ExpressionAttributeNames).toEqual({
        '#count': 'count',
        '#increment': 'increment',
      })
      expect(result.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle SET with subtract expression using reference', () => {
      const result = parseUpdate({
        balance: subtract(ref('deduction')),
      })

      expect(result.UpdateExpression).toBe('SET #balance = #balance - #deduction')
      expect(result.ExpressionAttributeNames).toEqual({
        '#balance': 'balance',
        '#deduction': 'deduction',
      })
      expect(result.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle SET with append expression using reference', () => {
      const result = parseUpdate({
        items: append(ref('newItems')),
      })

      expect(result.UpdateExpression).toBe('SET #items = list_append(#items, #newItems)')
      expect(result.ExpressionAttributeNames).toEqual({
        '#items': 'items',
        '#newItems': 'newItems',
      })
      expect(result.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle SET with prepend expression using reference', () => {
      const result = parseUpdate({
        items: prepend(ref('newItems')),
      })

      expect(result.UpdateExpression).toBe('SET #items = list_append(#newItems, #items)')
      expect(result.ExpressionAttributeNames).toEqual({
        '#items': 'items',
        '#newItems': 'newItems',
      })
      expect(result.ExpressionAttributeValues).toBeUndefined()
    })
  })

  describe('REMOVE operations', () => {
    it('should handle REMOVE single attribute', () => {
      const result = parseUpdate({
        oldField: remove(),
      })

      expect(result.UpdateExpression).toBe('REMOVE #oldField')
      expect(result.ExpressionAttributeNames).toEqual({
        '#oldField': 'oldField',
      })
      expect(result.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle REMOVE multiple attributes', () => {
      const result = parseUpdate({
        field1: remove(),
        field2: remove(),
        field3: remove(),
      })

      expect(result.UpdateExpression).toBe('REMOVE #field1, #field2, #field3')
      expect(result.ExpressionAttributeNames).toEqual({
        '#field1': 'field1',
        '#field2': 'field2',
        '#field3': 'field3',
      })
      expect(result.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle REMOVE with nested attribute paths', () => {
      const result = parseUpdate({
        'user.profile.email': remove(),
      })

      expect(result.UpdateExpression).toBe('REMOVE #user.#profile.#email')
      expect(result.ExpressionAttributeNames).toEqual({
        '#user': 'user',
        '#profile': 'profile',
        '#email': 'email',
      })
      expect(result.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle REMOVE with array index notation', () => {
      const result = parseUpdate({
        'items[2]': remove(),
      })

      expect(result.UpdateExpression).toBe('REMOVE #items[2]')
      expect(result.ExpressionAttributeNames).toEqual({
        '#items': 'items',
      })
      expect(result.ExpressionAttributeValues).toBeUndefined()
    })
  })

  describe('ADD operations', () => {
    it('should handle ADD with addToSet', () => {
      const result = parseUpdate({
        tags: addToSet(['tag1', 'tag2']),
      })

      expect(result.UpdateExpression).toBe('ADD #tags :tags')
      expect(result.ExpressionAttributeNames).toEqual({
        '#tags': 'tags',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':tags': ['tag1', 'tag2'],
      })
    })

    it('should handle ADD with addToSet using reference', () => {
      const result = parseUpdate({
        tags: addToSet(ref('newTags')),
      })

      expect(result.UpdateExpression).toBe('ADD #tags #newTags')
      expect(result.ExpressionAttributeNames).toEqual({
        '#tags': 'tags',
        '#newTags': 'newTags',
      })
      expect(result.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle ADD with multiple addToSet expressions', () => {
      const result = parseUpdate({
        tags: addToSet(['tag1']),
        categories: addToSet(['cat1']),
      })

      expect(result.UpdateExpression).toBe('ADD #tags :tags, #categories :categories')
      expect(result.ExpressionAttributeNames).toEqual({
        '#tags': 'tags',
        '#categories': 'categories',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':tags': ['tag1'],
        ':categories': ['cat1'],
      })
    })
  })

  describe('DELETE operations', () => {
    it('should handle DELETE from set', () => {
      const result = parseUpdate({
        tags: removeFromSet(['tag1', 'tag2']),
      })

      expect(result.UpdateExpression).toBe('DELETE #tags :tags')
      expect(result.ExpressionAttributeNames).toEqual({
        '#tags': 'tags',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':tags': ['tag1', 'tag2'],
      })
    })

    it('should handle DELETE using reference', () => {
      const result = parseUpdate({
        tags: removeFromSet(ref('tagsToRemove')),
      })

      expect(result.UpdateExpression).toBe('DELETE #tags #tagsToRemove')
      expect(result.ExpressionAttributeNames).toEqual({
        '#tags': 'tags',
        '#tagsToRemove': 'tagsToRemove',
      })
      expect(result.ExpressionAttributeValues).toBeUndefined()
    })

    it('should handle DELETE with multiple attributes', () => {
      const result = parseUpdate({
        tags: removeFromSet(['tag1']),
        categories: removeFromSet(['cat1']),
      })

      expect(result.UpdateExpression).toBe('DELETE #tags :tags, #categories :categories')
      expect(result.ExpressionAttributeNames).toEqual({
        '#tags': 'tags',
        '#categories': 'categories',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':tags': ['tag1'],
        ':categories': ['cat1'],
      })
    })
  })

  describe('Mixed operations', () => {
    it('should handle SET and REMOVE together', () => {
      const result = parseUpdate({
        name: 'John',
        oldField: remove(),
      })

      expect(result.UpdateExpression).toBe('SET #name = :name REMOVE #oldField')
      expect(result.ExpressionAttributeNames).toEqual({
        '#name': 'name',
        '#oldField': 'oldField',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':name': 'John',
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

      expect(result.UpdateExpression).toBe(
        'SET #name = :name, #count = #count + :count REMOVE #oldField ADD #tags :tags DELETE #categories :categories',
      )
      expect(result.ExpressionAttributeNames).toEqual({
        '#name': 'name',
        '#count': 'count',
        '#oldField': 'oldField',
        '#tags': 'tags',
        '#categories': 'categories',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':name': 'John',
        ':count': 5,
        ':tags': ['tag1'],
        ':categories': ['cat1'],
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

      expect(result.UpdateExpression).toBe(
        'SET #name = :name, #age = :age REMOVE #field1, #field2 ADD #tags1 :tags1, #tags2 :tags2 DELETE #cats1 :cats1, #cats2 :cats2',
      )
      expect(result.ExpressionAttributeNames).toEqual({
        '#name': 'name',
        '#age': 'age',
        '#field1': 'field1',
        '#field2': 'field2',
        '#tags1': 'tags1',
        '#tags2': 'tags2',
        '#cats1': 'cats1',
        '#cats2': 'cats2',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':name': 'John',
        ':age': 30,
        ':tags1': ['tag1'],
        ':tags2': ['tag2'],
        ':cats1': ['cat1'],
        ':cats2': ['cat2'],
      })
    })
  })

  describe('Edge cases', () => {
    it('should throw error for empty update object', () => {
      expect(() => parseUpdate({})).toThrow(InvalidDynamoDBUpdateExpressionError)
      expect(() => parseUpdate({})).toThrow('Update expression cannot be empty')
    })

    it('should handle reserved keywords in attribute names', () => {
      const result = parseUpdate({
        name: 'John',
        status: 'active',
      })

      expect(result.UpdateExpression).toBe('SET #name = :name, #status = :status')
      expect(result.ExpressionAttributeNames).toEqual({
        '#name': 'name',
        '#status': 'status',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':name': 'John',
        ':status': 'active',
      })
    })

    it('should handle attributes with special characters', () => {
      const result = parseUpdate({
        'user-name': 'John',
      })

      expect(result.UpdateExpression).toBe('SET #user_name = :user_name')
      expect(result.ExpressionAttributeNames).toEqual({
        '#user_name': 'user-name',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':user_name': 'John',
      })
    })

    it('should handle single update value', () => {
      const result = parseUpdate({
        name: 'John',
      })

      expect(result.UpdateExpression).toBe('SET #name = :name')
      expect(result.ExpressionAttributeNames).toEqual({
        '#name': 'name',
      })
      expect(result.ExpressionAttributeValues).toEqual({
        ':name': 'John',
      })
    })
  })
})
