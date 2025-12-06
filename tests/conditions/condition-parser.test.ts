import { beforeEach, describe, expect, it } from 'vitest'
import {
  DeleteCommand,
  type DeleteCommandInput,
  DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb'
import { ConditionalDelete } from '@/commands/conditional-delete'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoEntity } from '@/core/entity'
import { DynamoTable } from '@/core/table'
import {
  and,
  beginsWith,
  between,
  contains,
  equals,
  exists,
  greaterThan,
  isIn,
  lessThan,
  not,
  notEquals,
  or,
  size,
  typeIs,
} from '@/conditions'
import { isoDatetime } from '@/codec'
import { $exists } from '@/conditions/condition-symbols'
import { key } from '@/core/key'
import { mockClient } from 'aws-sdk-client-mock'
import { z } from 'zod/v4'

describe('condition parser', () => {
  const dynamoClient = new DynamoDBClient()
  const dynamoMock = mockClient(DynamoDBDocumentClient)

  beforeEach(() => {
    dynamoMock.reset()
  })

  const testTable: DynamoTable = new DynamoTable({
    tableName: 'TestTable',
    documentClient: DynamoDBDocumentClient.from(dynamoClient),
  })

  const testEntity = new DynamoEntity({
    table: testTable,
    schema: z.object({
      id: z.string(),
      name: z.string(),
      timestamp: isoDatetime(),
    }),
    partitionKey: item => key('TEST', item.id),
    sortKey: item => item.timestamp.toISOString(),
  })

  it('parses a simple condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: equals('Test Item'),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name = :name',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':name': 'Test Item',
      },
    })
  })

  it('parses a simple value condition (implicit equals)', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: 'Test Item',
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name = :name',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':name': 'Test Item',
      },
    })
  })

  it('parses multiple conditions (implicit and)', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: 'Test Item',
        id: '123',
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name = :name AND #id = :id',
      ExpressionAttributeNames: {
        '#name': 'name',
        '#id': 'id',
      },
      ExpressionAttributeValues: {
        ':name': 'Test Item',
        ':id': '123',
      },
    })
  })

  it('parses AND condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: and({ name: equals('Test Item') }, not({ id: equals('123') })),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name = :name AND NOT (#id = :id)',
    })
  })

  it('parses OR condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: or({ name: equals('Item A') }, { name: equals('Item B') }),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name = :name OR #name = :name1',
    })
  })

  it('parses NOT condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: not({ name: equals('Test Item') }),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: 'NOT (#name = :name)',
    })
  })

  it('parses BETWEEN condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        id: between('100', '200'),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#id BETWEEN :id AND :id1',
      ExpressionAttributeNames: {
        '#id': 'id',
      },
      ExpressionAttributeValues: {
        ':id': '100',
        ':id1': '200',
      },
    })
  })

  it('parses IN condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: isIn('Item A', 'Item B', 'Item C'),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name IN (:name, :name1, :name2)',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':name': 'Item A',
        ':name1': 'Item B',
        ':name2': 'Item C',
      },
    })
  })

  it('parses attribute_exists condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: exists(),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: 'attribute_exists(#name)',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
    })
  })

  it('parses begins_with condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: beginsWith('Test'),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: 'begins_with(#name, :name)',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':name': 'Test',
      },
    })
  })

  it('parses contains condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: contains('Item'),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: 'contains(#name, :name)',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':name': 'Item',
      },
    })
  })

  it('parses attribute_type condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: typeIs('S'),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: 'attribute_type(#name, :name)',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':name': 'S',
      },
    })
  })

  it('parses size() in comparison', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: greaterThan(size('name')),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name > size(#name)',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
    })
    expect(dynamoMock.call(0).args[0].input).not.toHaveProperty('ExpressionAttributeValues')
  })

  it('parses comparison operators', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: and(
        { id: greaterThan('100') },
        { name: lessThan('ZZZ') },
        { id: notEquals('999') },
      ),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#id > :id AND #name < :name AND #id <> :id1',
      ExpressionAttributeNames: {
        '#name': 'name',
        '#id': 'id',
      },
      ExpressionAttributeValues: {
        ':name': 'ZZZ',
        ':id': '100',
        ':id1': '999',
      },
    })
  })

  it('parses nested logical conditions', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: or(
        and({ id: equals('100') }, { name: equals('Test') }),
        and({ id: equals('200') }, { name: equals('Other') }),
      ),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '(#id = :id AND #name = :name) OR (#id = :id1 AND #name = :name1)',
      ExpressionAttributeNames: {
        '#name': 'name',
        '#id': 'id',
      },
      ExpressionAttributeValues: {
        ':name': 'Test',
        ':id': '100',
        ':name1': 'Other',
        ':id1': '200',
      },
    })
  })

  it('parses condition array (implicit AND)', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: [{ name: equals('Test') }, { id: greaterThan('100') }],
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name = :name AND #id > :id',
      ExpressionAttributeNames: {
        '#name': 'name',
        '#id': 'id',
      },
      ExpressionAttributeValues: {
        ':name': 'Test',
        ':id': '100',
      },
    })
  })

  it('handles nested attribute paths with dot notation', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        'user.profile.email': equals('test@example.com'),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#user.#profile.#email = :email',
      ExpressionAttributeNames: {
        '#user': 'user',
        '#profile': 'profile',
        '#email': 'email',
      },
      ExpressionAttributeValues: {
        ':email': 'test@example.com',
      },
    })
  })

  it('reuses value placeholders for same values across different attributes', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: and({ id: equals('active') }, { name: equals('active') }),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#id = :id AND #name = :name',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':id': 'active',
        ':name': 'active',
      },
    })
  })

  it('handles deeply nested OR within AND conditions', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: and(
        { id: equals('123') },
        or({ name: equals('A') }, { name: equals('B') }, { name: equals('C') }),
      ),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#id = :id AND (#name = :name OR #name = :name1 OR #name = :name2)',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':id': '123',
        ':name': 'A',
        ':name1': 'B',
        ':name2': 'C',
      },
    })
  })

  it('handles deeply nested AND within OR conditions', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: or(
        and({ id: equals('100') }, { name: equals('Alice') }, { id: greaterThan('50') }),
        and({ id: equals('200') }, { name: equals('Bob') }),
      ),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression:
        '(#id = :id AND #name = :name AND #id > :id1) OR (#id = :id2 AND #name = :name1)',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':id': '100',
        ':name': 'Alice',
        ':id1': '50',
        ':id2': '200',
        ':name1': 'Bob',
      },
    })
  })

  it('handles NOT with complex nested conditions', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: not(or({ id: equals('100') }, { name: equals('Test') })),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: 'NOT (#id = :id OR #name = :name)',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':id': '100',
        ':name': 'Test',
      },
    })
  })

  it('handles multiple NOT conditions', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: and(not({ id: equals('100') }), not({ name: equals('Test') })),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: 'NOT (#id = :id) AND NOT (#name = :name)',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':id': '100',
        ':name': 'Test',
      },
    })
  })

  it('handles BETWEEN with size() expressions', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: between(size('minLength'), size('maxLength')),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name BETWEEN size(#minLength) AND size(#maxLength)',
      ExpressionAttributeNames: {
        '#name': 'name',
        '#minLength': 'minLength',
        '#maxLength': 'maxLength',
      },
    })
    expect(dynamoMock.call(0).args[0].input).not.toHaveProperty('ExpressionAttributeValues')
  })

  it('handles IN with mixed values and size expressions', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: isIn('value1', 'value2', size('otherField')),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name IN (:name, :name1, size(#otherField))',
      ExpressionAttributeNames: {
        '#name': 'name',
        '#otherField': 'otherField',
      },
      ExpressionAttributeValues: {
        ':name': 'value1',
        ':name1': 'value2',
      },
    })
  })

  it('handles comparison with numeric values', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: and({ id: greaterThan(100) }, { id: lessThan(200) }, { name: equals(42) }),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#id > :id AND #id < :id1 AND #name = :name',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':id': 100,
        ':id1': 200,
        ':name': 42,
      },
    })
  })

  it('handles comparison with boolean values', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: and({ id: equals(true) }, { name: equals(false) }),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#id = :id AND #name = :name',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':id': true,
        ':name': false,
      },
    })
  })

  it('handles comparison with null values', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: equals(null),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name = :name',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':name': null,
      },
    })
  })

  it('handles multiple conditions with same attribute and different operators', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: and({ id: greaterThan('100') }, { id: lessThan('999') }, { id: notEquals('500') }),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#id > :id AND #id < :id1 AND #id <> :id2',
      ExpressionAttributeNames: {
        '#id': 'id',
      },
      ExpressionAttributeValues: {
        ':id': '100',
        ':id1': '999',
        ':id2': '500',
      },
    })
  })

  it('handles complex condition with all expression types', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: and(
        { id: exists() },
        { name: beginsWith('user_') },
        or({ id: between('100', '200') }, { id: isIn('999', '888', '777') }),
        not({ name: contains('deleted') }),
        { id: typeIs('S') },
      ),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression:
        'attribute_exists(#id) AND begins_with(#name, :name) AND (#id BETWEEN :id AND :id1 OR #id IN (:id2, :id3, :id4)) AND NOT (contains(#name, :name1)) AND attribute_type(#id, :id5)',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':name': 'user_',
        ':id': '100',
        ':id1': '200',
        ':id2': '999',
        ':id3': '888',
        ':id4': '777',
        ':name1': 'deleted',
        ':id5': 'S',
      },
    })
  })

  it('handles template with mixed simple values and complex expressions', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        id: greaterThan('100'),
        name: 'TestName',
        timestamp: lessThan(new Date('2024-01-01')),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    const input = dynamoMock.call(0).args[0].input as DeleteCommandInput
    expect(input).toHaveProperty('ConditionExpression')
    expect(input.ConditionExpression).toContain('#id > :id')
    expect(input.ConditionExpression).toContain('#name = :name')
    expect(input.ConditionExpression).toContain('#timestamp < :timestamp')
    expect(input.ConditionExpression).toContain(' AND ')
  })

  it('handles array of templates', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: [{ id: equals('100') }, { name: equals('Test') }, { id: greaterThan('50') }],
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#id = :id AND #name = :name AND #id > :id1',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':id': '100',
        ':name': 'Test',
        ':id1': '50',
      },
    })
  })

  it('handles triple nested logical conditions', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: or(
        and({ id: equals('1') }, or({ name: equals('A') }, { name: equals('B') })),
        and({ id: equals('2') }, or({ name: equals('C') }, { name: equals('D') })),
      ),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression:
        '(#id = :id AND (#name = :name OR #name = :name1)) OR (#id = :id1 AND (#name = :name2 OR #name = :name3))',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':id': '1',
        ':name': 'A',
        ':name1': 'B',
        ':id1': '2',
        ':name2': 'C',
        ':name3': 'D',
      },
    })
  })

  it('handles size comparison with numeric values', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: and({ name: greaterThan(size('name')) }, { id: lessThan(10) }),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name > size(#name) AND #id < :id',
      ExpressionAttributeNames: {
        '#name': 'name',
        '#id': 'id',
      },
      ExpressionAttributeValues: {
        ':id': 10,
      },
    })
  })

  it('handles attribute_not_exists', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: { type: $exists, not: true },
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: 'attribute_not_exists(#name)',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
    })
    expect(dynamoMock.call(0).args[0].input).not.toHaveProperty('ExpressionAttributeValues')
  })

  it('handles all comparison operators on same attribute', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: or(
        { id: equals('A') },
        { id: notEquals('B') },
        { id: lessThan('C') },
        { id: greaterThan('E') },
      ),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    const input = dynamoMock.call(0).args[0].input as DeleteCommandInput
    expect(input.ConditionExpression).toContain('#id = :id')
    expect(input.ConditionExpression).toContain('#id <> :id1')
    expect(input.ConditionExpression).toContain('#id < :id2')
    expect(input.ConditionExpression).toContain('#id > :id3')
    expect(input.ExpressionAttributeValues?.[':id']).toBe('A')
    expect(input.ExpressionAttributeValues?.[':id1']).toBe('B')
    expect(input.ExpressionAttributeValues?.[':id2']).toBe('C')
    expect(input.ExpressionAttributeValues?.[':id3']).toBe('E')
  })

  it('handles empty string values', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: equals(''),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name = :name',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':name': '',
      },
    })
  })

  it('handles array values in conditions', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: equals(['tag1', 'tag2', 'tag3']),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name = :name',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':name': ['tag1', 'tag2', 'tag3'],
      },
    })
  })

  it('handles object values in conditions', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: equals({ nested: 'value', count: 42 }),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name = :name',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':name': { nested: 'value', count: 42 },
      },
    })
  })

  it('handles multiple BETWEEN conditions on different attributes', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: and({ id: between('1', '100') }, { name: between('A', 'Z') }),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#id BETWEEN :id AND :id1 AND #name BETWEEN :name AND :name1',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':id': '1',
        ':id1': '100',
        ':name': 'A',
        ':name1': 'Z',
      },
    })
  })

  it('handles value reuse in BETWEEN with same bounds', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: and({ id: between('100', '200') }, { name: between('100', '200') }),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#id BETWEEN :id AND :id1 AND #name BETWEEN :name AND :name1',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':id': '100',
        ':id1': '200',
        ':name': '100',
        ':name1': '200',
      },
    })
  })
})
