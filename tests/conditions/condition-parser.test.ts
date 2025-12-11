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
      ConditionExpression: '#name = :v1',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': 'Test Item',
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
      ConditionExpression: '#name = :v1',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': 'Test Item',
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
      ConditionExpression: '#name = :v1 AND #id = :v2',
      ExpressionAttributeNames: {
        '#name': 'name',
        '#id': 'id',
      },
      ExpressionAttributeValues: {
        ':v1': 'Test Item',
        ':v2': '123',
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
      ConditionExpression: '#name = :v1 AND NOT (#id = :v2)',
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
      ConditionExpression: '#name = :v1 OR #name = :v2',
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
      ConditionExpression: 'NOT (#name = :v1)',
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
      ConditionExpression: '#id BETWEEN :v1 AND :v2',
      ExpressionAttributeNames: {
        '#id': 'id',
      },
      ExpressionAttributeValues: {
        ':v1': '100',
        ':v2': '200',
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
      ConditionExpression: '#name IN (:v1, :v2, :v3)',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': 'Item A',
        ':v2': 'Item B',
        ':v3': 'Item C',
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
      ConditionExpression: 'begins_with(#name, :v1)',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': 'Test',
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
      ConditionExpression: 'contains(#name, :v1)',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': 'Item',
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
      ConditionExpression: 'attribute_type(#name, :v1)',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': 'S',
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
      ConditionExpression: '#id > :v1 AND #name < :v2 AND #id <> :v3',
      ExpressionAttributeNames: {
        '#name': 'name',
        '#id': 'id',
      },
      ExpressionAttributeValues: {
        ':v1': '100',
        ':v2': 'ZZZ',
        ':v3': '999',
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
      ConditionExpression: '(#id = :v1 AND #name = :v2) OR (#id = :v3 AND #name = :v4)',
      ExpressionAttributeNames: {
        '#name': 'name',
        '#id': 'id',
      },
      ExpressionAttributeValues: {
        ':v1': '100',
        ':v2': 'Test',
        ':v3': '200',
        ':v4': 'Other',
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
      ConditionExpression: '#name = :v1 AND #id > :v2',
      ExpressionAttributeNames: {
        '#name': 'name',
        '#id': 'id',
      },
      ExpressionAttributeValues: {
        ':v1': 'Test',
        ':v2': '100',
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
      ConditionExpression: '#user.#profile.#email = :v1',
      ExpressionAttributeNames: {
        '#user': 'user',
        '#profile': 'profile',
        '#email': 'email',
      },
      ExpressionAttributeValues: {
        ':v1': 'test@example.com',
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
      ConditionExpression: '#id = :v1 AND #name = :v1',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': 'active',
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
      ConditionExpression: '#id = :v1 AND (#name = :v2 OR #name = :v3 OR #name = :v4)',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': '123',
        ':v2': 'A',
        ':v3': 'B',
        ':v4': 'C',
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
        '(#id = :v1 AND #name = :v2 AND #id > :v3) OR (#id = :v4 AND #name = :v5)',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': '100',
        ':v2': 'Alice',
        ':v3': '50',
        ':v4': '200',
        ':v5': 'Bob',
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
      ConditionExpression: 'NOT (#id = :v1 OR #name = :v2)',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': '100',
        ':v2': 'Test',
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
      ConditionExpression: 'NOT (#id = :v1) AND NOT (#name = :v2)',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': '100',
        ':v2': 'Test',
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
      ConditionExpression: '#name IN (:v1, :v2, size(#otherField))',
      ExpressionAttributeNames: {
        '#name': 'name',
        '#otherField': 'otherField',
      },
      ExpressionAttributeValues: {
        ':v1': 'value1',
        ':v2': 'value2',
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
      ConditionExpression: '#id > :v1 AND #id < :v2 AND #name = :v3',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': 100,
        ':v2': 200,
        ':v3': 42,
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
      ConditionExpression: '#id = :v1 AND #name = :v2',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': true,
        ':v2': false,
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
      ConditionExpression: '#name = :v1',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': null,
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
      ConditionExpression: '#id > :v1 AND #id < :v2 AND #id <> :v3',
      ExpressionAttributeNames: {
        '#id': 'id',
      },
      ExpressionAttributeValues: {
        ':v1': '100',
        ':v2': '999',
        ':v3': '500',
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
        'attribute_exists(#id) AND begins_with(#name, :v1) AND (#id BETWEEN :v2 AND :v3 OR #id IN (:v4, :v5, :v6)) AND NOT (contains(#name, :v7)) AND attribute_type(#id, :v8)',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': 'user_',
        ':v2': '100',
        ':v3': '200',
        ':v4': '999',
        ':v5': '888',
        ':v6': '777',
        ':v7': 'deleted',
        ':v8': 'S',
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
    expect(input.ConditionExpression).toContain('#id > :v1')
    expect(input.ConditionExpression).toContain('#name = :v2')
    expect(input.ConditionExpression).toContain('#timestamp < :v3')
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
      ConditionExpression: '#id = :v1 AND #name = :v2 AND #id > :v3',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': '100',
        ':v2': 'Test',
        ':v3': '50',
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
        '(#id = :v1 AND (#name = :v2 OR #name = :v3)) OR (#id = :v4 AND (#name = :v5 OR #name = :v6))',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': '1',
        ':v2': 'A',
        ':v3': 'B',
        ':v4': '2',
        ':v5': 'C',
        ':v6': 'D',
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
      ConditionExpression: '#name > size(#name) AND #id < :v1',
      ExpressionAttributeNames: {
        '#name': 'name',
        '#id': 'id',
      },
      ExpressionAttributeValues: {
        ':v1': 10,
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
    expect(input.ConditionExpression).toContain('#id = :v1')
    expect(input.ConditionExpression).toContain('#id <> :v2')
    expect(input.ConditionExpression).toContain('#id < :v3')
    expect(input.ConditionExpression).toContain('#id > :v4')
    expect(input.ExpressionAttributeValues?.[':v1']).toBe('A')
    expect(input.ExpressionAttributeValues?.[':v2']).toBe('B')
    expect(input.ExpressionAttributeValues?.[':v3']).toBe('C')
    expect(input.ExpressionAttributeValues?.[':v4']).toBe('E')
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
      ConditionExpression: '#name = :v1',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': '',
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
      ConditionExpression: '#name = :v1',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': ['tag1', 'tag2', 'tag3'],
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
      ConditionExpression: '#name = :v1',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': { nested: 'value', count: 42 },
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
      ConditionExpression: '#id BETWEEN :v1 AND :v2 AND #name BETWEEN :v3 AND :v4',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': '1',
        ':v2': '100',
        ':v3': 'A',
        ':v4': 'Z',
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
      ConditionExpression: '#id BETWEEN :v1 AND :v2 AND #name BETWEEN :v1 AND :v2',
      ExpressionAttributeNames: {
        '#id': 'id',
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': '100',
        ':v2': '200',
      },
    })
  })
})
