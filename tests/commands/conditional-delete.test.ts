import { beforeEach, describe, expect, it } from 'vitest'
import { DeleteCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import { ConditionalDelete } from '@/commands/conditional-delete'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoEntity } from '@/core/entity'
import { DynamoTable } from '@/core/table'
import { isoDatetime } from '@/codec/iso-datetime'
import { key } from '@/core/key'
import { mockClient } from 'aws-sdk-client-mock'
import { z } from 'zod/v4'
import { equals, greaterThan, exists, and, or, not } from '@/conditions'

describe('conditional delete command', () => {
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
      version: z.number().optional(),
    }),
    partitionKey: item => key('TEST', item.id),
    sortKey: item => item.timestamp.toISOString(),
  })

  it('deletes an item with a simple condition', async () => {
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

    const result = await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.oldItem).toBeUndefined()
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Key: {
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: '#name = :v1',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': 'Test Item',
      },
    })
  })

  it('deletes an item with an exists condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        version: exists(),
      },
    })

    const result = await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.oldItem).toBeUndefined()
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Key: {
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: 'attribute_exists(#version)',
      ExpressionAttributeNames: {
        '#version': 'version',
      },
    })
  })

  it('deletes an item with compound AND condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: and({ name: equals('Test Item') }, { version: greaterThan(5) }),
    })

    const result = await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.oldItem).toBeUndefined()
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Key: {
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: '#name = :v1 AND #version > :v2',
      ExpressionAttributeNames: {
        '#name': 'name',
        '#version': 'version',
      },
      ExpressionAttributeValues: {
        ':v1': 'Test Item',
        ':v2': 5,
      },
    })
  })

  it('deletes an item with compound OR condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: or({ name: equals('Test Item') }, { name: equals('Alt Item') }),
    })

    const result = await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Key: {
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: '#name = :v1 OR #name = :v2',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': 'Test Item',
        ':v2': 'Alt Item',
      },
    })
  })

  it('deletes an item with NOT condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: not({ name: equals('Deleted') }),
    })

    const result = await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Key: {
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: 'NOT (#name = :v1)',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': 'Deleted',
      },
    })
  })

  it('deletes an item and returns old values', async () => {
    dynamoMock.on(DeleteCommand).resolves({
      Attributes: {
        id: '123',
        name: 'Test Item',
        timestamp: '2000-01-01T00:00:00.000Z',
        version: 5,
      },
    })

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        version: equals(5),
      },
      returnValues: 'ALL_OLD',
    })

    const result = await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.oldItem).toEqual({
      id: '123',
      name: 'Test Item',
      timestamp: new Date('2000-01-01T00:00:00Z'),
      version: 5,
    })
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Key: {
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: '#version = :v1',
      ExpressionAttributeNames: {
        '#version': 'version',
      },
      ExpressionAttributeValues: {
        ':v1': 5,
      },
      ReturnValues: 'ALL_OLD',
    })
  })

  it('deletes an item and returns consumed capacity', async () => {
    dynamoMock.on(DeleteCommand).resolves({
      ConsumedCapacity: {
        TableName: 'TestTable',
        CapacityUnits: 1,
        WriteCapacityUnits: 1,
      },
    })

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: exists(),
      },
      returnConsumedCapacity: 'TOTAL',
    })

    const result = await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.consumedCapacity).toEqual({
      TableName: 'TestTable',
      CapacityUnits: 1,
      WriteCapacityUnits: 1,
    })
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Key: {
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: 'attribute_exists(#name)',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ReturnConsumedCapacity: 'TOTAL',
    })
  })

  it('deletes with returnValuesOnConditionCheckFailure', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        version: equals(10),
      },
      returnValuesOnConditionCheckFailure: 'ALL_OLD',
    })

    const result = await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Key: {
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: '#version = :v1',
      ExpressionAttributeNames: {
        '#version': 'version',
      },
      ExpressionAttributeValues: {
        ':v1': 10,
      },
      ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
    })
  })

  it('deletes with returnItemCollectionMetrics', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: exists(),
      },
      returnItemCollectionMetrics: 'SIZE',
    })

    const result = await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Key: {
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: 'attribute_exists(#name)',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ReturnItemCollectionMetrics: 'SIZE',
    })
  })

  it('returns response metadata', async () => {
    dynamoMock.on(DeleteCommand).resolves({
      $metadata: {},
    })

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: exists(),
      },
    })

    const result = await testEntity.send(deleteCommand)

    expect(result.responseMetadata).toBeDefined()
  })

  it('returns undefined oldItem when item does not exist', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '999',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: exists(),
      },
      returnValues: 'ALL_OLD',
    })

    const result = await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.oldItem).toBeUndefined()
  })
})
