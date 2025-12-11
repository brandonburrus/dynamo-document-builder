import { beforeEach, describe, expect, it } from 'vitest'
import { PutCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import { ConditionalPut } from '@/commands/conditional-put'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoEntity } from '@/core/entity'
import { DynamoTable } from '@/core/table'
import { isoDatetime } from '@/codec/iso-datetime'
import { key } from '@/core/key'
import { mockClient } from 'aws-sdk-client-mock'
import { z } from 'zod/v4'
import { equals, greaterThan, exists, notExists, and, or, not } from '@/conditions'

describe('conditional put command', () => {
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
      status: z.string().optional(),
    }),
    partitionKey: item => key('TEST', item.id),
    sortKey: item => item.timestamp.toISOString(),
  })

  it('puts an item with a simple condition', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new ConditionalPut({
      item: {
        id: '123',
        name: 'Test Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: equals('Old Item'),
      },
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.returnItem).toBeUndefined()
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Item: {
        id: '123',
        name: 'Test Item',
        timestamp: '2000-01-01T00:00:00.000Z',
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: '#name = :v1',
      ExpressionAttributeNames: {
        '#name': 'name',
      },
      ExpressionAttributeValues: {
        ':v1': 'Old Item',
      },
    })
  })

  it('puts an item with attribute_not_exists condition', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new ConditionalPut({
      item: {
        id: '123',
        name: 'New Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        id: notExists(),
      },
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.returnItem).toBeUndefined()
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Item: {
        id: '123',
        name: 'New Item',
        timestamp: '2000-01-01T00:00:00.000Z',
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: 'attribute_not_exists(#id)',
      ExpressionAttributeNames: {
        '#id': 'id',
      },
    })
  })

  it('puts an item with attribute_exists condition', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new ConditionalPut({
      item: {
        id: '123',
        name: 'Updated Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        version: exists(),
      },
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.returnItem).toBeUndefined()
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Item: {
        id: '123',
        name: 'Updated Item',
        timestamp: '2000-01-01T00:00:00.000Z',
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: 'attribute_exists(#version)',
      ExpressionAttributeNames: {
        '#version': 'version',
      },
    })
  })

  it('puts an item with compound AND condition', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new ConditionalPut({
      item: {
        id: '123',
        name: 'Updated Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
        version: 2,
      },
      condition: and({ version: equals(1) }, { status: equals('active') }),
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.returnItem).toBeUndefined()
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Item: {
        id: '123',
        name: 'Updated Item',
        timestamp: '2000-01-01T00:00:00.000Z',
        version: 2,
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: '#version = :v1 AND #status = :v2',
      ExpressionAttributeNames: {
        '#version': 'version',
        '#status': 'status',
      },
      ExpressionAttributeValues: {
        ':v1': 1,
        ':v2': 'active',
      },
    })
  })

  it('puts an item with compound OR condition', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new ConditionalPut({
      item: {
        id: '123',
        name: 'Test Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: or({ status: equals('pending') }, { status: equals('draft') }),
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Item: {
        id: '123',
        name: 'Test Item',
        timestamp: '2000-01-01T00:00:00.000Z',
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: '#status = :v1 OR #status = :v2',
      ExpressionAttributeNames: {
        '#status': 'status',
      },
      ExpressionAttributeValues: {
        ':v1': 'pending',
        ':v2': 'draft',
      },
    })
  })

  it('puts an item with NOT condition', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new ConditionalPut({
      item: {
        id: '123',
        name: 'Test Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: not({ status: equals('deleted') }),
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Item: {
        id: '123',
        name: 'Test Item',
        timestamp: '2000-01-01T00:00:00.000Z',
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: 'NOT (#status = :v1)',
      ExpressionAttributeNames: {
        '#status': 'status',
      },
      ExpressionAttributeValues: {
        ':v1': 'deleted',
      },
    })
  })

  it('puts an item with version check using greaterThan', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new ConditionalPut({
      item: {
        id: '123',
        name: 'Test Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
        version: 10,
      },
      condition: {
        version: greaterThan(5),
      },
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Item: {
        id: '123',
        name: 'Test Item',
        timestamp: '2000-01-01T00:00:00.000Z',
        version: 10,
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: '#version > :v1',
      ExpressionAttributeNames: {
        '#version': 'version',
      },
      ExpressionAttributeValues: {
        ':v1': 5,
      },
    })
  })

  it('puts an item and returns old values', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
      Attributes: {
        id: '123',
        name: 'Old Item',
        timestamp: '2000-01-01T00:00:00.000Z',
        version: 1,
      },
    })

    const putCommand = new ConditionalPut({
      item: {
        id: '123',
        name: 'New Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
        version: 2,
      },
      condition: {
        version: equals(1),
      },
      returnValues: 'ALL_OLD',
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.returnItem).toEqual({
      id: '123',
      name: 'Old Item',
      timestamp: new Date('2000-01-01T00:00:00Z'),
      version: 1,
    })
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Item: {
        id: '123',
        name: 'New Item',
        timestamp: '2000-01-01T00:00:00.000Z',
        version: 2,
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: '#version = :v1',
      ExpressionAttributeNames: {
        '#version': 'version',
      },
      ExpressionAttributeValues: {
        ':v1': 1,
      },
      ReturnValues: 'ALL_OLD',
    })
  })

  it('puts an item and returns consumed capacity', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
      ConsumedCapacity: {
        TableName: 'TestTable',
        CapacityUnits: 2,
        WriteCapacityUnits: 2,
      },
    })

    const putCommand = new ConditionalPut({
      item: {
        id: '123',
        name: 'Test Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        id: notExists(),
      },
      returnConsumedCapacity: 'TOTAL',
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.consumedCapacity).toEqual({
      TableName: 'TestTable',
      CapacityUnits: 2,
      WriteCapacityUnits: 2,
    })
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Item: {
        id: '123',
        name: 'Test Item',
        timestamp: '2000-01-01T00:00:00.000Z',
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: 'attribute_not_exists(#id)',
      ExpressionAttributeNames: {
        '#id': 'id',
      },
      ReturnConsumedCapacity: 'TOTAL',
    })
  })

  it('puts with returnValuesOnConditionCheckFailure', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new ConditionalPut({
      item: {
        id: '123',
        name: 'Test Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        version: equals(10),
      },
      returnValuesOnConditionCheckFailure: 'ALL_OLD',
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Item: {
        id: '123',
        name: 'Test Item',
        timestamp: '2000-01-01T00:00:00.000Z',
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

  it('puts with returnItemCollectionMetrics', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new ConditionalPut({
      item: {
        id: '123',
        name: 'Test Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: exists(),
      },
      returnItemCollectionMetrics: 'SIZE',
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Item: {
        id: '123',
        name: 'Test Item',
        timestamp: '2000-01-01T00:00:00.000Z',
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
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new ConditionalPut({
      item: {
        id: '123',
        name: 'Test Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: exists(),
      },
    })

    const result = await testEntity.send(putCommand)

    expect(result.responseMetadata).toBeDefined()
  })

  it('returns undefined item when no old values exist', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new ConditionalPut({
      item: {
        id: '999',
        name: 'New Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        id: notExists(),
      },
      returnOldItem: 'ALL_OLD',
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.returnItem).toBeUndefined()
  })

  it('puts an item with all optional fields', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new ConditionalPut({
      item: {
        id: '456',
        name: 'Complete Item',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        version: 1,
        status: 'active',
      },
      condition: {
        id: notExists(),
      },
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.returnItem).toBeUndefined()
    expect(result.responseMetadata).toBeDefined()
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Item: {
        id: '456',
        name: 'Complete Item',
        timestamp: '2024-01-01T00:00:00.000Z',
        version: 1,
        status: 'active',
        PK: 'TEST#456',
        SK: '2024-01-01T00:00:00.000Z',
      },
      ConditionExpression: 'attribute_not_exists(#id)',
      ExpressionAttributeNames: {
        '#id': 'id',
      },
    })
  })

  it('puts an item with skipValidation', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new ConditionalPut({
      item: {
        id: '123',
        name: 'Test Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        id: notExists(),
      },
      skipValidation: true,
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.returnItem).toBeUndefined()
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Item: {
        id: '123',
        name: 'Test Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConditionExpression: 'attribute_not_exists(#id)',
      ExpressionAttributeNames: {
        '#id': 'id',
      },
    })
  })

  it('puts an item with skipValidation and returns old values', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
      Attributes: {
        id: '123',
        name: 'Old Item',
        timestamp: '2000-01-01T00:00:00.000Z',
        version: 1,
      },
    })

    const putCommand = new ConditionalPut({
      item: {
        id: '123',
        name: 'New Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
        version: 2,
      },
      condition: {
        version: equals(1),
      },
      returnValues: 'ALL_OLD',
      skipValidation: true,
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.returnItem).toEqual({
      id: '123',
      name: 'Old Item',
      timestamp: '2000-01-01T00:00:00.000Z',
      version: 1,
    })
  })
})
