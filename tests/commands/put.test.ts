import { beforeEach, describe, expect, it } from 'vitest'
import { PutCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import { Put } from '@/commands/put'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoEntity } from '@/core/entity'
import { DynamoTable } from '@/core/table'
import { isoDatetime } from '@/codec/iso-datetime'
import { key } from '@/core/key'
import { mockClient } from 'aws-sdk-client-mock'
import { z } from 'zod/v4'

describe('put command', () => {
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
      count: z.number().optional(),
    }),
    partitionKey: item => key('TEST', item.id),
    sortKey: item => item.timestamp.toISOString(),
  })

  it('puts an item', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new Put({
      item: {
        id: '123',
        name: 'Test Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.returnItem).toBeUndefined()
    expect(result.responseMetadata).toBeDefined()
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Item: {
        id: '123',
        name: 'Test Item',
        timestamp: '2000-01-01T00:00:00.000Z',
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
    })
  })

  it('puts an item with all attributes', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new Put({
      item: {
        id: '456',
        name: 'Complete Item',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        count: 42,
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
        count: 42,
        PK: 'TEST#456',
        SK: '2024-01-01T00:00:00.000Z',
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
        count: 10,
      },
    })

    const putCommand = new Put({
      item: {
        id: '123',
        name: 'Test Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      returnValues: 'ALL_OLD',
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.returnItem).toEqual({
      id: '123',
      name: 'Old Item',
      timestamp: new Date('2000-01-01T00:00:00Z'),
      count: 10,
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
      ReturnValues: 'ALL_OLD',
    })
  })

  it('returns consumed capacity when provided', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
      ConsumedCapacity: {
        TableName: 'TestTable',
        CapacityUnits: 2,
        WriteCapacityUnits: 2,
      },
    })

    const putCommand = new Put({
      item: {
        id: '789',
        name: 'Test Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.consumedCapacity).toEqual({
      TableName: 'TestTable',
      CapacityUnits: 2,
      WriteCapacityUnits: 2,
    })
  })

  it('returns response metadata', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new Put({
      item: {
        id: '123',
        name: 'Test Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
    })

    const result = await testEntity.send(putCommand)

    expect(result.responseMetadata).toBeDefined()
  })

  it('returns undefined item when old item does not exist', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new Put({
      item: {
        id: '999',
        name: 'New Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      returnValues: 'ALL_OLD',
    })

    const result = await testEntity.send(putCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.returnItem).toBeUndefined()
    expect(result.responseMetadata).toBeDefined()
  })

  it('puts an item with skipValidation', async () => {
    dynamoMock.on(PutCommand).resolves({
      $metadata: {},
    })

    const putCommand = new Put({
      item: {
        id: '123',
        name: 'Test Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
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
        timestamp: new Date('2000-01-01T00:00:00.000Z'),
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
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
        count: 10,
      },
    })

    const putCommand = new Put({
      item: {
        id: '123',
        name: 'Test Item',
        timestamp: new Date('2000-01-01T00:00:00Z'),
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
      count: 10,
    })
  })
})
