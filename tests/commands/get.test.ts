import { beforeEach, describe, expect, it } from 'vitest'
import { GetCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import { Get } from '@/commands/get'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoEntity } from '@/core/entity'
import { DynamoTable } from '@/core/table'
import { isoDatetime } from '@/codec/iso-datetime'
import { key } from '@/core/key'
import { mockClient } from 'aws-sdk-client-mock'
import { z } from 'zod/v4'

describe('get command', () => {
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

  it('gets an item', async () => {
    dynamoMock.on(GetCommand).resolves({
      Item: {
        id: '123',
        name: 'Test Item',
        timestamp: '2000-01-01T00:00:00.000Z',
      },
    })

    const getCommand = new Get({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
    })

    const result = await testEntity.send(getCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.item).toEqual({
      id: '123',
      name: 'Test Item',
      timestamp: new Date('2000-01-01T00:00:00Z'),
    })
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Key: {
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConsistentRead: false,
    })
  })

  it('returns undefined when item does not exist', async () => {
    dynamoMock.on(GetCommand).resolves({
      $metadata: {},
    })

    const getCommand = new Get({
      key: {
        id: '999',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
    })

    const result = await testEntity.send(getCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.item).toBeUndefined()
    expect(result.responseMetadata).toBeDefined()
  })

  it('gets an item with consistent read', async () => {
    dynamoMock.on(GetCommand).resolves({
      Item: {
        id: '123',
        name: 'Test Item',
        timestamp: '2000-01-01T00:00:00.000Z',
      },
    })

    const getCommand = new Get({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      consistentRead: true,
    })

    const result = await testEntity.send(getCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.item).toBeDefined()
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Key: {
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ConsistentRead: true,
    })
  })

  it('returns consumed capacity when provided', async () => {
    dynamoMock.on(GetCommand).resolves({
      Item: {
        id: '123',
        name: 'Test Item',
        timestamp: '2000-01-01T00:00:00.000Z',
      },
      ConsumedCapacity: {
        TableName: 'TestTable',
        CapacityUnits: 1,
        ReadCapacityUnits: 1,
      },
    })

    const getCommand = new Get({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
    })

    const result = await testEntity.send(getCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.consumedCapacity).toEqual({
      TableName: 'TestTable',
      CapacityUnits: 1,
      ReadCapacityUnits: 1,
    })
  })

  it('returns response metadata', async () => {
    dynamoMock.on(GetCommand).resolves({
      Item: {
        id: '123',
        name: 'Test Item',
        timestamp: '2000-01-01T00:00:00.000Z',
      },
      $metadata: {},
    })

    const getCommand = new Get({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
    })

    const result = await testEntity.send(getCommand)

    expect(result.responseMetadata).toBeDefined()
  })

  it('gets an item with skipValidation', async () => {
    dynamoMock.on(GetCommand).resolves({
      Item: {
        id: '123',
        name: 'Test Item',
        timestamp: '2000-01-01T00:00:00.000Z',
      },
    })

    const getCommand = new Get({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      skipValidation: true,
    })

    const result = await testEntity.send(getCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.item).toEqual({
      id: '123',
      name: 'Test Item',
      timestamp: '2000-01-01T00:00:00.000Z',
    })
  })
})
