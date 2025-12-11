import { beforeEach, describe, expect, it } from 'vitest'
import { QueryCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import { Query } from '@/commands/query'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoEntity } from '@/core/entity'
import { DynamoTable } from '@/core/table'
import { isoDatetime } from '@/codec/iso-datetime'
import { key } from '@/core/key'
import { mockClient } from 'aws-sdk-client-mock'
import { z } from 'zod/v4'
import { and, beginsWith, between, eq, gt, gte, lt, lte } from '@/conditions'

describe('query command', () => {
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
      userId: z.string(),
      timestamp: isoDatetime(),
      name: z.string(),
      count: z.number().optional(),
    }),
    partitionKey: item => key('USER', item.userId),
    sortKey: item => item.timestamp.toISOString(),
  })

  it('queries items with key condition', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          userId: '123',
          timestamp: '2000-01-01T00:00:00.000Z',
          name: 'Item 1',
        },
        {
          userId: '123',
          timestamp: '2000-01-02T00:00:00.000Z',
          name: 'Item 2',
        },
      ],
      Count: 2,
      ScannedCount: 2,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
    })

    const result = await testEntity.send(queryCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.items).toHaveLength(2)
    expect(result.items[0]).toEqual({
      userId: '123',
      timestamp: new Date('2000-01-01T00:00:00.000Z'),
      name: 'Item 1',
    })
    expect(result.items[1]).toEqual({
      userId: '123',
      timestamp: new Date('2000-01-02T00:00:00.000Z'),
      name: 'Item 2',
    })
    expect(result.count).toBe(2)
    expect(result.scannedCount).toBe(2)

    const input = dynamoMock.call(0).args[0].input
    expect(input).toMatchObject({
      TableName: 'TestTable',
      ConsistentRead: false,
      ScanIndexForward: true,
    })
    expect(input.KeyConditionExpression).toBeDefined()
    expect(input.ExpressionAttributeNames).toBeDefined()
    expect(input.ExpressionAttributeValues).toBeDefined()
  })

  it('queries items with key condition and begins_with on sort key', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          userId: '456',
          timestamp: '2024-01-01T00:00:00.000Z',
          name: 'Item 1',
        },
      ],
      Count: 1,
      ScannedCount: 1,
    })

    const queryCommand = new Query({
      keyCondition: and({ userId: eq('456') }, { timestamp: beginsWith('2024-01') }),
    })

    const result = await testEntity.send(queryCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.items).toHaveLength(1)
    expect(result.items[0]).toEqual({
      userId: '456',
      timestamp: new Date('2024-01-01T00:00:00.000Z'),
      name: 'Item 1',
    })

    const input = dynamoMock.call(0).args[0].input
    expect(input.KeyConditionExpression).toBeDefined()
    expect(input.KeyConditionExpression).toContain('begins_with')
  })

  it('queries items with key condition using comparison operators', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
    })

    const queryCommand = new Query({
      keyCondition: and(
        { userId: eq('789') },
        { timestamp: gte(new Date('2024-01-01T00:00:00.000Z')) },
      ),
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input.KeyConditionExpression).toBeDefined()
    expect(input.ExpressionAttributeValues).toBeDefined()
  })

  it('queries items with key condition using between', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
    })

    const queryCommand = new Query({
      keyCondition: and(
        { userId: '123' },
        { timestamp: between(new Date('2024-01-01'), new Date('2024-12-31')) },
      ),
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input.KeyConditionExpression).toBeDefined()
    expect(input.KeyConditionExpression).toContain('BETWEEN')
  })

  it('queries items with filter expression', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          userId: '123',
          timestamp: '2000-01-01T00:00:00.000Z',
          name: 'Active Item',
          count: 10,
        },
      ],
      Count: 1,
      ScannedCount: 5,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
      filter: { count: gt(5) },
    })

    const result = await testEntity.send(queryCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.items).toHaveLength(1)
    expect(result.count).toBe(1)
    expect(result.scannedCount).toBe(5)

    const input = dynamoMock.call(0).args[0].input
    expect(input.FilterExpression).toBeDefined()
    expect(input.KeyConditionExpression).toBeDefined()
  })

  it('queries items with projection', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          userId: '123',
          name: 'Item 1',
        },
      ],
      Count: 1,
      ScannedCount: 1,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
      projection: ['userId', 'name'],
      skipValidation: true,
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input.ProjectionExpression).toBe('userId, name')
  })

  it('queries items with select option', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
      select: 'ALL_ATTRIBUTES',
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input.Select).toBe('ALL_ATTRIBUTES')
  })

  it('queries items with limit', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          userId: '123',
          timestamp: '2000-01-01T00:00:00.000Z',
          name: 'Item 1',
        },
      ],
      Count: 1,
      ScannedCount: 1,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
      limit: 10,
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input.Limit).toBe(10)
  })

  it('queries items with consistent read', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
      consistentRead: true,
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input.ConsistentRead).toBe(true)
  })

  it('queries items with consistent read false by default', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input.ConsistentRead).toBe(false)
  })

  it('queries items on an index', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
      queryIndex: 'UserNameIndex',
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input.IndexName).toBe('UserNameIndex')
  })

  it('queries items with reverse index scan', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
      reverseIndexScan: true,
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input.ScanIndexForward).toBe(false)
  })

  it('queries items with forward index scan by default', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input.ScanIndexForward).toBe(true)
  })

  it('queries items with exclusive start key', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
    })

    const exclusiveStartKey = {
      userId: '123',
      timestamp: new Date('2000-01-01T00:00:00.000Z'),
    }

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
      exclusiveStartKey,
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input.ExclusiveStartKey).toEqual(exclusiveStartKey)
  })

  it('returns last evaluated key for pagination', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          userId: '123',
          timestamp: '2000-01-01T00:00:00.000Z',
          name: 'Item 1',
        },
      ],
      Count: 1,
      ScannedCount: 1,
      LastEvaluatedKey: {
        PK: 'USER#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
      limit: 1,
    })

    const result = await testEntity.send(queryCommand)

    expect(result.lastEvaluatedKey).toBeDefined()
    expect(result.lastEvaluatedKey).toEqual({
      PK: 'USER#123',
      SK: '2000-01-01T00:00:00.000Z',
    })
  })

  it('returns consumed capacity when requested', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
      ConsumedCapacity: {
        TableName: 'TestTable',
        CapacityUnits: 1,
        ReadCapacityUnits: 1,
      },
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
      returnConsumedCapacity: 'TOTAL',
    })

    const result = await testEntity.send(queryCommand)

    expect(result.consumedCapacity).toEqual({
      TableName: 'TestTable',
      CapacityUnits: 1,
      ReadCapacityUnits: 1,
    })

    const input = dynamoMock.call(0).args[0].input
    expect(input.ReturnConsumedCapacity).toBe('TOTAL')
  })

  it('returns response metadata', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
      $metadata: {},
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
    })

    const result = await testEntity.send(queryCommand)

    expect(result.responseMetadata).toBeDefined()
  })

  it('returns empty array when no items found', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Count: 0,
      ScannedCount: 0,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '999' },
    })

    const result = await testEntity.send(queryCommand)

    expect(result.items).toEqual([])
    expect(result.count).toBe(0)
    expect(result.scannedCount).toBe(0)
  })

  it('queries items with skipValidation', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          userId: '123',
          timestamp: '2000-01-01T00:00:00.000Z',
          name: 'Item 1',
        },
      ],
      Count: 1,
      ScannedCount: 1,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
      skipValidation: true,
    })

    const result = await testEntity.send(queryCommand)

    expect(result.items).toHaveLength(1)
    expect(result.items[0]).toEqual({
      userId: '123',
      timestamp: '2000-01-01T00:00:00.000Z',
      name: 'Item 1',
    })
  })

  it('queries items with custom validation concurrency', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          userId: '123',
          timestamp: '2000-01-01T00:00:00.000Z',
          name: 'Item 1',
        },
        {
          userId: '123',
          timestamp: '2000-01-02T00:00:00.000Z',
          name: 'Item 2',
        },
      ],
      Count: 2,
      ScannedCount: 2,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
      validationConcurrency: 10,
    })

    const result = await testEntity.send(queryCommand)

    expect(result.items).toHaveLength(2)
  })

  it('constructs correct DynamoDB command with all options', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
    })

    const queryCommand = new Query({
      keyCondition: and(
        { userId: eq('123') },
        { timestamp: gte(new Date('2024-01-01T00:00:00.000Z')) },
      ),
      filter: { count: gt(5) },
      projection: ['userId', 'name'],
      select: 'SPECIFIC_ATTRIBUTES',
      limit: 20,
      consistentRead: true,
      queryIndex: 'UserIndex',
      reverseIndexScan: true,
      exclusiveStartKey: {
        userId: '123',
        timestamp: new Date('2024-01-01T00:00:00.000Z'),
      },
      returnConsumedCapacity: 'INDEXES',
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input).toMatchObject({
      TableName: 'TestTable',
      ConsistentRead: true,
      ScanIndexForward: false,
      Limit: 20,
      IndexName: 'UserIndex',
      Select: 'SPECIFIC_ATTRIBUTES',
      ProjectionExpression: 'userId, name',
      ReturnConsumedCapacity: 'INDEXES',
    })
    expect(input.KeyConditionExpression).toBeDefined()
    expect(input.FilterExpression).toBeDefined()
    expect(input.ExpressionAttributeNames).toBeDefined()
    expect(input.ExpressionAttributeValues).toBeDefined()
    expect(input.ExclusiveStartKey).toBeDefined()
  })

  it('validates that ScanIndexForward defaults to true when reverseIndexScan is not set', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input.ScanIndexForward).toBe(true)
  })

  it('validates that ScanIndexForward is false when reverseIndexScan is true', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
      reverseIndexScan: true,
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input.ScanIndexForward).toBe(false)
  })

  it('validates that ScanIndexForward is true when reverseIndexScan is false', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
      reverseIndexScan: false,
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input.ScanIndexForward).toBe(true)
  })

  it('validates that filter expression is not set when filter is undefined', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input.FilterExpression).toBeUndefined()
  })

  it('validates that projection expression is not set when projection is undefined', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123' },
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input.ProjectionExpression).toBeUndefined()
  })

  it('validates that both key condition and filter use same attribute map', async () => {
    dynamoMock.on(QueryCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
    })

    const queryCommand = new Query({
      keyCondition: { userId: '123', timestamp: gte(new Date('2024-01-01')) },
      filter: { name: eq('Test'), count: lt(100) },
    })

    await testEntity.send(queryCommand)

    const input = dynamoMock.call(0).args[0].input
    expect(input.ExpressionAttributeNames).toBeDefined()
    expect(input.ExpressionAttributeValues).toBeDefined()
    expect(input.KeyConditionExpression).toBeDefined()
    expect(input.FilterExpression).toBeDefined()
  })
})
