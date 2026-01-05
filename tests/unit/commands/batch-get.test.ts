import { describe, it, expect, beforeEach } from 'vitest'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, BatchGetCommand } from '@aws-sdk/lib-dynamodb'
import { DynamoEntity, DynamoTable, key } from '@/core'
import { BatchGet } from '@/commands'
import { mockClient } from 'aws-sdk-client-mock'
import { z, ZodError } from 'zod'

const dynamoClient = new DynamoDBClient()
const documentClient = DynamoDBDocumentClient.from(dynamoClient)
const dynamoMock = mockClient(DynamoDBDocumentClient)

describe('Batch Get Command', () => {
  beforeEach(() => dynamoMock.reset())

  const table = new DynamoTable({
    tableName: 'TestTable',
    documentClient,
  })

  const entity = new DynamoEntity({
    table,
    schema: z.object({
      id: z.string(),
      name: z.string(),
      age: z.number().optional(),
    }),
    partitionKey: item => key('USER', item.id),
    sortKey: item => key('NAME', item.name),
  })

  it('should build a batch get operation', async () => {
    const batchGet = new BatchGet({
      keys: [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' },
      ],
    })

    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          { id: '1', name: 'Alice', age: 30 },
          { id: '2', name: 'Bob', age: 25 },
        ],
      },
    })

    const result = await entity.send(batchGet)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        RequestItems: {
          TestTable: {
            Keys: [
              { PK: 'USER#1', SK: 'NAME#Alice' },
              { PK: 'USER#2', SK: 'NAME#Bob' },
            ],
            ConsistentRead: false,
          },
        },
      }),
    )

    expect(result.items).toEqual([
      { id: '1', name: 'Alice', age: 30 },
      { id: '2', name: 'Bob', age: 25 },
    ])
  })

  it('should build a batch get with consistent reads', async () => {
    const batchGet = new BatchGet({
      keys: [{ id: '1', name: 'Alice' }],
      consistent: true,
    })

    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [{ id: '1', name: 'Alice', age: 30 }],
      },
    })

    const result = await entity.send(batchGet)

    expect(dynamoMock.calls()[0].args[0].input).toEqual({
      RequestItems: {
        TestTable: {
          Keys: [{ PK: 'USER#1', SK: 'NAME#Alice' }],
          ConsistentRead: true,
        },
      },
      ReturnConsumedCapacity: undefined,
    })

    expect(result.items).toEqual([{ id: '1', name: 'Alice', age: 30 }])
  })

  it('should return unprocessed keys', async () => {
    const batchGet = new BatchGet({
      keys: [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' },
      ],
    })

    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [{ id: '1', name: 'Alice', age: 30 }],
      },
      UnprocessedKeys: {
        TestTable: {
          Keys: [{ PK: 'USER#2', SK: 'NAME#Bob' }],
        },
      },
    })

    const result = await entity.send(batchGet)

    expect(result.items).toEqual([{ id: '1', name: 'Alice', age: 30 }])

    expect(result.unprocessedKeys).toEqual([{ PK: 'USER#2', SK: 'NAME#Bob' }])
  })

  it('should return response metadata and consumed capacity', async () => {
    const batchGet = new BatchGet({
      keys: [{ id: '1', name: 'Alice' }],
      returnConsumedCapacity: 'TOTAL',
    })

    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [{ id: '1', name: 'Alice', age: 30 }],
      },
      ConsumedCapacity: [
        {
          TableName: 'TestTable',
          CapacityUnits: 1,
        },
      ],
    })

    const result = await entity.send(batchGet)

    expect(result.items).toEqual([{ id: '1', name: 'Alice', age: 30 }])

    expect(result.consumedCapacity).toEqual({
      TableName: 'TestTable',
      CapacityUnits: 1,
    })
  })

  it('should skip validation when configured', async () => {
    const batchGet = new BatchGet({
      keys: [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' },
      ],
      skipValidation: true,
    })

    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          { id: '1', name: 'Alice', age: 30 },
          { id: '2', name: 'Bob', age: 'invalid-age' }, // invalid age type
        ],
      },
    })

    const result = await entity.send(batchGet)

    expect(result.items).toEqual([
      { id: '1', name: 'Alice', age: 30 },
      { id: '2', name: 'Bob', age: 'invalid-age' },
    ])
  })

  it('should handle abort signal and timeout', async () => {
    const abortController = new AbortController()

    const batchGet = new BatchGet({
      keys: [{ id: '1', name: 'Alice' }],
      abortController,
      timeoutMs: 50,
    })

    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [{ id: '1', name: 'Alice', age: 30 }],
      },
    })

    const result = await entity.send(batchGet)

    expect(result.items).toEqual([{ id: '1', name: 'Alice', age: 30 }])
  })

  it('should throw validation error for invalid item', async () => {
    const batchGet = new BatchGet({
      keys: [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' },
      ],
    })

    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          { id: '1', name: 'Alice', age: 30 },
          { id: '2', name: 'Bob', age: 'invalid-age' }, // invalid age type
        ],
      },
    })

    await expect(entity.send(batchGet)).rejects.toThrow(ZodError)
  })
})
