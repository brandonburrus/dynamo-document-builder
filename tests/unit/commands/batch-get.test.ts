import { describe, it, expect, beforeEach } from 'vitest'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, BatchGetCommand } from '@aws-sdk/lib-dynamodb'
import { DynamoEntity, DynamoTable, key } from '@/core'
import { BatchGet } from '@/commands'
import { BatchProjectedGet } from '@/commands/batch-projected-get'
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

describe('Batch Projected Get Command', () => {
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
      status: z.string().optional(),
      email: z.string().optional(),
    }),
    partitionKey: item => key('USER', item.id),
    sortKey: item => key('NAME', item.name),
  })

  it('should build a batch projected get operation', async () => {
    const batchProjectedGet = new BatchProjectedGet({
      keys: [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' },
      ],
      projection: ['id', 'name', 'status'],
      projectionSchema: z.object({
        id: z.string(),
        name: z.string(),
        status: z.string().optional(),
      }),
    })

    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          { id: '1', name: 'Alice', status: 'active' },
          { id: '2', name: 'Bob', status: 'inactive' },
        ],
      },
    })

    const result = await entity.send(batchProjectedGet)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        RequestItems: {
          TestTable: {
            Keys: [
              { PK: 'USER#1', SK: 'NAME#Alice' },
              { PK: 'USER#2', SK: 'NAME#Bob' },
            ],
            ConsistentRead: false,
            ProjectionExpression: '#id, #name, #status',
            ExpressionAttributeNames: {
              '#id': 'id',
              '#name': 'name',
              '#status': 'status',
            },
          },
        },
      }),
    )

    expect(result.items).toEqual([
      { id: '1', name: 'Alice', status: 'active' },
      { id: '2', name: 'Bob', status: 'inactive' },
    ])
  })

  it('should build a batch projected get with consistent reads', async () => {
    const batchProjectedGet = new BatchProjectedGet({
      keys: [{ id: '1', name: 'Alice' }],
      projection: ['id', 'email'],
      projectionSchema: z.object({
        id: z.string(),
        email: z.string().optional(),
      }),
      consistent: true,
    })

    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [{ id: '1', email: 'alice@example.com' }],
      },
    })

    const result = await entity.send(batchProjectedGet)

    expect(dynamoMock.calls()[0].args[0].input).toEqual({
      RequestItems: {
        TestTable: {
          Keys: [{ PK: 'USER#1', SK: 'NAME#Alice' }],
          ConsistentRead: true,
          ProjectionExpression: '#id, #email',
          ExpressionAttributeNames: {
            '#id': 'id',
            '#email': 'email',
          },
        },
      },
      ReturnConsumedCapacity: undefined,
    })

    expect(result.items).toEqual([{ id: '1', email: 'alice@example.com' }])
  })

  it('should return unprocessed keys with projections', async () => {
    const batchProjectedGet = new BatchProjectedGet({
      keys: [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' },
      ],
      projection: ['id', 'status'],
      projectionSchema: z.object({
        id: z.string(),
        status: z.string().optional(),
      }),
    })

    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [{ id: '1', status: 'active' }],
      },
      UnprocessedKeys: {
        TestTable: {
          Keys: [{ PK: 'USER#2', SK: 'NAME#Bob' }],
        },
      },
    })

    const result = await entity.send(batchProjectedGet)

    expect(result.items).toEqual([{ id: '1', status: 'active' }])
    expect(result.unprocessedKeys).toEqual([{ PK: 'USER#2', SK: 'NAME#Bob' }])
  })

  it('should return response metadata and consumed capacity', async () => {
    const batchProjectedGet = new BatchProjectedGet({
      keys: [{ id: '1', name: 'Alice' }],
      projection: ['id', 'age'],
      projectionSchema: z.object({
        id: z.string(),
        age: z.number().optional(),
      }),
      returnConsumedCapacity: 'TOTAL',
    })

    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [{ id: '1', age: 30 }],
      },
      ConsumedCapacity: [
        {
          TableName: 'TestTable',
          CapacityUnits: 1,
        },
      ],
      $metadata: {
        requestId: 'test-request-id',
        httpStatusCode: 200,
      },
    })

    const result = await entity.send(batchProjectedGet)

    expect(result.items).toEqual([{ id: '1', age: 30 }])
    expect(result.consumedCapacity).toEqual({
      TableName: 'TestTable',
      CapacityUnits: 1,
    })
    expect(result.responseMetadata).toEqual({
      requestId: 'test-request-id',
      httpStatusCode: 200,
    })
  })

  it('should skip validation when configured', async () => {
    const batchProjectedGet = new BatchProjectedGet({
      keys: [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' },
      ],
      projection: ['id', 'age'],
      projectionSchema: z.object({
        id: z.string(),
        age: z.number().optional(),
      }),
      skipValidation: true,
    })

    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          { id: '1', age: 30 },
          { id: '2', age: 'invalid-age' }, // invalid age type
        ],
      },
    })

    const result = await entity.send(batchProjectedGet)

    expect(result.items).toEqual([
      { id: '1', age: 30 },
      { id: '2', age: 'invalid-age' },
    ])
  })

  it('should handle abort signal and timeout', async () => {
    const abortController = new AbortController()

    const batchProjectedGet = new BatchProjectedGet({
      keys: [{ id: '1', name: 'Alice' }],
      projection: ['id', 'name'],
      projectionSchema: z.object({
        id: z.string(),
        name: z.string(),
      }),
      abortController,
      timeoutMs: 50,
    })

    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [{ id: '1', name: 'Alice' }],
      },
    })

    const result = await entity.send(batchProjectedGet)

    expect(result.items).toEqual([{ id: '1', name: 'Alice' }])
  })

  it('should throw validation error for invalid projected items', async () => {
    const batchProjectedGet = new BatchProjectedGet({
      keys: [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' },
      ],
      projection: ['id', 'age'],
      projectionSchema: z.object({
        id: z.string(),
        age: z.number().optional(),
      }),
    })

    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          { id: '1', age: 30 },
          { id: '2', age: 'invalid-age' }, // invalid age type
        ],
      },
    })

    await expect(entity.send(batchProjectedGet)).rejects.toThrow(ZodError)
  })

  it('should handle nested object projections', async () => {
    const entityWithNested = new DynamoEntity({
      table,
      schema: z.object({
        id: z.string(),
        name: z.string(),
        address: z
          .object({
            street: z.string(),
            city: z.string(),
          })
          .optional(),
      }),
      partitionKey: item => key('USER', item.id),
      sortKey: item => key('NAME', item.name),
    })

    const batchProjectedGet = new BatchProjectedGet({
      keys: [{ id: '1', name: 'Alice' }],
      projection: ['id', 'address.city'],
      projectionSchema: z.object({
        id: z.string(),
        address: z
          .object({
            city: z.string(),
          })
          .optional(),
      }),
    })

    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          {
            id: '1',
            address: {
              city: 'New York',
            },
          },
        ],
      },
    })

    const result = await entityWithNested.send(batchProjectedGet)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        RequestItems: {
          TestTable: {
            Keys: [{ PK: 'USER#1', SK: 'NAME#Alice' }],
            ConsistentRead: false,
            ProjectionExpression: '#id, #address.#city',
            ExpressionAttributeNames: {
              '#id': 'id',
              '#address': 'address',
              '#city': 'city',
            },
          },
        },
      }),
    )

    expect(result.items).toEqual([
      {
        id: '1',
        address: {
          city: 'New York',
        },
      },
    ])
  })
})
