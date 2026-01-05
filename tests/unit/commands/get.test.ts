import { describe, it, expect, beforeEach } from 'vitest'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, GetCommand } from '@aws-sdk/lib-dynamodb'
import { DynamoEntity, DynamoTable, key } from '@/core'
import { Get, ProjectedGet } from '@/commands'
import { mockClient } from 'aws-sdk-client-mock'
import { z, ZodError } from 'zod'

const dynamoClient = new DynamoDBClient()
const documentClient = DynamoDBDocumentClient.from(dynamoClient)
const dynamoMock = mockClient(DynamoDBDocumentClient)

describe('Get Command', () => {
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
    }),
    partitionKey: item => key('ITEM', item.id),
    sortKey: item => key('NAME', item.name),
  })

  it('should build a GetItem operation', async () => {
    const getCommand = new Get({
      key: {
        id: '123',
        name: 'TestName',
      },
    })

    dynamoMock.on(GetCommand).resolves({
      Item: {
        id: '123',
        name: 'TestName',
      },
    })

    const result = await entity.send(getCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#123',
          SK: 'NAME#TestName',
        },
      }),
    )

    expect(result.item).toEqual({
      id: '123',
      name: 'TestName',
    })
  })

  it('should build a GetItem operation without key builders', async () => {
    const simpleEntity = new DynamoEntity({
      table,
      schema: z.object({
        id: z.string(),
        age: z.number(),
      }),
    })

    const getCommand = new Get({
      key: {
        PK: '456',
        SK: 30,
      },
    })

    dynamoMock.on(GetCommand).resolves({
      Item: {
        id: '456',
        age: 30,
      },
    })

    const result = await simpleEntity.send(getCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: '456',
          SK: 30,
        },
      }),
    )

    expect(result.item).toEqual({
      id: '456',
      age: 30,
    })
  })

  it('should build a GetItem operation with consistent read', async () => {
    const getCommand = new Get({
      key: {
        id: '789',
        name: 'ConsistentRead',
      },
      consistent: true,
    })

    dynamoMock.on(GetCommand).resolves({
      Item: {
        id: '789',
        name: 'ConsistentRead',
      },
    })

    const result = await entity.send(getCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#789',
          SK: 'NAME#ConsistentRead',
        },
        ConsistentRead: true,
      }),
    )

    expect(result.item).toEqual({
      id: '789',
      name: 'ConsistentRead',
    })
  })

  it('should return undefined when item does not exist', async () => {
    const getCommand = new Get({
      key: {
        id: '000',
        name: 'NonExistent',
      },
    })

    dynamoMock.on(GetCommand).resolves({})

    const result = await entity.send(getCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#000',
          SK: 'NAME#NonExistent',
        },
      }),
    )

    expect(result.item).toBeUndefined()
  })

  it('should return response metadata and consumed capacity', async () => {
    const getCommand = new Get({
      key: {
        id: '321',
        name: 'MetaDataTest',
      },
      returnConsumedCapacity: 'TOTAL',
    })

    dynamoMock.on(GetCommand).resolves({
      Item: {
        id: '321',
        name: 'MetaDataTest',
      },
      ConsumedCapacity: {
        TableName: 'TestTable',
        CapacityUnits: 1,
      },
      $metadata: {
        requestId: 'test-request-id',
        httpStatusCode: 200,
      },
    })

    const result = await entity.send(getCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#321',
          SK: 'NAME#MetaDataTest',
        },
        ReturnConsumedCapacity: 'TOTAL',
      }),
    )

    expect(result.item).toEqual({
      id: '321',
      name: 'MetaDataTest',
    })
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
    const getCommand = new Get({
      key: {
        id: '654',
        name: 'SkipValidation',
      },
      skipValidation: true,
    })

    dynamoMock.on(GetCommand).resolves({
      Item: {
        id: '654',
        name: 'SkipValidation',
        extraField: 'This should be ignored',
      },
    })

    const result = await entity.send(getCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#654',
          SK: 'NAME#SkipValidation',
        },
      }),
    )

    expect(result.item).toEqual({
      id: '654',
      name: 'SkipValidation',
      extraField: 'This should be ignored',
    })
  })

  it('should handle abort signal and timeout', async () => {
    const abortController = new AbortController()

    const getCommand = new Get({
      key: {
        id: '987',
        name: 'AbortTimeout',
      },
      abortController,
      timeoutMs: 5000,
    })

    dynamoMock.on(GetCommand).resolves({
      Item: {
        id: '987',
        name: 'AbortTimeout',
      },
    })

    const result = await entity.send(getCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#987',
          SK: 'NAME#AbortTimeout',
        },
      }),
    )

    expect(result.item).toEqual({
      id: '987',
      name: 'AbortTimeout',
    })
  })

  it('should throw validation error for invalid item', async () => {
    const getCommand = new Get({
      key: {
        id: '111',
        name: 'InvalidItem',
      },
    })

    dynamoMock.on(GetCommand).resolves({
      Item: {
        id: '111',
        name: 12345, // Invalid type
      },
    })

    let threwError = false
    try {
      await entity.send(getCommand)
    } catch (error) {
      threwError = true
      expect(error).toBeInstanceOf(ZodError)
      expect((error as ZodError).issues).toEqual([
        {
          code: 'invalid_type',
          expected: 'string',
          message: 'Invalid input: expected string, received number',
          path: ['name'],
        },
      ])
    }
    expect(threwError).toBe(true)
  })
})

describe('Projected Get Command', () => {
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
    }),
    partitionKey: item => key('USER', item.id),
    sortKey: item => key('NAME', item.name),
  })

  it('should build a projected GetItem operation', async () => {
    const projectedGet = new ProjectedGet({
      key: {
        id: '222',
        name: 'ProjectedItem',
      },
      projection: ['id', 'status'],
      projectionSchema: z.object({
        id: z.string(),
        status: z.string().optional(),
      }),
    })

    dynamoMock.on(GetCommand).resolves({
      Item: {
        id: '222',
        status: 'active',
      },
    })

    const result = await entity.send(projectedGet)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'USER#222',
          SK: 'NAME#ProjectedItem',
        },
        ProjectionExpression: '#id, #status',
        ExpressionAttributeNames: {
          '#id': 'id',
          '#status': 'status',
        },
      }),
    )

    expect(result.item).toEqual({
      id: '222',
      status: 'active',
    })
  })
})
