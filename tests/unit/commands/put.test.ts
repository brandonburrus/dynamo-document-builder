import { describe, it, expect, beforeEach } from 'vitest'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb'
import { DynamoEntity, DynamoTable, key } from '@/core'
import { ConditionalPut, Put } from '@/commands'
import { mockClient } from 'aws-sdk-client-mock'
import { z, ZodError } from 'zod'

const dynamoClient = new DynamoDBClient()
const documentClient = DynamoDBDocumentClient.from(dynamoClient)
const dynamoMock = mockClient(DynamoDBDocumentClient)

describe('Put Command', () => {
  beforeEach(() => dynamoMock.reset())

  const table = new DynamoTable({
    tableName: 'TestTable',
    documentClient,
  })

  const entity = new DynamoEntity({
    table,
    schema: z.object({
      id: z.string(),
      value: z.number(),
    }),
    partitionKey: item => key('ITEM', item.id),
    sortKey: item => key('VALUE', item.value),
  })

  it('should build a PutItem operation', async () => {
    const putCommand = new Put({
      item: {
        id: '456',
        value: 100,
      },
    })

    dynamoMock.on(PutCommand).resolves({})

    const result = await entity.send(putCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Item: {
          PK: 'ITEM#456',
          SK: 'VALUE#100',
          id: '456',
          value: 100,
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should throw validation error for invalid item', async () => {
    const putCommand = new Put({
      item: {
        id: '789',
        value: 'invalid-number', // This should be a number
        // biome-ignore lint/suspicious/noExplicitAny: for testing purposes
      } as any,
    })

    let threwError = false
    try {
      await entity.send(putCommand)
    } catch (error) {
      threwError = true
      expect(error).toBeInstanceOf(ZodError)
      expect((error as ZodError).issues).toEqual([
        {
          code: 'invalid_type',
          expected: 'number',
          message: 'Invalid input: expected number, received string',
          path: ['value'],
        },
      ])
    }
    expect(threwError).toBe(true)
  })

  it('should skip validation if configured', async () => {
    const putCommand = new Put({
      item: {
        id: '789',
        value: 'not-a-number', // Invalid type, but we will skip validation
      },
      skipValidation: true,
    })

    dynamoMock.on(PutCommand).resolves({})

    const result = await entity.send(putCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Item: {
          PK: 'ITEM#789',
          SK: 'VALUE#not-a-number',
          id: '789',
          value: 'not-a-number',
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should return the old item', async () => {
    const putCommand = new Put({
      item: {
        id: '123',
        value: 42,
      },
      returnValues: 'ALL_OLD',
    })

    dynamoMock.on(PutCommand).resolves({
      Attributes: {
        id: '123',
        value: 24,
      },
    })

    const result = await entity.send(putCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Item: {
          PK: 'ITEM#123',
          SK: 'VALUE#42',
          id: '123',
          value: 42,
        },
        ReturnValues: 'ALL_OLD',
      }),
    )

    expect(result.returnItem).toEqual({
      id: '123',
      value: 24,
    })
  })

  it('should return undefined when there is no old item', async () => {
    const putCommand = new Put({
      item: {
        id: '124',
        value: 43,
      },
      returnValues: 'ALL_OLD',
    })

    dynamoMock.on(PutCommand).resolves({})

    const result = await entity.send(putCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Item: {
          PK: 'ITEM#124',
          SK: 'VALUE#43',
          id: '124',
          value: 43,
        },
        ReturnValues: 'ALL_OLD',
      }),
    )

    expect(result.returnItem).toBeUndefined()
  })

  it('should return the new item', async () => {
    const putCommand = new Put({
      item: {
        id: '125',
        value: 44,
      },
      returnValues: 'ALL_NEW',
    })

    dynamoMock.on(PutCommand).resolves({
      Attributes: {
        id: '125',
        value: 44,
      },
    })

    const result = await entity.send(putCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Item: {
          PK: 'ITEM#125',
          SK: 'VALUE#44',
          id: '125',
          value: 44,
        },
        ReturnValues: 'ALL_NEW',
      }),
    )

    expect(result.returnItem).toEqual({
      id: '125',
      value: 44,
    })
  })

  it('should handle consumed capacity and item collection metrics', async () => {
    const putCommand = new Put({
      item: {
        id: '321',
        value: 55,
      },
      returnConsumedCapacity: 'TOTAL',
      returnItemCollectionMetrics: 'SIZE',
    })

    dynamoMock.on(PutCommand).resolves({
      ConsumedCapacity: {
        TableName: 'TestTable',
        CapacityUnits: 1,
      },
      ItemCollectionMetrics: {
        ItemCollectionKey: {
          PK: 'ITEM#321',
        },
        SizeEstimateRangeGB: [0, 1],
      },
    })

    const result = await entity.send(putCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Item: {
          PK: 'ITEM#321',
          SK: 'VALUE#55',
          id: '321',
          value: 55,
        },
        ReturnConsumedCapacity: 'TOTAL',
        ReturnItemCollectionMetrics: 'SIZE',
      }),
    )

    expect(result.consumedCapacity).toEqual({
      TableName: 'TestTable',
      CapacityUnits: 1,
    })

    expect(result.itemCollectionMetrics).toEqual({
      ItemCollectionKey: {
        PK: 'ITEM#321',
      },
      SizeEstimateRangeGB: [0, 1],
    })
  })

  it('should handle abort signal and timeout', async () => {
    const abortController = new AbortController()
    const putCommand = new Put({
      item: {
        id: '654',
        value: 77,
      },
      abortController,
      timeoutMs: 5000,
    })

    dynamoMock.on(PutCommand).resolves({})

    const result = await entity.send(putCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Item: {
          PK: 'ITEM#654',
          SK: 'VALUE#77',
          id: '654',
          value: 77,
        },
      }),
    )

    expect(result).toBeDefined()
  })
})

describe('Conditional Put Command', () => {
  beforeEach(() => dynamoMock.reset())

  const table = new DynamoTable({
    tableName: 'TestTable',
    documentClient,
  })

  const entity = new DynamoEntity({
    table,
    schema: z.object({
      id: z.string(),
      value: z.number(),
    }),
    partitionKey: item => key('ITEM', item.id),
    sortKey: item => key('VALUE', item.value),
  })

  it('should perform a conditional put operation', async () => {
    const putCommand = new ConditionalPut({
      item: {
        id: '555',
        value: 88,
      },
      condition: {
        value: 100,
      },
    })

    dynamoMock.on(PutCommand).resolves({})

    const result = await entity.send(putCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Item: {
          PK: 'ITEM#555',
          SK: 'VALUE#88',
          id: '555',
          value: 88,
        },
        ConditionExpression: '#value = :v1',
        ExpressionAttributeNames: {
          '#value': 'value',
        },
        ExpressionAttributeValues: {
          ':v1': 100,
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should return the item on conditional put', async () => {
    const putCommand = new ConditionalPut({
      item: {
        id: '556',
        value: 89,
      },
      condition: {
        value: 200,
      },
      returnValues: 'ALL_NEW',
    })

    dynamoMock.on(PutCommand).resolves({
      Attributes: {
        id: '556',
        value: 89,
      },
    })

    const result = await entity.send(putCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Item: {
          PK: 'ITEM#556',
          SK: 'VALUE#89',
          id: '556',
          value: 89,
        },
        ConditionExpression: '#value = :v1',
        ExpressionAttributeNames: {
          '#value': 'value',
        },
        ExpressionAttributeValues: {
          ':v1': 200,
        },
        ReturnValues: 'ALL_NEW',
      }),
    )

    expect(result.returnItem).toEqual({
      id: '556',
      value: 89,
    })
  })

  it('should throw validation error for invalid item in conditional put', async () => {
    const putCommand = new ConditionalPut({
      item: {
        id: '557',
        value: 'not-a-number', // Invalid type
        // biome-ignore lint/suspicious/noExplicitAny: for testing purposes
      } as any,
      condition: {
        value: 300,
      },
    })

    let threwError = false
    try {
      await entity.send(putCommand)
    } catch (error) {
      threwError = true
      expect(error).toBeInstanceOf(ZodError)
      expect((error as ZodError).issues).toEqual([
        {
          code: 'invalid_type',
          expected: 'number',
          message: 'Invalid input: expected number, received string',
          path: ['value'],
        },
      ])
    }
    expect(threwError).toBe(true)
  })

  it('should skip validation if configured in conditional put', async () => {
    const putCommand = new ConditionalPut({
      item: {
        id: '558',
        value: 'not-a-number', // Invalid type, but we will skip validation
      },
      condition: {
        value: 400,
      },
      skipValidation: true,
    })

    dynamoMock.on(PutCommand).resolves({})
    const result = await entity.send(putCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Item: {
          PK: 'ITEM#558',
          SK: 'VALUE#not-a-number',
          id: '558',
          value: 'not-a-number',
        },
        ConditionExpression: '#value = :v1',
        ExpressionAttributeNames: {
          '#value': 'value',
        },
        ExpressionAttributeValues: {
          ':v1': 400,
        },
      }),
    )

    expect(result).toBeDefined()
  })
})
