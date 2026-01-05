import { describe, it, expect, beforeEach } from 'vitest'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DeleteCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import { DynamoEntity, DynamoTable, key } from '@/core'
import { Delete, ConditionalDelete } from '@/commands'
import { mockClient } from 'aws-sdk-client-mock'
import { z, ZodError } from 'zod'

const dynamoClient = new DynamoDBClient()
const documentClient = DynamoDBDocumentClient.from(dynamoClient)
const dynamoMock = mockClient(DynamoDBDocumentClient)

describe('Delete Command', () => {
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

  it('should build a DeleteItem operation', async () => {
    const deleteCommand = new Delete({
      key: {
        id: '456',
        value: 100,
      },
    })

    dynamoMock.on(DeleteCommand).resolves({})

    const result = await entity.send(deleteCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#456',
          SK: 'VALUE#100',
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should return the deleted item', async () => {
    const deleteCommand = new Delete({
      key: {
        id: '456',
        value: 100,
      },
      returnValues: 'ALL_OLD',
    })

    dynamoMock.on(DeleteCommand).resolves({
      Attributes: {
        id: '456',
        value: 100,
      },
    })

    const result = await entity.send(deleteCommand)

    expect(result.deletedItem).toEqual({
      id: '456',
      value: 100,
    })
  })

  it('should throw validation error for invalid deleted item', async () => {
    const deleteCommand = new Delete({
      key: {
        id: '789',
        value: 200,
      },
      returnValues: 'ALL_OLD',
    })

    dynamoMock.on(DeleteCommand).resolves({
      Attributes: {
        id: '789',
        value: 'invalid-number',
      },
    })

    let threwError = false
    try {
      await entity.send(deleteCommand)
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

  it('should skip validation for deleted item if configured', async () => {
    const deleteCommand = new Delete({
      key: {
        id: '789',
        value: 200,
      },
      returnValues: 'ALL_OLD',
      skipValidation: true,
    })

    dynamoMock.on(DeleteCommand).resolves({
      Attributes: {
        id: '789',
        value: 'invalid-number',
      },
    })

    const result = await entity.send(deleteCommand)

    expect(result.deletedItem).toEqual({
      id: '789',
      value: 'invalid-number',
    })
  })

  it('should handle consumed capacity and item collection metrics', async () => {
    const deleteCommand = new Delete({
      key: {
        id: '789',
        value: 200,
      },
      returnConsumedCapacity: 'TOTAL',
      returnItemCollectionMetrics: 'SIZE',
    })

    dynamoMock.on(DeleteCommand).resolves({
      ConsumedCapacity: {
        TableName: 'TestTable',
        CapacityUnits: 1,
      },
      ItemCollectionMetrics: {
        ItemCollectionKey: {
          PK: 'ITEM#789',
          SK: 'VALUE#200',
        },
        SizeEstimateRangeGB: [0, 1],
      },
    })

    const result = await entity.send(deleteCommand)

    expect(result.consumedCapacity).toEqual({
      TableName: 'TestTable',
      CapacityUnits: 1,
    })

    expect(result.itemCollectionMetrics).toEqual({
      ItemCollectionKey: {
        PK: 'ITEM#789',
        SK: 'VALUE#200',
      },
      SizeEstimateRangeGB: [0, 1],
    })
  })

  it('should handle abort signal and timeout', async () => {
    const abortController = new AbortController()

    const deleteCommand = new Delete({
      key: {
        id: '999',
        value: 300,
      },
      abortController,
      timeoutMs: 5000,
    })

    dynamoMock.on(DeleteCommand).resolves({})

    const result = await entity.send(deleteCommand)
    expect(result).toBeDefined()
  })
})

describe('Conditional Delete Command', () => {
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

  it('should build a conditional DeleteItem operation', async () => {
    const conditionalDeleteCommand = new ConditionalDelete({
      key: {
        id: '111',
        value: 400,
      },
      condition: {
        attribute: 'value',
      },
    })

    dynamoMock.on(DeleteCommand).resolves({})

    const result = await entity.send(conditionalDeleteCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#111',
          SK: 'VALUE#400',
        },
        ConditionExpression: '#attribute = :v1',
        ExpressionAttributeNames: {
          '#attribute': 'attribute',
        },
        ExpressionAttributeValues: {
          ':v1': 'value',
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should return the deleted item on conditional delete', async () => {
    const conditionalDeleteCommand = new ConditionalDelete({
      key: {
        id: '111',
        value: 400,
      },
      condition: {
        attribute: 'value',
      },
      returnValues: 'ALL_OLD',
    })

    dynamoMock.on(DeleteCommand).resolves({
      Attributes: {
        id: '111',
        value: 400,
      },
    })

    const result = await entity.send(conditionalDeleteCommand)

    expect(result.deletedItem).toEqual({
      id: '111',
      value: 400,
    })
  })

  it('should throw validation error for invalid deleted item on conditional delete', async () => {
    const conditionalDeleteCommand = new ConditionalDelete({
      key: {
        id: '222',
        value: 500,
      },
      condition: {
        attribute: 'value',
      },
      returnValues: 'ALL_OLD',
    })

    dynamoMock.on(DeleteCommand).resolves({
      Attributes: {
        id: '222',
        value: 'invalid-number',
      },
    })

    let threwError = false
    try {
      await entity.send(conditionalDeleteCommand)
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

  it('should skip validation for deleted item if configured on conditional delete', async () => {
    const conditionalDeleteCommand = new ConditionalDelete({
      key: {
        id: '222',
        value: 500,
      },
      condition: {
        attribute: 'value',
      },
      returnValues: 'ALL_OLD',
      skipValidation: true,
    })

    dynamoMock.on(DeleteCommand).resolves({
      Attributes: {
        id: '222',
        value: 'invalid-number',
      },
    })

    const result = await entity.send(conditionalDeleteCommand)

    expect(result.deletedItem).toEqual({
      id: '222',
      value: 'invalid-number',
    })
  })
})
