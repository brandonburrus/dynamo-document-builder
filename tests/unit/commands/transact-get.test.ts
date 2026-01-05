import { describe, it, expect, beforeEach } from 'vitest'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, TransactGetCommand } from '@aws-sdk/lib-dynamodb'
import { DynamoEntity, DynamoTable, key } from '@/core'
import { TransactGet } from '@/commands'
import { mockClient } from 'aws-sdk-client-mock'
import { z, ZodError } from 'zod'

const dynamoClient = new DynamoDBClient()
const documentClient = DynamoDBDocumentClient.from(dynamoClient)
const dynamoMock = mockClient(DynamoDBDocumentClient)

describe('Transact Get Command', () => {
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
    partitionKey: item => key('USER', item.id),
    sortKey: item => key('NAME', item.name),
  })

  it('should build a transact get operation', async () => {
    const transactGet = new TransactGet({
      keys: [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' },
      ],
    })

    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [{ Item: { id: '1', name: 'Alice' } }, { Item: { id: '2', name: 'Bob' } }],
    })

    const result = await entity.send(transactGet)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TransactItems: [
          {
            Get: {
              TableName: 'TestTable',
              Key: { PK: 'USER#1', SK: 'NAME#Alice' },
            },
          },
          {
            Get: {
              TableName: 'TestTable',
              Key: { PK: 'USER#2', SK: 'NAME#Bob' },
            },
          },
        ],
      }),
    )

    expect(result.items).toEqual([
      { id: '1', name: 'Alice' },
      { id: '2', name: 'Bob' },
    ])
  })

  it('should handle missing items in transact get', async () => {
    const transactGet = new TransactGet({
      keys: [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' },
      ],
    })

    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [{ Item: { id: '1', name: 'Alice' } }, { Item: undefined }],
    })

    const result = await entity.send(transactGet)

    expect(result.items).toEqual([{ id: '1', name: 'Alice' }, undefined])
  })

  it('should throw validation error for invalid items', async () => {
    const transactGet = new TransactGet({
      keys: [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' },
      ],
    })

    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [
        { Item: { id: '1', name: 'Alice' } },
        { Item: { id: '2', name: 123 } }, // Invalid type for name
      ],
    })

    await expect(entity.send(transactGet)).rejects.toThrow(ZodError)
  })

  it('should handle abort signal and timeout', async () => {
    const abortController = new AbortController()

    const transactGet = new TransactGet({
      keys: [{ id: '1', name: 'Alice' }],
      abortController,
      timeoutMs: 50,
    })

    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [{ Item: { id: '1', name: 'Alice' } }],
    })

    const result = await entity.send(transactGet)

    expect(result.items).toEqual([{ id: '1', name: 'Alice' }])
  })

  it('should return response metadata and consumed capacity', async () => {
    const transactGet = new TransactGet({
      keys: [{ id: '1', name: 'Alice' }],
      returnConsumedCapacity: 'TOTAL',
    })

    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [{ Item: { id: '1', name: 'Alice' } }],
      ConsumedCapacity: [
        {
          TableName: 'TestTable',
          CapacityUnits: 1,
        },
      ],
    })

    const result = await entity.send(transactGet)

    expect(result.items).toEqual([{ id: '1', name: 'Alice' }])
    expect(result.consumedCapacity).toEqual({
      TableName: 'TestTable',
      CapacityUnits: 1,
    })
  })

  it('should skip validation when configured', async () => {
    const transactGet = new TransactGet({
      keys: [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' },
      ],
      skipValidation: true,
    })

    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [
        { Item: { id: '1', name: 'Alice' } },
        { Item: { id: '2', name: 123 } }, // invalid name type
      ],
    })

    const result = await entity.send(transactGet)

    expect(result.items).toEqual([
      { id: '1', name: 'Alice' },
      { id: '2', name: 123 },
    ])
  })
})
