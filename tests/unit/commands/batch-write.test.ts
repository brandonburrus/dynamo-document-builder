import { describe, it, expect, beforeEach } from 'vitest'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, BatchWriteCommand } from '@aws-sdk/lib-dynamodb'
import { DynamoEntity, DynamoTable, key } from '@/core'
import { BatchWrite } from '@/commands'
import { mockClient } from 'aws-sdk-client-mock'
import { z, ZodError } from 'zod'

const dynamoClient = new DynamoDBClient()
const documentClient = DynamoDBDocumentClient.from(dynamoClient)
const dynamoMock = mockClient(DynamoDBDocumentClient)

describe('Batch Write Command', () => {
  beforeEach(() => dynamoMock.reset())

  const table = new DynamoTable({
    tableName: 'TestTable',
    documentClient,
  })

  const entity = new DynamoEntity({
    table,
    schema: z.object({
      id: z.string(),
      sku: z.string().length(8),
      price: z.number(),
    }),
    partitionKey: item => key('PRODUCT', item.id),
    sortKey: item => key('SKU', item.sku),
  })

  it('should build a batch write operation with puts', async () => {
    const batchWrite = new BatchWrite({
      items: [
        { id: '1', sku: 'ABCDEFGH', price: 19.99 },
        { id: '2', sku: 'IJKLMNOP', price: 29.99 },
      ],
    })

    dynamoMock.on(BatchWriteCommand).resolves({
      UnprocessedItems: {},
    })

    const result = await entity.send(batchWrite)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        RequestItems: {
          TestTable: [
            {
              PutRequest: {
                Item: {
                  PK: 'PRODUCT#1',
                  SK: 'SKU#ABCDEFGH',
                  id: '1',
                  sku: 'ABCDEFGH',
                  price: 19.99,
                },
              },
            },
            {
              PutRequest: {
                Item: {
                  PK: 'PRODUCT#2',
                  SK: 'SKU#IJKLMNOP',
                  id: '2',
                  sku: 'IJKLMNOP',
                  price: 29.99,
                },
              },
            },
          ],
        },
      }),
    )

    expect(result.unprocessedPuts).toBeUndefined()
  })

  it('should build a batch write with deletes', async () => {
    const batchWrite = new BatchWrite({
      deletes: [
        { id: '1', sku: 'ABCDEFGH' },
        { id: '2', sku: 'IJKLMNOP' },
      ],
    })

    dynamoMock.on(BatchWriteCommand).resolves({
      UnprocessedItems: {},
    })

    const result = await entity.send(batchWrite)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        RequestItems: {
          TestTable: [
            {
              DeleteRequest: {
                Key: {
                  PK: 'PRODUCT#1',
                  SK: 'SKU#ABCDEFGH',
                },
              },
            },
            {
              DeleteRequest: {
                Key: {
                  PK: 'PRODUCT#2',
                  SK: 'SKU#IJKLMNOP',
                },
              },
            },
          ],
        },
      }),
    )

    expect(result.unprocessedDeletes).toBeUndefined()
  })

  it('should return unprocessed items', async () => {
    const batchWrite = new BatchWrite({
      items: [
        { id: '1', sku: 'ABCDEFGH', price: 19.99 },
        { id: '2', sku: 'IJKLMNOP', price: 29.99 },
      ],
      deletes: [{ id: '3', sku: 'QRSTUVWX' }],
    })

    dynamoMock.on(BatchWriteCommand).resolves({
      UnprocessedItems: {
        TestTable: [
          {
            PutRequest: {
              Item: {
                PK: 'PRODUCT#2',
                SK: 'SKU#IJKLMNOP',
                id: '2',
                sku: 'IJKLMNOP',
                price: 29.99,
              },
            },
          },
          {
            DeleteRequest: {
              Key: {
                PK: 'PRODUCT#3',
                SK: 'SKU#QRSTUVWX',
              },
            },
          },
        ],
      },
    })

    const result = await entity.send(batchWrite)

    expect(result.unprocessedPuts).toEqual([
      {
        PK: 'PRODUCT#2',
        SK: 'SKU#IJKLMNOP',
        id: '2',
        sku: 'IJKLMNOP',
        price: 29.99,
      },
    ])

    expect(result.unprocessedDeletes).toEqual([
      {
        PK: 'PRODUCT#3',
        SK: 'SKU#QRSTUVWX',
      },
    ])
  })

  it('should build a batch write operation with puts and deletes', async () => {
    const batchWrite = new BatchWrite({
      items: [{ id: '1', sku: 'ABCDEFGH', price: 19.99 }],
      deletes: [{ id: '2', sku: 'IJKLMNOP' }],
    })

    dynamoMock.on(BatchWriteCommand).resolves({
      UnprocessedItems: {},
    })

    const result = await entity.send(batchWrite)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        RequestItems: {
          TestTable: [
            {
              PutRequest: {
                Item: {
                  PK: 'PRODUCT#1',
                  SK: 'SKU#ABCDEFGH',
                  id: '1',
                  sku: 'ABCDEFGH',
                  price: 19.99,
                },
              },
            },
            {
              DeleteRequest: {
                Key: {
                  PK: 'PRODUCT#2',
                  SK: 'SKU#IJKLMNOP',
                },
              },
            },
          ],
        },
      }),
    )

    expect(result.unprocessedPuts).toBeUndefined()
    expect(result.unprocessedDeletes).toBeUndefined()
  })

  it('should throw a validation error for invalid items', async () => {
    const batchWrite = new BatchWrite({
      items: [
        { id: '1', sku: 'SHORT', price: 19.99 }, // Invalid SKU length
      ],
    })

    await expect(entity.send(batchWrite)).rejects.toThrow(ZodError)
  })

  it('should skip validation when configured', async () => {
    const batchWrite = new BatchWrite({
      items: [
        { id: '1', sku: 'SHORT', price: 19.99 }, // Invalid SKU length
      ],
      skipValidation: true,
    })

    dynamoMock.on(BatchWriteCommand).resolves({
      UnprocessedItems: {},
    })

    const result = await entity.send(batchWrite)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        RequestItems: {
          TestTable: [
            {
              PutRequest: {
                Item: {
                  PK: 'PRODUCT#1',
                  SK: 'SKU#SHORT',
                  id: '1',
                  sku: 'SHORT',
                  price: 19.99,
                },
              },
            },
          ],
        },
      }),
    )

    expect(result.unprocessedPuts).toBeUndefined()
  })

  it('should handle abort signal and timeout', async () => {
    const abortController = new AbortController()

    const batchWrite = new BatchWrite({
      items: [{ id: '1', sku: 'ABCDEFGH', price: 19.99 }],
      abortController,
      timeoutMs: 5000,
    })

    dynamoMock.on(BatchWriteCommand).resolves({
      UnprocessedItems: {},
    })

    const result = await entity.send(batchWrite)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        RequestItems: {
          TestTable: [
            {
              PutRequest: {
                Item: {
                  PK: 'PRODUCT#1',
                  SK: 'SKU#ABCDEFGH',
                  id: '1',
                  sku: 'ABCDEFGH',
                  price: 19.99,
                },
              },
            },
          ],
        },
      }),
    )

    expect(result.unprocessedPuts).toBeUndefined()
  })
})
