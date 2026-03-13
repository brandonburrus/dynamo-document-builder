import { describe, it, expect, beforeEach } from 'vitest'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, BatchWriteCommand } from '@aws-sdk/lib-dynamodb'
import { DynamoEntity, DynamoTable, key } from '@/core'
import { BatchWrite, TableBatchWrite } from '@/commands'
import { DocumentBuilderError } from '@/errors'
import { mockClient } from 'aws-sdk-client-mock'
import { z, ZodError } from 'zod'

const dynamoClient = new DynamoDBClient()
const documentClient = DynamoDBDocumentClient.from(dynamoClient)
const dynamoMock = mockClient(DynamoDBDocumentClient)

describe('TableBatchWrite Command', () => {
  beforeEach(() => dynamoMock.reset())

  const table = new DynamoTable({
    tableName: 'TestTable',
    documentClient,
  })

  const userEntity = new DynamoEntity({
    table,
    schema: z.object({
      userId: z.string(),
      name: z.string(),
    }),
    partitionKey: item => key('USER', item.userId),
    sortKey: () => 'METADATA',
  })

  const orderEntity = new DynamoEntity({
    table,
    schema: z.object({
      orderId: z.string(),
      status: z.string(),
      total: z.number(),
    }),
    partitionKey: item => key('ORDER', item.orderId),
    sortKey: () => 'METADATA',
  })

  const productEntity = new DynamoEntity({
    table,
    schema: z.object({
      productId: z.string(),
      price: z.number(),
    }),
    partitionKey: item => key('PRODUCT', item.productId),
    sortKey: () => 'METADATA',
  })

  // ---------------------------------------------------------------------------
  // Request shape
  // ---------------------------------------------------------------------------

  it('should build a batch write with puts across multiple entity types', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} })

    await table.send(
      new TableBatchWrite({
        writes: [
          userEntity.prepare(new BatchWrite({ items: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchWrite({ items: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
          ),
        ],
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        RequestItems: {
          TestTable: [
            {
              PutRequest: {
                Item: { PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' },
              },
            },
            {
              PutRequest: {
                Item: {
                  PK: 'ORDER#o1',
                  SK: 'METADATA',
                  orderId: 'o1',
                  status: 'pending',
                  total: 99,
                },
              },
            },
          ],
        },
      }),
    )
  })

  it('should build a batch write with deletes across multiple entity types', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} })

    await table.send(
      new TableBatchWrite({
        writes: [
          userEntity.prepare(new BatchWrite({ deletes: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchWrite({ deletes: [{ orderId: 'o1', status: 'pending', total: 0 }] }),
          ),
        ],
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        RequestItems: {
          TestTable: [
            { DeleteRequest: { Key: { PK: 'USER#u1', SK: 'METADATA' } } },
            { DeleteRequest: { Key: { PK: 'ORDER#o1', SK: 'METADATA' } } },
          ],
        },
      }),
    )
  })

  it('should build a batch write with mixed puts and deletes within the same entity group', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} })

    await table.send(
      new TableBatchWrite({
        writes: [
          userEntity.prepare(
            new BatchWrite({
              items: [{ userId: 'u1', name: 'Alice' }],
              deletes: [{ userId: 'u2', name: 'Bob' }],
            }),
          ),
        ],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const requestItems = (dynamoMock.calls()[0].args[0].input as any).RequestItems.TestTable
    expect(requestItems).toHaveLength(2)
    expect(requestItems[0]).toHaveProperty('PutRequest.Item.userId', 'u1')
    expect(requestItems[1]).toHaveProperty('DeleteRequest.Key.PK', 'USER#u2')
  })

  it('should interleave puts and deletes from different entity groups preserving group order', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} })

    await table.send(
      new TableBatchWrite({
        writes: [
          userEntity.prepare(
            new BatchWrite({
              items: [
                { userId: 'u1', name: 'Alice' },
                { userId: 'u2', name: 'Bob' },
              ],
            }),
          ),
          orderEntity.prepare(
            new BatchWrite({
              items: [{ orderId: 'o1', status: 'pending', total: 10 }],
              deletes: [{ orderId: 'o2', status: 'cancelled', total: 0 }],
            }),
          ),
          userEntity.prepare(new BatchWrite({ deletes: [{ userId: 'u3', name: 'Charlie' }] })),
        ],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const requestItems = (dynamoMock.calls()[0].args[0].input as any).RequestItems.TestTable
    expect(requestItems).toHaveLength(5)
    expect(requestItems[0]).toHaveProperty('PutRequest.Item.userId', 'u1')
    expect(requestItems[1]).toHaveProperty('PutRequest.Item.userId', 'u2')
    expect(requestItems[2]).toHaveProperty('PutRequest.Item.orderId', 'o1')
    expect(requestItems[3]).toHaveProperty('DeleteRequest.Key.PK', 'ORDER#o2')
    expect(requestItems[4]).toHaveProperty('DeleteRequest.Key.PK', 'USER#u3')
  })

  it('should pass returnConsumedCapacity to the underlying DynamoDB command', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} })

    await table.send(
      new TableBatchWrite({
        writes: [userEntity.prepare(new BatchWrite({ items: [{ userId: 'u1', name: 'Alice' }] }))],
        returnConsumedCapacity: 'INDEXES',
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({ ReturnConsumedCapacity: 'INDEXES' }),
    )
  })

  it('should pass returnItemCollectionMetrics to the underlying DynamoDB command', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} })

    await table.send(
      new TableBatchWrite({
        writes: [userEntity.prepare(new BatchWrite({ items: [{ userId: 'u1', name: 'Alice' }] }))],
        returnItemCollectionMetrics: 'SIZE',
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({ ReturnItemCollectionMetrics: 'SIZE' }),
    )
  })

  // ---------------------------------------------------------------------------
  // Response / result
  // ---------------------------------------------------------------------------

  it('should return response metadata and consumed capacity', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({
      UnprocessedItems: {},
      ConsumedCapacity: [{ TableName: 'TestTable', CapacityUnits: 2 }],
      $metadata: { requestId: 'abc-123', httpStatusCode: 200 },
    })

    const result = await table.send(
      new TableBatchWrite({
        writes: [userEntity.prepare(new BatchWrite({ items: [{ userId: 'u1', name: 'Alice' }] }))],
      }),
    )

    expect(result.responseMetadata).toEqual({ requestId: 'abc-123', httpStatusCode: 200 })
    expect(result.consumedCapacity).toEqual({ TableName: 'TestTable', CapacityUnits: 2 })
  })

  it('should return undefined unprocessedPuts and unprocessedDeletes when all items are processed', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} })

    const result = await table.send(
      new TableBatchWrite({
        writes: [
          userEntity.prepare(new BatchWrite({ items: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchWrite({ items: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
          ),
        ],
      }),
    )

    const [userUnprocessedPuts, orderUnprocessedPuts] = result.unprocessedPuts
    const [userUnprocessedDeletes, orderUnprocessedDeletes] = result.unprocessedDeletes

    expect(userUnprocessedPuts).toBeUndefined()
    expect(orderUnprocessedPuts).toBeUndefined()
    expect(userUnprocessedDeletes).toBeUndefined()
    expect(orderUnprocessedDeletes).toBeUndefined()
  })

  // ---------------------------------------------------------------------------
  // Unprocessed items — mapping back to entity groups
  // ---------------------------------------------------------------------------

  it('should map unprocessed puts back to the correct entity group', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({
      UnprocessedItems: {
        TestTable: [
          {
            PutRequest: {
              Item: { PK: 'ORDER#o1', SK: 'METADATA', orderId: 'o1', status: 'pending', total: 99 },
            },
          },
        ],
      },
    })

    const result = await table.send(
      new TableBatchWrite({
        writes: [
          userEntity.prepare(new BatchWrite({ items: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchWrite({ items: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
          ),
        ],
      }),
    )

    const [userUnprocessedPuts, orderUnprocessedPuts] = result.unprocessedPuts
    expect(userUnprocessedPuts).toBeUndefined()
    expect(orderUnprocessedPuts).toEqual([{ orderId: 'o1', status: 'pending', total: 99 }])
  })

  it('should map unprocessed deletes back to the correct entity group', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({
      UnprocessedItems: {
        TestTable: [
          {
            DeleteRequest: {
              Key: { PK: 'USER#u1', SK: 'METADATA' },
            },
          },
        ],
      },
    })

    const result = await table.send(
      new TableBatchWrite({
        writes: [
          userEntity.prepare(new BatchWrite({ deletes: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchWrite({ deletes: [{ orderId: 'o1', status: 'pending', total: 0 }] }),
          ),
        ],
      }),
    )

    const [userUnprocessedDeletes, orderUnprocessedDeletes] = result.unprocessedDeletes
    expect(userUnprocessedDeletes).toEqual([{ userId: 'u1', name: 'Alice' }])
    expect(orderUnprocessedDeletes).toBeUndefined()
  })

  it('should map mixed unprocessed puts and deletes to their respective entity groups', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({
      UnprocessedItems: {
        TestTable: [
          {
            PutRequest: {
              Item: { PK: 'USER#u2', SK: 'METADATA', userId: 'u2', name: 'Bob' },
            },
          },
          {
            DeleteRequest: {
              Key: { PK: 'ORDER#o1', SK: 'METADATA' },
            },
          },
        ],
      },
    })

    const result = await table.send(
      new TableBatchWrite({
        writes: [
          userEntity.prepare(
            new BatchWrite({
              items: [
                { userId: 'u1', name: 'Alice' },
                { userId: 'u2', name: 'Bob' },
              ],
            }),
          ),
          orderEntity.prepare(
            new BatchWrite({
              deletes: [{ orderId: 'o1', status: 'pending', total: 0 }],
            }),
          ),
        ],
      }),
    )

    const [userUnprocessedPuts, orderUnprocessedPuts] = result.unprocessedPuts
    const [userUnprocessedDeletes, orderUnprocessedDeletes] = result.unprocessedDeletes

    expect(userUnprocessedPuts).toEqual([{ userId: 'u2', name: 'Bob' }])
    expect(orderUnprocessedPuts).toBeUndefined()
    expect(userUnprocessedDeletes).toBeUndefined()
    expect(orderUnprocessedDeletes).toEqual([{ orderId: 'o1', status: 'pending', total: 0 }])
  })

  it('should handle multiple unprocessed items in the same entity group', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({
      UnprocessedItems: {
        TestTable: [
          {
            PutRequest: {
              Item: { PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' },
            },
          },
          {
            PutRequest: {
              Item: { PK: 'USER#u2', SK: 'METADATA', userId: 'u2', name: 'Bob' },
            },
          },
        ],
      },
    })

    const result = await table.send(
      new TableBatchWrite({
        writes: [
          userEntity.prepare(
            new BatchWrite({
              items: [
                { userId: 'u1', name: 'Alice' },
                { userId: 'u2', name: 'Bob' },
                { userId: 'u3', name: 'Charlie' },
              ],
            }),
          ),
        ],
      }),
    )

    const [userUnprocessedPuts] = result.unprocessedPuts
    expect(userUnprocessedPuts).toHaveLength(2)
    expect(userUnprocessedPuts).toEqual([
      { userId: 'u1', name: 'Alice' },
      { userId: 'u2', name: 'Bob' },
    ])
  })

  // ---------------------------------------------------------------------------
  // Validation
  // ---------------------------------------------------------------------------

  it('should validate items against the schema before writing', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} })

    await expect(
      table.send(
        new TableBatchWrite({
          writes: [
            userEntity.prepare(
              // @ts-expect-error intentionally invalid: name should be string
              new BatchWrite({ items: [{ userId: 'u1', name: 123 }] }),
            ),
          ],
        }),
      ),
    ).rejects.toThrow(ZodError)
  })

  it('should skip validation when skipValidation is true', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} })

    await expect(
      table.send(
        new TableBatchWrite({
          writes: [
            userEntity.prepare(
              // @ts-expect-error intentionally invalid: name should be string
              new BatchWrite({ items: [{ userId: 'u1', name: 123 }] }),
            ),
          ],
          skipValidation: true,
        }),
      ),
    ).resolves.not.toThrow()
  })

  // ---------------------------------------------------------------------------
  // Table mismatch error
  // ---------------------------------------------------------------------------

  it('should throw DocumentBuilderError when an entity belongs to a different table', async () => {
    const otherTable = new DynamoTable({ tableName: 'OtherTable', documentClient })
    const otherEntity = new DynamoEntity({
      table: otherTable,
      schema: z.object({ id: z.string() }),
      partitionKey: item => key('OTHER', item.id),
    })

    await expect(
      table.send(
        new TableBatchWrite({
          writes: [
            userEntity.prepare(new BatchWrite({ items: [{ userId: 'u1', name: 'Alice' }] })),
            // @ts-expect-error intentionally wrong table
            otherEntity.prepare(new BatchWrite({ items: [{ id: 'x1' }] })),
          ],
        }),
      ),
    ).rejects.toThrow(DocumentBuilderError)
  })

  it('should include the mismatched table names in the error message', async () => {
    const otherTable = new DynamoTable({ tableName: 'OtherTable', documentClient })
    const otherEntity = new DynamoEntity({
      table: otherTable,
      schema: z.object({ id: z.string() }),
      partitionKey: item => key('OTHER', item.id),
    })

    await expect(
      table.send(
        new TableBatchWrite({
          writes: [
            userEntity.prepare(new BatchWrite({ items: [{ userId: 'u1', name: 'Alice' }] })),
            // @ts-expect-error intentionally wrong table
            otherEntity.prepare(new BatchWrite({ items: [{ id: 'x1' }] })),
          ],
        }),
      ),
    ).rejects.toThrow('OtherTable')
  })

  // ---------------------------------------------------------------------------
  // Edge cases
  // ---------------------------------------------------------------------------

  it('should handle a single entity group with only deletes', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} })

    await table.send(
      new TableBatchWrite({
        writes: [
          userEntity.prepare(
            new BatchWrite({
              deletes: [
                { userId: 'u1', name: 'Alice' },
                { userId: 'u2', name: 'Bob' },
              ],
            }),
          ),
        ],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const requestItems = (dynamoMock.calls()[0].args[0].input as any).RequestItems.TestTable
    expect(requestItems).toHaveLength(2)
    expect(requestItems[0]).toHaveProperty('DeleteRequest.Key.PK', 'USER#u1')
    expect(requestItems[1]).toHaveProperty('DeleteRequest.Key.PK', 'USER#u2')
  })

  it('should handle three entity groups simultaneously', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} })

    await table.send(
      new TableBatchWrite({
        writes: [
          userEntity.prepare(new BatchWrite({ items: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchWrite({ items: [{ orderId: 'o1', status: 'pending', total: 50 }] }),
          ),
          productEntity.prepare(new BatchWrite({ items: [{ productId: 'p1', price: 19.99 }] })),
        ],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const requestItems = (dynamoMock.calls()[0].args[0].input as any).RequestItems.TestTable
    expect(requestItems).toHaveLength(3)
    expect(requestItems[0]).toHaveProperty('PutRequest.Item.userId', 'u1')
    expect(requestItems[1]).toHaveProperty('PutRequest.Item.orderId', 'o1')
    expect(requestItems[2]).toHaveProperty('PutRequest.Item.productId', 'p1')
  })

  // ---------------------------------------------------------------------------
  // Abort / timeout
  // ---------------------------------------------------------------------------

  it('should handle abort signal and timeout options without throwing', async () => {
    const abortController = new AbortController()
    dynamoMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} })

    const result = await table.send(
      new TableBatchWrite({
        writes: [userEntity.prepare(new BatchWrite({ items: [{ userId: 'u1', name: 'Alice' }] }))],
        abortController,
        timeoutMs: 5000,
      }),
    )

    expect(result).toBeDefined()
  })

  // ---------------------------------------------------------------------------
  // itemCollectionMetrics in result
  // ---------------------------------------------------------------------------

  it('should include itemCollectionMetrics in the result when returned by DynamoDB', async () => {
    const metrics = {
      TestTable: [{ ItemCollectionKey: { PK: 'USER#u1' }, SizeEstimateRangeGB: [1, 2] }],
    }

    dynamoMock.on(BatchWriteCommand).resolves({
      UnprocessedItems: {},
      ItemCollectionMetrics: metrics,
    })

    const result = await table.send(
      new TableBatchWrite({
        writes: [userEntity.prepare(new BatchWrite({ items: [{ userId: 'u1', name: 'Alice' }] }))],
        returnItemCollectionMetrics: 'SIZE',
      }),
    )

    expect(result.itemCollectionMetrics).toEqual(metrics)
  })

  it('should return undefined consumed capacity and itemCollectionMetrics when DynamoDB omits them', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({
      UnprocessedItems: {},
      $metadata: { httpStatusCode: 200 },
    })

    const result = await table.send(
      new TableBatchWrite({
        writes: [userEntity.prepare(new BatchWrite({ items: [{ userId: 'u1', name: 'Alice' }] }))],
      }),
    )

    expect(result.consumedCapacity).toBeUndefined()
    expect(result.itemCollectionMetrics).toBeUndefined()
  })

  // ---------------------------------------------------------------------------
  // Table mismatch — non-first position
  // ---------------------------------------------------------------------------

  it('should throw DocumentBuilderError when the mismatched entity is the third in the list', async () => {
    const otherTable = new DynamoTable({ tableName: 'OtherTable', documentClient })
    const otherEntity = new DynamoEntity({
      table: otherTable,
      schema: z.object({ id: z.string() }),
      partitionKey: item => key('OTHER', item.id),
    })

    await expect(
      table.send(
        new TableBatchWrite({
          writes: [
            userEntity.prepare(new BatchWrite({ items: [{ userId: 'u1', name: 'Alice' }] })),
            orderEntity.prepare(
              new BatchWrite({ items: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
            ),
            // @ts-expect-error intentionally wrong table
            otherEntity.prepare(new BatchWrite({ items: [{ id: 'x1' }] })),
          ],
        }),
      ),
    ).rejects.toThrow(DocumentBuilderError)
  })

  // ---------------------------------------------------------------------------
  // Key building edge cases
  // ---------------------------------------------------------------------------

  it('should build keys using a custom partition key name', async () => {
    const customTable = new DynamoTable({
      tableName: 'CustomTable',
      documentClient,
      keyNames: { partitionKey: 'id', sortKey: null },
    })

    const customEntity = new DynamoEntity({
      table: customTable,
      schema: z.object({ userId: z.string(), name: z.string() }),
      partitionKey: item => key('USER', item.userId),
    })

    dynamoMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} })

    await customTable.send(
      new TableBatchWrite({
        writes: [
          customEntity.prepare(new BatchWrite({ items: [{ userId: 'u1', name: 'Alice' }] })),
        ],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const requestItems = (dynamoMock.calls()[0].args[0].input as any).RequestItems.CustomTable
    expect(requestItems[0]).toHaveProperty('PutRequest.Item.id', 'USER#u1')
    expect(requestItems[0].PutRequest?.Item?.SK).toBeUndefined()
    expect(requestItems[0].PutRequest?.Item?.PK).toBeUndefined()
  })

  it('should use only a partition key when the entity has no sort key builder', async () => {
    const simplePKEntity = new DynamoEntity({
      table,
      schema: z.object({ tagId: z.string(), label: z.string() }),
      partitionKey: item => key('TAG', item.tagId),
    })

    dynamoMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} })

    await table.send(
      new TableBatchWrite({
        writes: [
          simplePKEntity.prepare(new BatchWrite({ items: [{ tagId: 't1', label: 'promo' }] })),
        ],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const requestItems = (dynamoMock.calls()[0].args[0].input as any).RequestItems.TestTable
    expect(requestItems[0]).toHaveProperty('PutRequest.Item.PK', 'TAG#t1')
    // SK should be absent — not forced to undefined
    expect(requestItems[0].PutRequest?.Item?.SK).toBeUndefined()
  })

  // ---------------------------------------------------------------------------
  // Validation — second entity group
  // ---------------------------------------------------------------------------

  it('should throw ZodError when a second-group item fails schema validation', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} })

    await expect(
      table.send(
        new TableBatchWrite({
          writes: [
            userEntity.prepare(new BatchWrite({ items: [{ userId: 'u1', name: 'Alice' }] })),
            orderEntity.prepare(
              // @ts-expect-error intentionally invalid: total should be number
              new BatchWrite({ items: [{ orderId: 'o1', status: 'pending', total: 'bad' }] }),
            ),
          ],
        }),
      ),
    ).rejects.toThrow(ZodError)
  })

  it('should pass raw data through when skipValidation is true (even for invalid data)', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} })

    // Should not throw despite name being invalid type
    await expect(
      table.send(
        new TableBatchWrite({
          writes: [
            userEntity.prepare(
              // @ts-expect-error intentionally invalid
              new BatchWrite({ items: [{ userId: 'u1', name: 999 }] }),
            ),
          ],
          skipValidation: true,
        }),
      ),
    ).resolves.toBeDefined()

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const requestItems = (dynamoMock.calls()[0].args[0].input as any).RequestItems.TestTable
    expect(requestItems[0]).toHaveProperty('PutRequest.Item.name', 999)
  })

  // ---------------------------------------------------------------------------
  // Single entity group
  // ---------------------------------------------------------------------------

  it('should handle a single entity group with a single put', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({ UnprocessedItems: {} })

    await table.send(
      new TableBatchWrite({
        writes: [userEntity.prepare(new BatchWrite({ items: [{ userId: 'u1', name: 'Alice' }] }))],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const requestItems = (dynamoMock.calls()[0].args[0].input as any).RequestItems.TestTable
    expect(requestItems).toHaveLength(1)
    expect(requestItems[0]).toHaveProperty('PutRequest.Item.userId', 'u1')
  })

  it('should distribute unprocessed puts across three entity groups correctly', async () => {
    dynamoMock.on(BatchWriteCommand).resolves({
      UnprocessedItems: {
        TestTable: [
          {
            PutRequest: {
              Item: { PK: 'PRODUCT#p1', SK: 'METADATA', productId: 'p1', price: 19.99 },
            },
          },
          {
            PutRequest: {
              Item: { PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' },
            },
          },
        ],
      },
    })

    const result = await table.send(
      new TableBatchWrite({
        writes: [
          userEntity.prepare(new BatchWrite({ items: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchWrite({ items: [{ orderId: 'o1', status: 'pending', total: 50 }] }),
          ),
          productEntity.prepare(new BatchWrite({ items: [{ productId: 'p1', price: 19.99 }] })),
        ],
      }),
    )

    const [userUnprocessedPuts, orderUnprocessedPuts, productUnprocessedPuts] =
      result.unprocessedPuts
    expect(userUnprocessedPuts).toEqual([{ userId: 'u1', name: 'Alice' }])
    expect(orderUnprocessedPuts).toBeUndefined()
    expect(productUnprocessedPuts).toEqual([{ productId: 'p1', price: 19.99 }])
  })
})
