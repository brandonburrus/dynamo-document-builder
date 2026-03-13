import { describe, it, expect, beforeEach } from 'vitest'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, BatchGetCommand } from '@aws-sdk/lib-dynamodb'
import { DynamoEntity, DynamoTable, key } from '@/core'
import { BatchGet, TableBatchGet } from '@/commands'
import { DocumentBuilderError } from '@/errors'
import { mockClient } from 'aws-sdk-client-mock'
import { z, ZodError } from 'zod'

const dynamoClient = new DynamoDBClient()
const documentClient = DynamoDBDocumentClient.from(dynamoClient)
const dynamoMock = mockClient(DynamoDBDocumentClient)

describe('TableBatchGet Command', () => {
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

  it('should build a batch get operation across multiple entity types', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          { PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' },
          { PK: 'ORDER#o1', SK: 'METADATA', orderId: 'o1', status: 'pending', total: 99 },
        ],
      },
    })

    await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
          ),
        ],
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        RequestItems: {
          TestTable: {
            Keys: [
              { PK: 'USER#u1', SK: 'METADATA' },
              { PK: 'ORDER#o1', SK: 'METADATA' },
            ],
            ConsistentRead: false,
          },
        },
      }),
    )
  })

  it('should send all keys in a single RequestItems entry for the table', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: { TestTable: [] },
    })

    await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(
            new BatchGet({
              keys: [
                { userId: 'u1', name: 'Alice' },
                { userId: 'u2', name: 'Bob' },
              ],
            }),
          ),
          orderEntity.prepare(
            new BatchGet({
              keys: [
                { orderId: 'o1', status: 'pending', total: 10 },
                { orderId: 'o2', status: 'shipped', total: 20 },
              ],
            }),
          ),
        ],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const keys = (dynamoMock.calls()[0].args[0].input as any).RequestItems.TestTable.Keys
    expect(keys).toHaveLength(4)
    expect(keys[0]).toEqual({ PK: 'USER#u1', SK: 'METADATA' })
    expect(keys[1]).toEqual({ PK: 'USER#u2', SK: 'METADATA' })
    expect(keys[2]).toEqual({ PK: 'ORDER#o1', SK: 'METADATA' })
    expect(keys[3]).toEqual({ PK: 'ORDER#o2', SK: 'METADATA' })
  })

  it('should use ConsistentRead: false when no group requests consistent reads', async () => {
    dynamoMock.on(BatchGetCommand).resolves({ Responses: { TestTable: [] } })

    await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
          ),
        ],
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        RequestItems: expect.objectContaining({
          TestTable: expect.objectContaining({ ConsistentRead: false }),
        }),
      }),
    )
  })

  it('should use ConsistentRead: true when any group requests consistent reads', async () => {
    dynamoMock.on(BatchGetCommand).resolves({ Responses: { TestTable: [] } })

    await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(
            new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }], consistent: true }),
          ),
          orderEntity.prepare(
            new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
          ),
        ],
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        RequestItems: expect.objectContaining({
          TestTable: expect.objectContaining({ ConsistentRead: true }),
        }),
      }),
    )
  })

  it('should pass returnConsumedCapacity to the underlying DynamoDB command', async () => {
    dynamoMock.on(BatchGetCommand).resolves({ Responses: { TestTable: [] } })

    await table.send(
      new TableBatchGet({
        gets: [userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] }))],
        returnConsumedCapacity: 'INDEXES',
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({ ReturnConsumedCapacity: 'INDEXES' }),
    )
  })

  // ---------------------------------------------------------------------------
  // Response grouping / tuple structure
  // ---------------------------------------------------------------------------

  it('should return items grouped by entity in a tuple structure', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          { PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' },
          { PK: 'ORDER#o1', SK: 'METADATA', orderId: 'o1', status: 'pending', total: 99 },
        ],
      },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
          ),
        ],
      }),
    )

    const [users, orders] = result.items
    expect(users).toEqual([{ userId: 'u1', name: 'Alice' }])
    expect(orders).toEqual([{ orderId: 'o1', status: 'pending', total: 99 }])
  })

  it('should correctly group results across three entity types', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          { PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' },
          { PK: 'ORDER#o1', SK: 'METADATA', orderId: 'o1', status: 'pending', total: 99 },
          { PK: 'PRODUCT#p1', SK: 'METADATA', productId: 'p1', price: 49.99 },
        ],
      },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
          ),
          productEntity.prepare(new BatchGet({ keys: [{ productId: 'p1', price: 49.99 }] })),
        ],
      }),
    )

    const [users, orders, products] = result.items
    expect(users).toEqual([{ userId: 'u1', name: 'Alice' }])
    expect(orders).toEqual([{ orderId: 'o1', status: 'pending', total: 99 }])
    expect(products).toEqual([{ productId: 'p1', price: 49.99 }])
  })

  it('should handle multiple items returned for the same entity group', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          { PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' },
          { PK: 'USER#u2', SK: 'METADATA', userId: 'u2', name: 'Bob' },
          { PK: 'USER#u3', SK: 'METADATA', userId: 'u3', name: 'Charlie' },
        ],
      },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(
            new BatchGet({
              keys: [
                { userId: 'u1', name: 'Alice' },
                { userId: 'u2', name: 'Bob' },
                { userId: 'u3', name: 'Charlie' },
              ],
            }),
          ),
        ],
      }),
    )

    const [users] = result.items
    expect(users).toHaveLength(3)
    expect(users[0]).toEqual({ userId: 'u1', name: 'Alice' })
    expect(users[1]).toEqual({ userId: 'u2', name: 'Bob' })
    expect(users[2]).toEqual({ userId: 'u3', name: 'Charlie' })
  })

  it('should correctly group results when DynamoDB returns items out of request order', async () => {
    // DynamoDB does not guarantee order in batch get responses
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          // Order entity comes back first, users second
          { PK: 'ORDER#o1', SK: 'METADATA', orderId: 'o1', status: 'shipped', total: 50 },
          { PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' },
          { PK: 'USER#u2', SK: 'METADATA', userId: 'u2', name: 'Bob' },
        ],
      },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(
            new BatchGet({
              keys: [
                { userId: 'u1', name: 'Alice' },
                { userId: 'u2', name: 'Bob' },
              ],
            }),
          ),
          orderEntity.prepare(
            new BatchGet({ keys: [{ orderId: 'o1', status: 'shipped', total: 50 }] }),
          ),
        ],
      }),
    )

    const [users, orders] = result.items
    expect(users).toHaveLength(2)
    expect(orders).toHaveLength(1)
    expect(users[0]).toEqual({ userId: 'u1', name: 'Alice' })
    expect(users[1]).toEqual({ userId: 'u2', name: 'Bob' })
    expect(orders[0]).toEqual({ orderId: 'o1', status: 'shipped', total: 50 })
  })

  it('should return empty arrays for entity groups with no matching responses', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [{ PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' }],
      },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
          ),
        ],
      }),
    )

    const [users, orders] = result.items
    expect(users).toEqual([{ userId: 'u1', name: 'Alice' }])
    expect(orders).toEqual([])
  })

  it('should return response metadata and consumed capacity', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: { TestTable: [] },
      ConsumedCapacity: [{ TableName: 'TestTable', CapacityUnits: 2 }],
      $metadata: { requestId: 'req-abc', httpStatusCode: 200 },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] }))],
      }),
    )

    expect(result.responseMetadata).toEqual({ requestId: 'req-abc', httpStatusCode: 200 })
    expect(result.consumedCapacity).toEqual({ TableName: 'TestTable', CapacityUnits: 2 })
  })

  // ---------------------------------------------------------------------------
  // Unprocessed keys — mapping back to entity groups
  // ---------------------------------------------------------------------------

  it('should return undefined unprocessedKeys per entity when all keys are processed', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [{ PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' }],
      },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
          ),
        ],
      }),
    )

    const [userUnprocessedKeys, orderUnprocessedKeys] = result.unprocessedKeys
    expect(userUnprocessedKeys).toBeUndefined()
    expect(orderUnprocessedKeys).toBeUndefined()
  })

  it('should map unprocessed keys back to the correct entity group', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: { TestTable: [] },
      UnprocessedKeys: {
        TestTable: {
          Keys: [{ PK: 'ORDER#o1', SK: 'METADATA' }],
        },
      },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
          ),
        ],
      }),
    )

    const [userUnprocessedKeys, orderUnprocessedKeys] = result.unprocessedKeys
    expect(userUnprocessedKeys).toBeUndefined()
    expect(orderUnprocessedKeys).toEqual([{ orderId: 'o1', status: 'pending', total: 10 }])
  })

  it('should map unprocessed keys across multiple entity groups', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: { TestTable: [] },
      UnprocessedKeys: {
        TestTable: {
          Keys: [
            { PK: 'USER#u2', SK: 'METADATA' },
            { PK: 'ORDER#o1', SK: 'METADATA' },
          ],
        },
      },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(
            new BatchGet({
              keys: [
                { userId: 'u1', name: 'Alice' },
                { userId: 'u2', name: 'Bob' },
              ],
            }),
          ),
          orderEntity.prepare(
            new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
          ),
        ],
      }),
    )

    const [userUnprocessedKeys, orderUnprocessedKeys] = result.unprocessedKeys
    expect(userUnprocessedKeys).toEqual([{ userId: 'u2', name: 'Bob' }])
    expect(orderUnprocessedKeys).toEqual([{ orderId: 'o1', status: 'pending', total: 10 }])
  })

  // ---------------------------------------------------------------------------
  // Validation
  // ---------------------------------------------------------------------------

  it('should validate returned items against their entity schema', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          // name is missing — invalid
          { PK: 'USER#u1', SK: 'METADATA', userId: 'u1' },
        ],
      },
    })

    await expect(
      table.send(
        new TableBatchGet({
          gets: [userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] }))],
        }),
      ),
    ).rejects.toThrow(ZodError)
  })

  it('should skip validation when skipValidation is true', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          // name is missing — would fail schema validation
          { PK: 'USER#u1', SK: 'METADATA', userId: 'u1' },
        ],
      },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] }))],
        skipValidation: true,
      }),
    )

    const [users] = result.items
    expect(users).toHaveLength(1)
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
        new TableBatchGet({
          gets: [
            userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
            // @ts-expect-error intentionally wrong table
            otherEntity.prepare(new BatchGet({ keys: [{ id: 'x1' }] })),
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
        new TableBatchGet({
          gets: [
            userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
            // @ts-expect-error intentionally wrong table
            otherEntity.prepare(new BatchGet({ keys: [{ id: 'x1' }] })),
          ],
        }),
      ),
    ).rejects.toThrow('OtherTable')
  })

  // ---------------------------------------------------------------------------
  // Edge cases
  // ---------------------------------------------------------------------------

  it('should handle empty responses gracefully', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: { TestTable: [] },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
          ),
        ],
      }),
    )

    const [users, orders] = result.items
    expect(users).toEqual([])
    expect(orders).toEqual([])
  })

  it('should handle a single entity group with multiple keys', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          { PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' },
          { PK: 'USER#u2', SK: 'METADATA', userId: 'u2', name: 'Bob' },
        ],
      },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(
            new BatchGet({
              keys: [
                { userId: 'u1', name: 'Alice' },
                { userId: 'u2', name: 'Bob' },
              ],
            }),
          ),
        ],
      }),
    )

    const [users] = result.items
    expect(users).toHaveLength(2)
    expect(users[0]).toEqual({ userId: 'u1', name: 'Alice' })
    expect(users[1]).toEqual({ userId: 'u2', name: 'Bob' })
  })

  it('should handle asymmetric key counts across entity groups', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          { PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' },
          { PK: 'USER#u2', SK: 'METADATA', userId: 'u2', name: 'Bob' },
          { PK: 'USER#u3', SK: 'METADATA', userId: 'u3', name: 'Charlie' },
          { PK: 'ORDER#o1', SK: 'METADATA', orderId: 'o1', status: 'pending', total: 10 },
        ],
      },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(
            new BatchGet({
              keys: [
                { userId: 'u1', name: 'Alice' },
                { userId: 'u2', name: 'Bob' },
                { userId: 'u3', name: 'Charlie' },
              ],
            }),
          ),
          orderEntity.prepare(
            new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
          ),
        ],
      }),
    )

    const [users, orders] = result.items
    expect(users).toHaveLength(3)
    expect(orders).toHaveLength(1)
  })

  // ---------------------------------------------------------------------------
  // Abort / timeout
  // ---------------------------------------------------------------------------

  it('should handle abort signal and timeout options without throwing', async () => {
    const abortController = new AbortController()
    dynamoMock.on(BatchGetCommand).resolves({ Responses: { TestTable: [] } })

    const result = await table.send(
      new TableBatchGet({
        gets: [userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] }))],
        abortController,
        timeoutMs: 5000,
      }),
    )

    expect(result).toBeDefined()
  })

  // ---------------------------------------------------------------------------
  // Response metadata
  // ---------------------------------------------------------------------------

  it('should return undefined consumed capacity when DynamoDB omits it', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: { TestTable: [{ PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' }] },
      $metadata: { httpStatusCode: 200 },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] }))],
      }),
    )

    expect(result.consumedCapacity).toBeUndefined()
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
        new TableBatchGet({
          gets: [
            userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
            orderEntity.prepare(
              new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
            ),
            // @ts-expect-error intentionally wrong table
            otherEntity.prepare(new BatchGet({ keys: [{ id: 'x1' }] })),
          ],
        }),
      ),
    ).rejects.toThrow(DocumentBuilderError)
  })

  // ---------------------------------------------------------------------------
  // Same entity type in multiple groups
  // ---------------------------------------------------------------------------

  it('should correctly group results when the same entity type appears in multiple get groups', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          { PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' },
          { PK: 'USER#u2', SK: 'METADATA', userId: 'u2', name: 'Bob' },
        ],
      },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          userEntity.prepare(new BatchGet({ keys: [{ userId: 'u2', name: 'Bob' }] })),
        ],
      }),
    )

    const [group1, group2] = result.items
    expect(group1).toHaveLength(1)
    expect(group1[0]).toEqual({ userId: 'u1', name: 'Alice' })
    expect(group2).toHaveLength(1)
    expect(group2[0]).toEqual({ userId: 'u2', name: 'Bob' })
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

    dynamoMock.on(BatchGetCommand).resolves({
      Responses: { CustomTable: [{ id: 'USER#u1', userId: 'u1', name: 'Alice' }] },
    })

    await customTable.send(
      new TableBatchGet({
        gets: [customEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] }))],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const keys = (dynamoMock.calls()[0].args[0].input as any).RequestItems.CustomTable.Keys
    expect(keys[0]).toEqual({ id: 'USER#u1' })
    expect(keys[0].PK).toBeUndefined()
    expect(keys[0].SK).toBeUndefined()
  })

  it('should use only a partition key when the entity has no sort key builder', async () => {
    const simplePKEntity = new DynamoEntity({
      table,
      schema: z.object({ tagId: z.string(), label: z.string() }),
      partitionKey: item => key('TAG', item.tagId),
    })

    dynamoMock.on(BatchGetCommand).resolves({
      Responses: { TestTable: [{ PK: 'TAG#t1', tagId: 't1', label: 'promo' }] },
    })

    await table.send(
      new TableBatchGet({
        gets: [simplePKEntity.prepare(new BatchGet({ keys: [{ tagId: 't1', label: 'promo' }] }))],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const keys = (dynamoMock.calls()[0].args[0].input as any).RequestItems.TestTable.Keys
    expect(keys[0]).toEqual({ PK: 'TAG#t1' })
    expect(keys[0].SK).toBeUndefined()
  })

  // ---------------------------------------------------------------------------
  // ConsistentRead — all groups
  // ---------------------------------------------------------------------------

  it('should use ConsistentRead: true when all groups request consistent reads', async () => {
    dynamoMock.on(BatchGetCommand).resolves({ Responses: { TestTable: [] } })

    await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(
            new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }], consistent: true }),
          ),
          orderEntity.prepare(
            new BatchGet({
              keys: [{ orderId: 'o1', status: 'pending', total: 10 }],
              consistent: true,
            }),
          ),
        ],
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        RequestItems: expect.objectContaining({
          TestTable: expect.objectContaining({ ConsistentRead: true }),
        }),
      }),
    )
  })

  // ---------------------------------------------------------------------------
  // ConsistentRead — command-level option
  // ---------------------------------------------------------------------------

  it('should use ConsistentRead: true when command-level consistent is true and no groups set it', async () => {
    dynamoMock.on(BatchGetCommand).resolves({ Responses: { TestTable: [] } })

    await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
          ),
        ],
        consistent: true,
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        RequestItems: expect.objectContaining({
          TestTable: expect.objectContaining({ ConsistentRead: true }),
        }),
      }),
    )
  })

  it('should use ConsistentRead: true when command-level consistent: true overrides groups with consistent: false', async () => {
    dynamoMock.on(BatchGetCommand).resolves({ Responses: { TestTable: [] } })

    await table.send(
      new TableBatchGet({
        gets: [
          // both groups explicitly set consistent: false
          userEntity.prepare(
            new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }], consistent: false }),
          ),
          orderEntity.prepare(
            new BatchGet({
              keys: [{ orderId: 'o1', status: 'pending', total: 10 }],
              consistent: false,
            }),
          ),
        ],
        consistent: true, // command-level should win
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        RequestItems: expect.objectContaining({
          TestTable: expect.objectContaining({ ConsistentRead: true }),
        }),
      }),
    )
  })

  it('should use ConsistentRead: false when command-level consistent: false overrides a group with consistent: true', async () => {
    dynamoMock.on(BatchGetCommand).resolves({ Responses: { TestTable: [] } })

    await table.send(
      new TableBatchGet({
        gets: [
          // one group requests consistent reads
          userEntity.prepare(
            new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }], consistent: true }),
          ),
          orderEntity.prepare(
            new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
          ),
        ],
        consistent: false, // command-level false should override the group-level true
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        RequestItems: expect.objectContaining({
          TestTable: expect.objectContaining({ ConsistentRead: false }),
        }),
      }),
    )
  })

  // ---------------------------------------------------------------------------
  // Responses key absent from DynamoDB response
  // ---------------------------------------------------------------------------

  it('should return empty arrays when Responses is absent from DynamoDB response', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      $metadata: { httpStatusCode: 200 },
      // no Responses key at all
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
          ),
        ],
      }),
    )

    const [users, orders] = result.items
    expect(users).toEqual([])
    expect(orders).toEqual([])
  })

  // ---------------------------------------------------------------------------
  // Validation — second entity group
  // ---------------------------------------------------------------------------

  it('should throw ZodError when a second-group item fails schema validation', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          { PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' },
          // total is a string — invalid for orderEntity schema
          { PK: 'ORDER#o1', SK: 'METADATA', orderId: 'o1', status: 'pending', total: 'bad' },
        ],
      },
    })

    await expect(
      table.send(
        new TableBatchGet({
          gets: [
            userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
            orderEntity.prepare(
              new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
            ),
          ],
        }),
      ),
    ).rejects.toThrow(ZodError)
  })

  it('should return raw invalid data when skipValidation is true', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: {
        TestTable: [
          // name is a number — would fail userEntity schema
          { PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 999 },
          // total is a string — would fail orderEntity schema
          { PK: 'ORDER#o1', SK: 'METADATA', orderId: 'o1', status: 'pending', total: 'bad' },
        ],
      },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
          ),
        ],
        skipValidation: true,
      }),
    )

    const [users, orders] = result.items
    expect(users[0]).toEqual(expect.objectContaining({ userId: 'u1', name: 999 }))
    expect(orders[0]).toEqual(expect.objectContaining({ orderId: 'o1', total: 'bad' }))
  })

  // ---------------------------------------------------------------------------
  // All keys unprocessed / multiple unprocessed keys in same group
  // ---------------------------------------------------------------------------

  it('should handle all keys being returned as unprocessed', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: { TestTable: [] },
      UnprocessedKeys: {
        TestTable: {
          Keys: [
            { PK: 'USER#u1', SK: 'METADATA' },
            { PK: 'ORDER#o1', SK: 'METADATA' },
          ],
        },
      },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(new BatchGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new BatchGet({ keys: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
          ),
        ],
      }),
    )

    const [users, orders] = result.items
    expect(users).toEqual([])
    expect(orders).toEqual([])

    const [userUnprocessedKeys, orderUnprocessedKeys] = result.unprocessedKeys
    expect(userUnprocessedKeys).toEqual([{ userId: 'u1', name: 'Alice' }])
    expect(orderUnprocessedKeys).toEqual([{ orderId: 'o1', status: 'pending', total: 10 }])
  })

  it('should handle multiple unprocessed keys in the same entity group', async () => {
    dynamoMock.on(BatchGetCommand).resolves({
      Responses: { TestTable: [] },
      UnprocessedKeys: {
        TestTable: {
          Keys: [
            { PK: 'USER#u1', SK: 'METADATA' },
            { PK: 'USER#u2', SK: 'METADATA' },
            { PK: 'USER#u3', SK: 'METADATA' },
          ],
        },
      },
    })

    const result = await table.send(
      new TableBatchGet({
        gets: [
          userEntity.prepare(
            new BatchGet({
              keys: [
                { userId: 'u1', name: 'Alice' },
                { userId: 'u2', name: 'Bob' },
                { userId: 'u3', name: 'Charlie' },
              ],
            }),
          ),
        ],
      }),
    )

    const [userUnprocessedKeys] = result.unprocessedKeys
    expect(userUnprocessedKeys).toHaveLength(3)
    expect(userUnprocessedKeys).toEqual([
      { userId: 'u1', name: 'Alice' },
      { userId: 'u2', name: 'Bob' },
      { userId: 'u3', name: 'Charlie' },
    ])
  })
})
