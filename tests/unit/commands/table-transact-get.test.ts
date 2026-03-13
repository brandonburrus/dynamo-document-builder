import { describe, it, expect, beforeEach } from 'vitest'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, TransactGetCommand } from '@aws-sdk/lib-dynamodb'
import { DynamoEntity, DynamoTable, key } from '@/core'
import { TableTransactGet, TransactGet } from '@/commands'
import { DocumentBuilderError } from '@/errors'
import { mockClient } from 'aws-sdk-client-mock'
import { z, ZodError } from 'zod'

const dynamoClient = new DynamoDBClient()
const documentClient = DynamoDBDocumentClient.from(dynamoClient)
const dynamoMock = mockClient(DynamoDBDocumentClient)

describe('TableTransactGet Command', () => {
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

  // ---------------------------------------------------------------------------
  // Request shape
  // ---------------------------------------------------------------------------

  it('should build a transact get operation across multiple entities', async () => {
    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [
        { Item: { userId: 'u1', name: 'Alice' } },
        { Item: { userId: 'u2', name: 'Bob' } },
        { Item: { orderId: 'o1', status: 'pending', total: 99 } },
      ],
    })

    await table.send(
      new TableTransactGet({
        gets: [
          userEntity.prepare(
            new TransactGet({
              keys: [
                { userId: 'u1', name: 'Alice' },
                { userId: 'u2', name: 'Bob' },
              ],
            }),
          ),
          orderEntity.prepare(
            new TransactGet({ keys: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
          ),
        ],
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TransactItems: [
          { Get: { TableName: 'TestTable', Key: { PK: 'USER#u1', SK: 'METADATA' } } },
          { Get: { TableName: 'TestTable', Key: { PK: 'USER#u2', SK: 'METADATA' } } },
          { Get: { TableName: 'TestTable', Key: { PK: 'ORDER#o1', SK: 'METADATA' } } },
        ],
      }),
    )
  })

  it('should send TransactGetItems in entity declaration order', async () => {
    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [
        { Item: { orderId: 'o1', status: 'pending', total: 10 } },
        { Item: { userId: 'u1', name: 'Alice' } },
      ],
    })

    await table.send(
      new TableTransactGet({
        gets: [
          orderEntity.prepare(
            new TransactGet({ keys: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
          ),
          userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
        ],
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TransactItems: [
          { Get: { TableName: 'TestTable', Key: { PK: 'ORDER#o1', SK: 'METADATA' } } },
          { Get: { TableName: 'TestTable', Key: { PK: 'USER#u1', SK: 'METADATA' } } },
        ],
      }),
    )
  })

  it('should pass returnConsumedCapacity to the underlying DynamoDB command', async () => {
    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [{ Item: { userId: 'u1', name: 'Alice' } }],
    })

    await table.send(
      new TableTransactGet({
        gets: [userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] }))],
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
    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [
        { Item: { userId: 'u1', name: 'Alice' } },
        { Item: { userId: 'u2', name: 'Bob' } },
        { Item: { orderId: 'o1', status: 'pending', total: 99 } },
      ],
    })

    const result = await table.send(
      new TableTransactGet({
        gets: [
          userEntity.prepare(
            new TransactGet({
              keys: [
                { userId: 'u1', name: 'Alice' },
                { userId: 'u2', name: 'Bob' },
              ],
            }),
          ),
          orderEntity.prepare(
            new TransactGet({ keys: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
          ),
        ],
      }),
    )

    expect(result.items[0]).toEqual([
      { userId: 'u1', name: 'Alice' },
      { userId: 'u2', name: 'Bob' },
    ])
    expect(result.items[1]).toEqual([{ orderId: 'o1', status: 'pending', total: 99 }])
  })

  it('should correctly split results between three entity groups', async () => {
    const productEntity = new DynamoEntity({
      table,
      schema: z.object({ productId: z.string(), price: z.number() }),
      partitionKey: item => key('PRODUCT', item.productId),
      sortKey: () => 'METADATA',
    })

    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [
        { Item: { userId: 'u1', name: 'Alice' } },
        { Item: { orderId: 'o1', status: 'pending', total: 99 } },
        { Item: { productId: 'p1', price: 49 } },
      ],
    })

    const result = await table.send(
      new TableTransactGet({
        gets: [
          userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new TransactGet({ keys: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
          ),
          productEntity.prepare(new TransactGet({ keys: [{ productId: 'p1', price: 49 }] })),
        ],
      }),
    )

    expect(result.items[0]).toEqual([{ userId: 'u1', name: 'Alice' }])
    expect(result.items[1]).toEqual([{ orderId: 'o1', status: 'pending', total: 99 }])
    expect(result.items[2]).toEqual([{ productId: 'p1', price: 49 }])
  })

  it('should handle a single entity with multiple keys', async () => {
    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [
        { Item: { userId: 'u1', name: 'Alice' } },
        { Item: { userId: 'u2', name: 'Bob' } },
        { Item: { userId: 'u3', name: 'Charlie' } },
      ],
    })

    const result = await table.send(
      new TableTransactGet({
        gets: [
          userEntity.prepare(
            new TransactGet({
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

    expect(result.items[0]).toEqual([
      { userId: 'u1', name: 'Alice' },
      { userId: 'u2', name: 'Bob' },
      { userId: 'u3', name: 'Charlie' },
    ])
  })

  it('should handle asymmetric key counts across entity groups', async () => {
    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [
        { Item: { userId: 'u1', name: 'Alice' } },
        { Item: { userId: 'u2', name: 'Bob' } },
        { Item: { userId: 'u3', name: 'Charlie' } },
        { Item: { orderId: 'o1', status: 'pending', total: 10 } },
      ],
    })

    const result = await table.send(
      new TableTransactGet({
        gets: [
          userEntity.prepare(
            new TransactGet({
              keys: [
                { userId: 'u1', name: 'Alice' },
                { userId: 'u2', name: 'Bob' },
                { userId: 'u3', name: 'Charlie' },
              ],
            }),
          ),
          orderEntity.prepare(
            new TransactGet({ keys: [{ orderId: 'o1', status: 'pending', total: 10 }] }),
          ),
        ],
      }),
    )

    expect(result.items[0]).toHaveLength(3)
    expect(result.items[1]).toHaveLength(1)
    expect(result.items[0][0]).toEqual({ userId: 'u1', name: 'Alice' })
    expect(result.items[1][0]).toEqual({ orderId: 'o1', status: 'pending', total: 10 })
  })

  // ---------------------------------------------------------------------------
  // Missing / undefined items
  // ---------------------------------------------------------------------------

  it('should handle missing items by returning undefined in the correct group position', async () => {
    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [
        { Item: { userId: 'u1', name: 'Alice' } },
        { Item: undefined },
        { Item: { orderId: 'o1', status: 'pending', total: 99 } },
      ],
    })

    const result = await table.send(
      new TableTransactGet({
        gets: [
          userEntity.prepare(
            new TransactGet({
              keys: [
                { userId: 'u1', name: 'Alice' },
                { userId: 'u2', name: 'Bob' },
              ],
            }),
          ),
          orderEntity.prepare(
            new TransactGet({ keys: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
          ),
        ],
      }),
    )

    expect(result.items[0]).toEqual([{ userId: 'u1', name: 'Alice' }, undefined])
    expect(result.items[1]).toEqual([{ orderId: 'o1', status: 'pending', total: 99 }])
  })

  it('should return all undefined when every item is missing', async () => {
    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [{ Item: undefined }, { Item: undefined }],
    })

    const result = await table.send(
      new TableTransactGet({
        gets: [
          userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new TransactGet({ keys: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
          ),
        ],
      }),
    )

    expect(result.items[0]).toEqual([undefined])
    expect(result.items[1]).toEqual([undefined])
  })

  it('should return empty arrays per group when DynamoDB returns no Responses', async () => {
    dynamoMock.on(TransactGetCommand).resolves({ Responses: [] })

    const result = await table.send(
      new TableTransactGet({
        gets: [
          userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new TransactGet({ keys: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
          ),
        ],
      }),
    )

    // An empty Responses array means no items came back — each group gets an empty slice
    expect(result.items[0]).toEqual([])
    expect(result.items[1]).toEqual([])
  })

  // ---------------------------------------------------------------------------
  // Schema validation
  // ---------------------------------------------------------------------------

  it('should validate items using each entity schema independently', async () => {
    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [
        { Item: { userId: 'u1', name: 'Alice' } },
        { Item: { orderId: 'o1', status: 'pending', total: 99 } },
      ],
    })

    const result = await table.send(
      new TableTransactGet({
        gets: [
          userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new TransactGet({ keys: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
          ),
        ],
      }),
    )

    expect(result.items[0]).toEqual([{ userId: 'u1', name: 'Alice' }])
    expect(result.items[1]).toEqual([{ orderId: 'o1', status: 'pending', total: 99 }])
  })

  it('should throw a ZodError when an item fails the entity schema', async () => {
    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [
        { Item: { userId: 'u1', name: 'Alice' } },
        { Item: { orderId: 'o1', status: 'pending', total: 'not-a-number' } }, // invalid
      ],
    })

    await expect(
      table.send(
        new TableTransactGet({
          gets: [
            userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
            orderEntity.prepare(
              new TransactGet({ keys: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
            ),
          ],
        }),
      ),
    ).rejects.toThrow(ZodError)
  })

  it('should throw a ZodError when a first-group item fails validation', async () => {
    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [
        { Item: { userId: 123, name: 'Alice' } }, // userId should be string
        { Item: { orderId: 'o1', status: 'pending', total: 99 } },
      ],
    })

    await expect(
      table.send(
        new TableTransactGet({
          gets: [
            userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
            orderEntity.prepare(
              new TransactGet({ keys: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
            ),
          ],
        }),
      ),
    ).rejects.toThrow(ZodError)
  })

  it('should skip validation for all entities when skipValidation is true', async () => {
    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [
        { Item: { userId: 'u1', name: 123 } }, // invalid name type
        { Item: { orderId: 'o1', status: 'pending', total: 'not-a-number' } }, // invalid total
      ],
    })

    const result = await table.send(
      new TableTransactGet({
        gets: [
          userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new TransactGet({ keys: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
          ),
        ],
        skipValidation: true,
      }),
    )

    expect(result.items[0]).toEqual([{ userId: 'u1', name: 123 }])
    expect(result.items[1]).toEqual([{ orderId: 'o1', status: 'pending', total: 'not-a-number' }])
  })

  it('should skip validation even when some items are missing', async () => {
    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [
        { Item: undefined },
        { Item: { orderId: 'o1', status: 'pending', total: 'bad' } },
      ],
    })

    const result = await table.send(
      new TableTransactGet({
        gets: [
          userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new TransactGet({ keys: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
          ),
        ],
        skipValidation: true,
      }),
    )

    expect(result.items[0]).toEqual([undefined])
    expect(result.items[1]).toEqual([{ orderId: 'o1', status: 'pending', total: 'bad' }])
  })

  // ---------------------------------------------------------------------------
  // Response metadata
  // ---------------------------------------------------------------------------

  it('should return response metadata and consumed capacity', async () => {
    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [
        { Item: { userId: 'u1', name: 'Alice' } },
        { Item: { orderId: 'o1', status: 'pending', total: 99 } },
      ],
      ConsumedCapacity: [{ TableName: 'TestTable', CapacityUnits: 2 }],
      $metadata: { requestId: 'xyz-789', httpStatusCode: 200 },
    })

    const result = await table.send(
      new TableTransactGet({
        gets: [
          userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new TransactGet({ keys: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
          ),
        ],
      }),
    )

    expect(result.responseMetadata).toEqual({ requestId: 'xyz-789', httpStatusCode: 200 })
    expect(result.consumedCapacity).toEqual({ TableName: 'TestTable', CapacityUnits: 2 })
  })

  it('should use the first ConsumedCapacity entry when multiple are returned', async () => {
    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [
        { Item: { userId: 'u1', name: 'Alice' } },
        { Item: { orderId: 'o1', status: 'pending', total: 99 } },
      ],
      ConsumedCapacity: [
        { TableName: 'TestTable', CapacityUnits: 2 },
        { TableName: 'TestTable', CapacityUnits: 1 },
      ],
      $metadata: { httpStatusCode: 200 },
    })

    const result = await table.send(
      new TableTransactGet({
        gets: [
          userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
          orderEntity.prepare(
            new TransactGet({ keys: [{ orderId: 'o1', status: 'pending', total: 99 }] }),
          ),
        ],
      }),
    )

    expect(result.consumedCapacity).toEqual({ TableName: 'TestTable', CapacityUnits: 2 })
  })

  it('should return undefined consumed capacity when DynamoDB omits it', async () => {
    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [{ Item: { userId: 'u1', name: 'Alice' } }],
      $metadata: { httpStatusCode: 200 },
    })

    const result = await table.send(
      new TableTransactGet({
        gets: [userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] }))],
      }),
    )

    expect(result.consumedCapacity).toBeUndefined()
  })

  // ---------------------------------------------------------------------------
  // Abort / timeout
  // ---------------------------------------------------------------------------

  it('should handle abort signal and timeout', async () => {
    const abortController = new AbortController()

    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [{ Item: { userId: 'u1', name: 'Alice' } }],
    })

    const result = await table.send(
      new TableTransactGet({
        gets: [userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] }))],
        abortController,
        timeoutMs: 5000,
      }),
    )

    expect(result).toBeDefined()
  })

  // ---------------------------------------------------------------------------
  // Table mismatch errors
  // ---------------------------------------------------------------------------

  it('should throw if an entity references a different table', async () => {
    const otherTable = new DynamoTable({ tableName: 'OtherTable', documentClient })
    const otherEntity = new DynamoEntity({
      table: otherTable,
      schema: z.object({ id: z.string(), name: z.string() }),
      partitionKey: item => key('OTHER', item.id),
    })

    await expect(
      table.send(
        new TableTransactGet({
          gets: [
            userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
            otherEntity.prepare(new TransactGet({ keys: [{ id: 'x1', name: 'X' }] })),
          ],
        }),
      ),
    ).rejects.toThrow()
  })

  it('should throw a DocumentBuilderError with the mismatched table names in the message', async () => {
    const otherTable = new DynamoTable({ tableName: 'OtherTable', documentClient })
    const otherEntity = new DynamoEntity({
      table: otherTable,
      schema: z.object({ id: z.string(), name: z.string() }),
      partitionKey: item => key('OTHER', item.id),
    })

    await expect(
      table.send(
        new TableTransactGet({
          gets: [
            userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
            otherEntity.prepare(new TransactGet({ keys: [{ id: 'x1', name: 'X' }] })),
          ],
        }),
      ),
    ).rejects.toThrow(DocumentBuilderError)
  })

  it('should throw when the mismatched entity is not the first one in the list', async () => {
    const productEntity = new DynamoEntity({
      table,
      schema: z.object({ productId: z.string(), price: z.number() }),
      partitionKey: item => key('PRODUCT', item.productId),
      sortKey: () => 'METADATA',
    })

    const otherTable = new DynamoTable({ tableName: 'OtherTable', documentClient })
    const otherEntity = new DynamoEntity({
      table: otherTable,
      schema: z.object({ id: z.string() }),
      partitionKey: item => key('OTHER', item.id),
    })

    await expect(
      table.send(
        new TableTransactGet({
          gets: [
            userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] })),
            productEntity.prepare(new TransactGet({ keys: [{ productId: 'p1', price: 49 }] })),
            otherEntity.prepare(new TransactGet({ keys: [{ id: 'x1' }] })),
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

    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [{ Item: { userId: 'u1', name: 'Alice' } }],
    })

    await customTable.send(
      new TableTransactGet({
        gets: [customEntity.prepare(new TransactGet({ keys: [{ userId: 'u1', name: 'Alice' }] }))],
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TransactItems: [{ Get: { TableName: 'CustomTable', Key: { id: 'USER#u1' } } }],
      }),
    )
  })

  it('should use only a partition key when the entity has no sort key builder', async () => {
    const simplePKEntity = new DynamoEntity({
      table,
      schema: z.object({ tagId: z.string(), label: z.string() }),
      partitionKey: item => key('TAG', item.tagId),
    })

    dynamoMock.on(TransactGetCommand).resolves({
      Responses: [{ Item: { tagId: 't1', label: 'promo' } }],
    })

    await table.send(
      new TableTransactGet({
        gets: [
          simplePKEntity.prepare(new TransactGet({ keys: [{ tagId: 't1', label: 'promo' }] })),
        ],
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TransactItems: [{ Get: { TableName: 'TestTable', Key: { PK: 'TAG#t1' } } }],
      }),
    )
  })
})
