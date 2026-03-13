import { describe, it, expect, beforeEach } from 'vitest'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, TransactWriteCommand } from '@aws-sdk/lib-dynamodb'
import { DynamoEntity, DynamoTable, key } from '@/core'
import {
  ConditionCheck,
  ConditionalDelete,
  ConditionalPut,
  ConditionalUpdate,
  Delete,
  Put,
  TableTransactWrite,
  Update,
} from '@/commands'
import { DocumentBuilderError } from '@/errors'
import { mockClient } from 'aws-sdk-client-mock'
import { z } from 'zod'

const dynamoClient = new DynamoDBClient()
const documentClient = DynamoDBDocumentClient.from(dynamoClient)
const dynamoMock = mockClient(DynamoDBDocumentClient)

describe('TableTransactWrite Command', () => {
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
  // Basic multi-entity write shapes
  // ---------------------------------------------------------------------------

  it('should build a transact write operation across multiple entities with puts', async () => {
    dynamoMock.on(TransactWriteCommand).resolves({})

    await table.send(
      new TableTransactWrite({
        transactions: [
          userEntity.prepare([new Put({ item: { userId: 'u1', name: 'Alice' } })]),
          orderEntity.prepare([new Put({ item: { orderId: 'o1', status: 'pending', total: 99 } })]),
        ],
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TransactItems: [
          {
            Put: {
              TableName: 'TestTable',
              Item: { PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' },
            },
          },
          {
            Put: {
              TableName: 'TestTable',
              Item: { PK: 'ORDER#o1', SK: 'METADATA', orderId: 'o1', status: 'pending', total: 99 },
            },
          },
        ],
      }),
    )
  })

  it('should build a transact write operation with mixed operation types across entities', async () => {
    dynamoMock.on(TransactWriteCommand).resolves({})

    await table.send(
      new TableTransactWrite({
        transactions: [
          userEntity.prepare([
            new Put({ item: { userId: 'u1', name: 'Alice' } }),
            new Delete({ key: { userId: 'u2', name: 'Bob' } }),
          ]),
          orderEntity.prepare([
            new Update({
              key: { orderId: 'o1', status: 'pending', total: 50 },
              update: { status: 'shipped' },
            }),
          ]),
        ],
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TransactItems: [
          {
            Put: {
              TableName: 'TestTable',
              Item: { PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' },
            },
          },
          {
            Delete: {
              TableName: 'TestTable',
              Key: { PK: 'USER#u2', SK: 'METADATA' },
            },
          },
          {
            Update: {
              TableName: 'TestTable',
              Key: { PK: 'ORDER#o1', SK: 'METADATA' },
              UpdateExpression: 'SET #status = :v1',
              ExpressionAttributeNames: { '#status': 'status' },
              ExpressionAttributeValues: { ':v1': 'shipped' },
            },
          },
        ],
      }),
    )
  })

  it('should support condition checks from one entity alongside writes from another', async () => {
    dynamoMock.on(TransactWriteCommand).resolves({})

    await table.send(
      new TableTransactWrite({
        transactions: [
          userEntity.prepare([
            new ConditionCheck({
              key: { userId: 'u1', name: 'Alice' },
              condition: { name: 'Alice' },
            }),
          ]),
          orderEntity.prepare([new Put({ item: { orderId: 'o1', status: 'pending', total: 99 } })]),
        ],
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TransactItems: [
          {
            ConditionCheck: {
              TableName: 'TestTable',
              Key: { PK: 'USER#u1', SK: 'METADATA' },
              ConditionExpression: '#name = :v1',
              ExpressionAttributeNames: { '#name': 'name' },
              ExpressionAttributeValues: { ':v1': 'Alice' },
            },
          },
          {
            Put: {
              TableName: 'TestTable',
              Item: { PK: 'ORDER#o1', SK: 'METADATA', orderId: 'o1', status: 'pending', total: 99 },
            },
          },
        ],
      }),
    )
  })

  it('should support conditional operations across entities', async () => {
    dynamoMock.on(TransactWriteCommand).resolves({})

    await table.send(
      new TableTransactWrite({
        transactions: [
          userEntity.prepare([
            new ConditionalPut({
              item: { userId: 'u1', name: 'Alice' },
              condition: { name: 'Alice' },
            }),
          ]),
          orderEntity.prepare([
            new ConditionalUpdate({
              key: { orderId: 'o1', status: 'pending', total: 50 },
              update: { status: 'shipped' },
              condition: { status: 'pending' },
            }),
            new ConditionalDelete({
              key: { orderId: 'o2', status: 'cancelled', total: 0 },
              condition: { status: 'cancelled' },
            }),
          ]),
        ],
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TransactItems: [
          {
            Put: {
              TableName: 'TestTable',
              Item: { PK: 'USER#u1', SK: 'METADATA', userId: 'u1', name: 'Alice' },
              ConditionExpression: '#name = :v1',
              ExpressionAttributeNames: { '#name': 'name' },
              ExpressionAttributeValues: { ':v1': 'Alice' },
            },
          },
          {
            Update: {
              TableName: 'TestTable',
              Key: { PK: 'ORDER#o1', SK: 'METADATA' },
              UpdateExpression: 'SET #status = :v1',
              ConditionExpression: '#status = :v2',
              ExpressionAttributeNames: { '#status': 'status' },
              ExpressionAttributeValues: { ':v1': 'shipped', ':v2': 'pending' },
            },
          },
          {
            Delete: {
              TableName: 'TestTable',
              Key: { PK: 'ORDER#o2', SK: 'METADATA' },
              ConditionExpression: '#status = :v1',
              ExpressionAttributeNames: { '#status': 'status' },
              ExpressionAttributeValues: { ':v1': 'cancelled' },
            },
          },
        ],
      }),
    )
  })

  it('should preserve the order of transactions across multiple entity groups', async () => {
    dynamoMock.on(TransactWriteCommand).resolves({})

    await table.send(
      new TableTransactWrite({
        transactions: [
          userEntity.prepare([
            new Put({ item: { userId: 'u1', name: 'Alice' } }),
            new Put({ item: { userId: 'u2', name: 'Bob' } }),
          ]),
          orderEntity.prepare([
            new Put({ item: { orderId: 'o1', status: 'pending', total: 10 } }),
            new Put({ item: { orderId: 'o2', status: 'pending', total: 20 } }),
          ]),
          userEntity.prepare([new Delete({ key: { userId: 'u3', name: 'Charlie' } })]),
        ],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const transactItems = (dynamoMock.calls()[0].args[0].input as any).TransactItems
    expect(transactItems).toHaveLength(5)
    expect(transactItems[0]).toHaveProperty('Put.Item.userId', 'u1')
    expect(transactItems[1]).toHaveProperty('Put.Item.userId', 'u2')
    expect(transactItems[2]).toHaveProperty('Put.Item.orderId', 'o1')
    expect(transactItems[3]).toHaveProperty('Put.Item.orderId', 'o2')
    expect(transactItems[4]).toHaveProperty('Delete.Key.PK', 'USER#u3')
  })

  it('should accept an idempotency token', async () => {
    dynamoMock.on(TransactWriteCommand).resolves({})

    await table.send(
      new TableTransactWrite({
        transactions: [userEntity.prepare([new Put({ item: { userId: 'u1', name: 'Alice' } })])],
        idempotencyToken: 'unique-token-123',
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        ClientRequestToken: 'unique-token-123',
      }),
    )
  })

  it('should return response metadata and consumed capacity', async () => {
    dynamoMock.on(TransactWriteCommand).resolves({
      ConsumedCapacity: [{ TableName: 'TestTable', CapacityUnits: 4 }],
      $metadata: { requestId: 'abc-123', httpStatusCode: 200 },
    })

    const result = await table.send(
      new TableTransactWrite({
        transactions: [
          userEntity.prepare([new Put({ item: { userId: 'u1', name: 'Alice' } })]),
          orderEntity.prepare([new Put({ item: { orderId: 'o1', status: 'pending', total: 99 } })]),
        ],
      }),
    )

    expect(result.responseMetadata).toEqual({ requestId: 'abc-123', httpStatusCode: 200 })
    expect(result.consumedCapacity).toEqual([{ TableName: 'TestTable', CapacityUnits: 4 }])
  })

  it('should handle abort signal and timeout', async () => {
    const abortController = new AbortController()
    dynamoMock.on(TransactWriteCommand).resolves({})

    const result = await table.send(
      new TableTransactWrite({
        transactions: [userEntity.prepare([new Put({ item: { userId: 'u1', name: 'Alice' } })])],
        abortController,
        timeoutMs: 5000,
      }),
    )

    expect(result).toBeDefined()
  })

  it('should throw if an entity references a different table', async () => {
    const otherTable = new DynamoTable({ tableName: 'OtherTable', documentClient })
    const otherEntity = new DynamoEntity({
      table: otherTable,
      schema: z.object({ id: z.string() }),
      partitionKey: item => key('OTHER', item.id),
    })

    await expect(
      table.send(
        new TableTransactWrite({
          transactions: [
            userEntity.prepare([new Put({ item: { userId: 'u1', name: 'Alice' } })]),
            otherEntity.prepare([new Put({ item: { id: 'x1' } })]),
          ],
        }),
      ),
    ).rejects.toThrow()
  })

  // ---------------------------------------------------------------------------
  // Additional coverage
  // ---------------------------------------------------------------------------

  it('should allow a single entity transaction with a single operation', async () => {
    dynamoMock.on(TransactWriteCommand).resolves({})

    await table.send(
      new TableTransactWrite({
        transactions: [userEntity.prepare([new Put({ item: { userId: 'u1', name: 'Alice' } })])],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const transactItems = (dynamoMock.calls()[0].args[0].input as any).TransactItems
    expect(transactItems).toHaveLength(1)
    expect(transactItems[0]).toHaveProperty('Put.Item.userId', 'u1')
  })

  it('should allow a single entity transaction with multiple operations', async () => {
    dynamoMock.on(TransactWriteCommand).resolves({})

    await table.send(
      new TableTransactWrite({
        transactions: [
          userEntity.prepare([
            new Put({ item: { userId: 'u1', name: 'Alice' } }),
            new Put({ item: { userId: 'u2', name: 'Bob' } }),
            new Delete({ key: { userId: 'u3', name: 'Charlie' } }),
          ]),
        ],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const transactItems = (dynamoMock.calls()[0].args[0].input as any).TransactItems
    expect(transactItems).toHaveLength(3)
    expect(transactItems[0]).toHaveProperty('Put.Item.userId', 'u1')
    expect(transactItems[1]).toHaveProperty('Put.Item.userId', 'u2')
    expect(transactItems[2]).toHaveProperty('Delete.Key.PK', 'USER#u3')
  })

  it('should pass returnConsumedCapacity to the underlying DynamoDB command', async () => {
    dynamoMock.on(TransactWriteCommand).resolves({})

    await table.send(
      new TableTransactWrite({
        transactions: [userEntity.prepare([new Put({ item: { userId: 'u1', name: 'Alice' } })])],
        returnConsumedCapacity: 'TOTAL',
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({ ReturnConsumedCapacity: 'TOTAL' }),
    )
  })

  it('should pass returnItemCollectionMetrics to the underlying DynamoDB command', async () => {
    dynamoMock.on(TransactWriteCommand).resolves({})

    await table.send(
      new TableTransactWrite({
        transactions: [userEntity.prepare([new Put({ item: { userId: 'u1', name: 'Alice' } })])],
        returnItemCollectionMetrics: 'SIZE',
      }),
    )

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({ ReturnItemCollectionMetrics: 'SIZE' }),
    )
  })

  it('should include itemCollectionMetrics in the result when returned by DynamoDB', async () => {
    const metrics = {
      TestTable: [{ ItemCollectionKey: { PK: 'USER#u1' }, SizeEstimateRangeGB: [1, 2] }],
    }

    dynamoMock.on(TransactWriteCommand).resolves({ ItemCollectionMetrics: metrics })

    const result = await table.send(
      new TableTransactWrite({
        transactions: [userEntity.prepare([new Put({ item: { userId: 'u1', name: 'Alice' } })])],
        returnItemCollectionMetrics: 'SIZE',
      }),
    )

    expect(result.itemCollectionMetrics).toEqual(metrics)
  })

  it('should return undefined consumed capacity when DynamoDB omits it', async () => {
    dynamoMock.on(TransactWriteCommand).resolves({ $metadata: { httpStatusCode: 200 } })

    const result = await table.send(
      new TableTransactWrite({
        transactions: [userEntity.prepare([new Put({ item: { userId: 'u1', name: 'Alice' } })])],
      }),
    )

    expect(result.consumedCapacity).toBeUndefined()
    expect(result.itemCollectionMetrics).toBeUndefined()
  })

  it('should throw a DocumentBuilderError with the mismatched table names in the message', async () => {
    const otherTable = new DynamoTable({ tableName: 'OtherTable', documentClient })
    const otherEntity = new DynamoEntity({
      table: otherTable,
      schema: z.object({ id: z.string() }),
      partitionKey: item => key('OTHER', item.id),
    })

    await expect(
      table.send(
        new TableTransactWrite({
          transactions: [
            userEntity.prepare([new Put({ item: { userId: 'u1', name: 'Alice' } })]),
            otherEntity.prepare([new Put({ item: { id: 'x1' } })]),
          ],
        }),
      ),
    ).rejects.toThrow(DocumentBuilderError)
  })

  it('should throw when the mismatched entity is not the first one in the list', async () => {
    const otherTable = new DynamoTable({ tableName: 'OtherTable', documentClient })
    const otherEntity = new DynamoEntity({
      table: otherTable,
      schema: z.object({ id: z.string() }),
      partitionKey: item => key('OTHER', item.id),
    })

    await expect(
      table.send(
        new TableTransactWrite({
          transactions: [
            userEntity.prepare([new Put({ item: { userId: 'u1', name: 'Alice' } })]),
            orderEntity.prepare([
              new Put({ item: { orderId: 'o1', status: 'pending', total: 10 } }),
            ]),
            otherEntity.prepare([new Put({ item: { id: 'x1' } })]),
          ],
        }),
      ),
    ).rejects.toThrow(DocumentBuilderError)
  })

  it('should span three different entity types in a single transaction', async () => {
    dynamoMock.on(TransactWriteCommand).resolves({})

    await table.send(
      new TableTransactWrite({
        transactions: [
          userEntity.prepare([new Put({ item: { userId: 'u1', name: 'Alice' } })]),
          orderEntity.prepare([new Put({ item: { orderId: 'o1', status: 'pending', total: 99 } })]),
          productEntity.prepare([new Put({ item: { productId: 'p1', price: 49 } })]),
        ],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const transactItems = (dynamoMock.calls()[0].args[0].input as any).TransactItems
    expect(transactItems).toHaveLength(3)
    expect(transactItems[0]).toHaveProperty('Put.Item.PK', 'USER#u1')
    expect(transactItems[1]).toHaveProperty('Put.Item.PK', 'ORDER#o1')
    expect(transactItems[2]).toHaveProperty('Put.Item.PK', 'PRODUCT#p1')
  })

  it('should interleave the same entity type multiple times while preserving order', async () => {
    dynamoMock.on(TransactWriteCommand).resolves({})

    await table.send(
      new TableTransactWrite({
        transactions: [
          userEntity.prepare([new Put({ item: { userId: 'u1', name: 'Alice' } })]),
          orderEntity.prepare([new Put({ item: { orderId: 'o1', status: 'pending', total: 10 } })]),
          userEntity.prepare([new Put({ item: { userId: 'u2', name: 'Bob' } })]),
          orderEntity.prepare([new Put({ item: { orderId: 'o2', status: 'pending', total: 20 } })]),
        ],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const transactItems = (dynamoMock.calls()[0].args[0].input as any).TransactItems
    expect(transactItems).toHaveLength(4)
    expect(transactItems[0]).toHaveProperty('Put.Item.userId', 'u1')
    expect(transactItems[1]).toHaveProperty('Put.Item.orderId', 'o1')
    expect(transactItems[2]).toHaveProperty('Put.Item.userId', 'u2')
    expect(transactItems[3]).toHaveProperty('Put.Item.orderId', 'o2')
  })

  it('should not include ClientRequestToken in the command when no idempotency token is given', async () => {
    dynamoMock.on(TransactWriteCommand).resolves({})

    await table.send(
      new TableTransactWrite({
        transactions: [userEntity.prepare([new Put({ item: { userId: 'u1', name: 'Alice' } })])],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    expect((dynamoMock.calls()[0].args[0].input as any).ClientRequestToken).toBeUndefined()
  })

  it('should use only a partition key when the entity has no sort key builder', async () => {
    const simplePKEntity = new DynamoEntity({
      table,
      schema: z.object({ tagId: z.string(), label: z.string() }),
      partitionKey: item => key('TAG', item.tagId),
    })

    dynamoMock.on(TransactWriteCommand).resolves({})

    await table.send(
      new TableTransactWrite({
        transactions: [
          simplePKEntity.prepare([new Put({ item: { tagId: 't1', label: 'promo' } })]),
        ],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const transactItems = (dynamoMock.calls()[0].args[0].input as any).TransactItems
    expect(transactItems[0]).toHaveProperty('Put.Item.PK', 'TAG#t1')
    // SK should be absent from the key — not forced to undefined
    expect(transactItems[0].Put?.Item?.SK).toBeUndefined()
  })

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

    dynamoMock.on(TransactWriteCommand).resolves({})

    await customTable.send(
      new TableTransactWrite({
        transactions: [customEntity.prepare([new Put({ item: { userId: 'u1', name: 'Alice' } })])],
      }),
    )

    // biome-ignore lint/suspicious/noExplicitAny: mock union type requires cast
    const transactItems = (dynamoMock.calls()[0].args[0].input as any).TransactItems
    expect(transactItems[0]).toHaveProperty('Put.Item.id', 'USER#u1')
    expect(transactItems[0]).toHaveProperty('Put.TableName', 'CustomTable')
  })
})
