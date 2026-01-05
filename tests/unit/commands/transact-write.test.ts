import { describe, it, expect, beforeEach } from 'vitest'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, TransactWriteCommand } from '@aws-sdk/lib-dynamodb'
import { DynamoEntity, DynamoTable, key } from '@/core'
import {
  ConditionalPut,
  ConditionalDelete,
  ConditionalUpdate,
  Delete,
  Put,
  TransactWrite,
  Update,
  ConditionCheck,
} from '@/commands'
import { mockClient } from 'aws-sdk-client-mock'
import { z } from 'zod'

const dynamoClient = new DynamoDBClient()
const documentClient = DynamoDBDocumentClient.from(dynamoClient)
const dynamoMock = mockClient(DynamoDBDocumentClient)

describe('Transact Write Command', () => {
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

  it('should build a transact write operation with puts', async () => {
    const transactWrite = new TransactWrite({
      writes: [
        new Put({
          item: { id: '1', name: 'Alice' },
        }),
        new Put({
          item: { id: '2', name: 'Bob' },
        }),
      ],
    })

    dynamoMock.on(TransactWriteCommand).resolves({})

    const result = await entity.send(transactWrite)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TransactItems: [
          {
            Put: {
              TableName: 'TestTable',
              Item: { PK: 'USER#1', SK: 'NAME#Alice', id: '1', name: 'Alice' },
            },
          },
          {
            Put: {
              TableName: 'TestTable',
              Item: { PK: 'USER#2', SK: 'NAME#Bob', id: '2', name: 'Bob' },
            },
          },
        ],
      }),
    )

    expect(result).toEqual({})
  })

  it('should build a transact write operation with updates', async () => {
    const transactWrite = new TransactWrite({
      writes: [
        new Update({
          key: { id: '1', name: 'Alice' },
          update: { name: 'Alex' },
        }),
      ],
    })

    dynamoMock.on(TransactWriteCommand).resolves({})

    const result = await entity.send(transactWrite)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TransactItems: [
          {
            Update: {
              TableName: 'TestTable',
              Key: { PK: 'USER#1', SK: 'NAME#Alice' },
              UpdateExpression: 'SET #name = :v1',
              ExpressionAttributeNames: { '#name': 'name' },
              ExpressionAttributeValues: { ':v1': 'Alex' },
            },
          },
        ],
      }),
    )

    expect(result).toEqual({})
  })

  it('should build a transact write operation with deletes', async () => {
    const transactWrite = new TransactWrite({
      writes: [
        new Delete({
          key: { id: '2', name: 'Bob' },
        }),
      ],
    })

    dynamoMock.on(TransactWriteCommand).resolves({})

    const result = await entity.send(transactWrite)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TransactItems: [
          {
            Delete: {
              TableName: 'TestTable',
              Key: { PK: 'USER#2', SK: 'NAME#Bob' },
            },
          },
        ],
      }),
    )

    expect(result).toEqual({})
  })

  it('should build a transact write operation with condition checks', async () => {
    const transactWrite = new TransactWrite({
      writes: [
        new ConditionCheck({
          key: { id: '1', name: 'Alice' },
          condition: { name: 'Alice' },
        }),
        new Put({
          item: { id: '4', name: 'Diana' },
        }),
      ],
    })

    dynamoMock.on(TransactWriteCommand).resolves({})

    const result = await entity.send(transactWrite)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TransactItems: [
          {
            ConditionCheck: {
              TableName: 'TestTable',
              Key: { PK: 'USER#1', SK: 'NAME#Alice' },
              ConditionExpression: '#name = :v1',
              ExpressionAttributeNames: { '#name': 'name' },
              ExpressionAttributeValues: { ':v1': 'Alice' },
            },
          },
          {
            Put: {
              TableName: 'TestTable',
              Item: { PK: 'USER#4', SK: 'NAME#Diana', id: '4', name: 'Diana' },
            },
          },
        ],
      }),
    )

    expect(result).toEqual({})
  })

  it('should build a transact write operation with a conditional put', async () => {
    const transactWrite = new TransactWrite({
      writes: [
        new ConditionalPut({
          item: { id: '5', name: 'Eve' },
          condition: { name: 'Eve' },
        }),
      ],
    })

    dynamoMock.on(TransactWriteCommand).resolves({})

    const result = await entity.send(transactWrite)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TransactItems: [
          {
            Put: {
              TableName: 'TestTable',
              Item: {
                PK: 'USER#5',
                SK: 'NAME#Eve',
                id: '5',
                name: 'Eve',
              },
              ConditionExpression: '#name = :v1',
              ExpressionAttributeNames: { '#name': 'name' },
              ExpressionAttributeValues: { ':v1': 'Eve' },
            },
          },
        ],
      }),
    )

    expect(result).toEqual({})
  })

  it('should build a transact write operation with a conditional update', async () => {
    const transactWrite = new TransactWrite({
      writes: [
        new ConditionalUpdate({
          key: { id: '2', name: 'Bob' },
          update: { name: 'Robert' },
          condition: { name: 'Bob' },
        }),
      ],
    })

    dynamoMock.on(TransactWriteCommand).resolves({})

    const result = await entity.send(transactWrite)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TransactItems: [
          {
            Update: {
              TableName: 'TestTable',
              Key: { PK: 'USER#2', SK: 'NAME#Bob' },
              UpdateExpression: 'SET #name = :v1',
              ConditionExpression: '#name = :v2',
              ExpressionAttributeNames: { '#name': 'name' },
              ExpressionAttributeValues: { ':v1': 'Robert', ':v2': 'Bob' },
            },
          },
        ],
      }),
    )

    expect(result).toEqual({})
  })

  it('should build a transact write operation with a conditional delete', async () => {
    const transactWrite = new TransactWrite({
      writes: [
        new ConditionalDelete({
          key: { id: '3', name: 'Charlie' },
          condition: { name: 'Charlie' },
        }),
      ],
    })

    dynamoMock.on(TransactWriteCommand).resolves({})

    const result = await entity.send(transactWrite)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TransactItems: [
          {
            Delete: {
              TableName: 'TestTable',
              Key: { PK: 'USER#3', SK: 'NAME#Charlie' },
              ConditionExpression: '#name = :v1',
              ExpressionAttributeNames: { '#name': 'name' },
              ExpressionAttributeValues: { ':v1': 'Charlie' },
            },
          },
        ],
      }),
    )

    expect(result).toEqual({})
  })

  it('should build a transact write operation with mixed writes', async () => {
    const transactWrite = new TransactWrite({
      writes: [
        new Put({
          item: { id: '1', name: 'Alice' },
        }),
        new Update({
          key: { id: '2', name: 'Bob' },
          update: { name: 'Bobby' },
        }),
        new Delete({
          key: { id: '3', name: 'Charlie' },
        }),
        new ConditionCheck({
          key: { id: '4', name: 'Diana' },
          condition: { name: 'Diana' },
        }),
        new ConditionalPut({
          item: { id: '5', name: 'Eve' },
          condition: { name: 'Eve' },
        }),
        new ConditionalUpdate({
          key: { id: '6', name: 'Frank' },
          update: { name: 'Franklin' },
          condition: { name: 'Frank' },
        }),
        new ConditionalDelete({
          key: { id: '7', name: 'Grace' },
          condition: { name: 'Grace' },
        }),
      ],
    })

    dynamoMock.on(TransactWriteCommand).resolves({})

    const result = await entity.send(transactWrite)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TransactItems: [
          {
            Put: {
              TableName: 'TestTable',
              Item: { PK: 'USER#1', SK: 'NAME#Alice', id: '1', name: 'Alice' },
            },
          },
          {
            Update: {
              TableName: 'TestTable',
              Key: { PK: 'USER#2', SK: 'NAME#Bob' },
              UpdateExpression: 'SET #name = :v1',
              ExpressionAttributeNames: { '#name': 'name' },
              ExpressionAttributeValues: { ':v1': 'Bobby' },
            },
          },
          {
            Delete: {
              TableName: 'TestTable',
              Key: { PK: 'USER#3', SK: 'NAME#Charlie' },
            },
          },
          {
            ConditionCheck: {
              TableName: 'TestTable',
              Key: { PK: 'USER#4', SK: 'NAME#Diana' },
              ConditionExpression: '#name = :v1',
              ExpressionAttributeNames: { '#name': 'name' },
              ExpressionAttributeValues: { ':v1': 'Diana' },
            },
          },
          {
            Put: {
              TableName: 'TestTable',
              Item: {
                PK: 'USER#5',
                SK: 'NAME#Eve',
                id: '5',
                name: 'Eve',
              },
              ConditionExpression: '#name = :v1',
              ExpressionAttributeNames: { '#name': 'name' },
              ExpressionAttributeValues: { ':v1': 'Eve' },
            },
          },
          {
            Update: {
              TableName: 'TestTable',
              Key: { PK: 'USER#6', SK: 'NAME#Frank' },
              UpdateExpression: 'SET #name = :v1',
              ConditionExpression: '#name = :v2',
              ExpressionAttributeNames: { '#name': 'name' },
              ExpressionAttributeValues: { ':v1': 'Franklin', ':v2': 'Frank' },
            },
          },
          {
            Delete: {
              TableName: 'TestTable',
              Key: { PK: 'USER#7', SK: 'NAME#Grace' },
              ConditionExpression: '#name = :v1',
              ExpressionAttributeNames: { '#name': 'name' },
              ExpressionAttributeValues: { ':v1': 'Grace' },
            },
          },
        ],
      }),
    )

    expect(result).toEqual({})
  })

  it('should accept an idempotency token', async () => {
    const transactWrite = new TransactWrite({
      writes: [
        new Put({
          item: { id: '1', name: 'Alice' },
        }),
      ],
      idempotencyToken: 'unique-token-123',
    })

    dynamoMock.on(TransactWriteCommand).resolves({})

    const result = await entity.send(transactWrite)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        ClientRequestToken: 'unique-token-123',
        TransactItems: [
          {
            Put: {
              TableName: 'TestTable',
              Item: { PK: 'USER#1', SK: 'NAME#Alice', id: '1', name: 'Alice' },
            },
          },
        ],
      }),
    )

    expect(result).toEqual({})
  })

  it('should handle abort signal and timeout', async () => {
    const abortController = new AbortController()

    const transactWrite = new TransactWrite({
      writes: [
        new Put({
          item: { id: '1', name: 'Alice' },
        }),
      ],
      abortController,
      timeoutMs: 5000,
    })

    dynamoMock.on(TransactWriteCommand).resolves({})
    const result = await entity.send(transactWrite)

    expect(result).toEqual({})
  })

  it('should return response metadata and consumed capacity', async () => {
    const transactWrite = new TransactWrite({
      writes: [
        new Put({
          item: { id: '1', name: 'Alice' },
        }),
        new Update({
          key: { id: '2', name: 'Bob' },
          update: { name: 'Bobbie' },
        }),
      ],
    })

    dynamoMock.on(TransactWriteCommand).resolves({
      ConsumedCapacity: [
        {
          TableName: 'TestTable',
          CapacityUnits: 3,
        },
      ],
      $metadata: {
        requestId: '12345',
        httpStatusCode: 200,
      },
    })

    const result = await entity.send(transactWrite)

    expect(result.responseMetadata).toEqual({
      requestId: '12345',
      httpStatusCode: 200,
    })
    expect(result.consumedCapacity).toEqual([
      {
        TableName: 'TestTable',
        CapacityUnits: 3,
      },
    ])
  })
})
