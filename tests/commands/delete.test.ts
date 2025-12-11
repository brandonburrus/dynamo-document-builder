import { beforeEach, describe, expect, it } from 'vitest'
import { DeleteCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import { Delete } from '@/commands/delete'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoEntity } from '@/core/entity'
import { DynamoTable } from '@/core/table'
import { isoDatetime } from '@/codec/iso-datetime'
import { key } from '@/core/key'
import { mockClient } from 'aws-sdk-client-mock'
import { z } from 'zod/v4'

describe('delete command', () => {
  const dynamoClient = new DynamoDBClient()
  const dynamoMock = mockClient(DynamoDBDocumentClient)

  beforeEach(() => {
    dynamoMock.reset()
  })

  const testTable: DynamoTable = new DynamoTable({
    tableName: 'TestTable',
    documentClient: DynamoDBDocumentClient.from(dynamoClient),
  })

  const testEntity = new DynamoEntity({
    table: testTable,
    schema: z.object({
      id: z.string(),
      name: z.string(),
      timestamp: isoDatetime(),
    }),
    partitionKey: item => key('TEST', item.id),
    sortKey: item => item.timestamp.toISOString(),
  })

  it('deletes an item', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new Delete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
    })

    const result = await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.oldItem).toBeUndefined()
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Key: {
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
    })
  })

  it('deletes an item and returns old values', async () => {
    dynamoMock.on(DeleteCommand).resolves({
      Attributes: {
        id: '123',
        name: 'Test Item',
        timestamp: '2000-01-01T00:00:00.000Z',
      },
    })

    const deleteCommand = new Delete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      returnValues: 'ALL_OLD',
    })

    const result = await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.oldItem).toEqual({
      id: '123',
      name: 'Test Item',
      timestamp: new Date('2000-01-01T00:00:00Z'),
    })
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Key: {
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ReturnValues: 'ALL_OLD',
    })
  })

  it('deletes an item and returns consumed write capacity', async () => {
    dynamoMock.on(DeleteCommand).resolves({
      ConsumedCapacity: {
        TableName: 'TestTable',
        CapacityUnits: 9,
        WriteCapacityUnits: 9,
      },
    })

    const deleteCommand = new Delete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      returnConsumedCapacity: 'TOTAL',
    })

    const result = await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.consumedCapacity).toEqual({
      TableName: 'TestTable',
      CapacityUnits: 9,
      WriteCapacityUnits: 9,
    })
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Key: {
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ReturnConsumedCapacity: 'TOTAL',
    })
  })

  it('deletes an item with returnItemCollectionMetrics', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new Delete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      returnItemCollectionMetrics: 'SIZE',
    })

    const result = await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Key: {
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ReturnItemCollectionMetrics: 'SIZE',
    })
  })

  it('deletes an item with returnValuesOnConditionCheckFailure', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new Delete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      returnValuesOnConditionCheckFailure: 'ALL_OLD',
    })

    const result = await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'TestTable',
      Key: {
        PK: 'TEST#123',
        SK: '2000-01-01T00:00:00.000Z',
      },
      ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
    })
  })

  it('deletes an item with skipValidation and returns attributes', async () => {
    dynamoMock.on(DeleteCommand).resolves({
      Attributes: {
        id: '123',
        name: 'Test Item',
        timestamp: '2000-01-01T00:00:00.000Z',
      },
    })

    const deleteCommand = new Delete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      returnValues: 'ALL_OLD',
      skipValidation: true,
    })

    const result = await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.oldItem).toBeUndefined()
  })
})
