import { beforeEach, describe, expect, it } from 'vitest'
import { DeleteCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import { ConditionalDelete } from '@/commands/conditional-delete'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoEntity } from '@/core/entity'
import { DynamoTable } from '@/core/table'
import { greaterThanOrEqualTo, gte, lessThanOrEqualTo, lte } from '@/conditions'
import { isoDatetime } from '@/codec'
import { key } from '@/core/key'
import { mockClient } from 'aws-sdk-client-mock'
import { z } from 'zod/v4'

describe('comparison operators - greaterThanOrEqualTo and lessThanOrEqualTo', () => {
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
      score: z.number(),
      timestamp: isoDatetime(),
    }),
    partitionKey: item => key('TEST', item.id),
    sortKey: item => item.timestamp.toISOString(),
  })

  it('parses greaterThanOrEqualTo condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        score: greaterThanOrEqualTo(100),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#score >= :v1',
      ExpressionAttributeNames: {
        '#score': 'score',
      },
      ExpressionAttributeValues: {
        ':v1': 100,
      },
    })
  })

  it('parses gte alias', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        score: gte(50),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#score >= :v1',
      ExpressionAttributeNames: {
        '#score': 'score',
      },
      ExpressionAttributeValues: {
        ':v1': 50,
      },
    })
  })

  it('parses lessThanOrEqualTo condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        score: lessThanOrEqualTo(200),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#score <= :v1',
      ExpressionAttributeNames: {
        '#score': 'score',
      },
      ExpressionAttributeValues: {
        ':v1': 200,
      },
    })
  })

  it('parses lte alias', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        score: lte(75),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#score <= :v1',
      ExpressionAttributeNames: {
        '#score': 'score',
      },
      ExpressionAttributeValues: {
        ':v1': 75,
      },
    })
  })

  it('parses combined gte and lte for range condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: [{ score: gte(10) }, { score: lte(100) }],
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#score >= :v1 AND #score <= :v2',
      ExpressionAttributeNames: {
        '#score': 'score',
      },
      ExpressionAttributeValues: {
        ':v1': 10,
        ':v2': 100,
      },
    })
  })
})
