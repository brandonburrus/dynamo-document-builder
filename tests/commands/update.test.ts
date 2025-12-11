import { beforeEach, describe, expect, it } from 'vitest'
import { UpdateCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { Update } from '@/commands/update'
import { DynamoEntity } from '@/core/entity'
import { DynamoTable } from '@/core/table'
import { key } from '@/core/key'
import { mockClient } from 'aws-sdk-client-mock'
import { add, addToSet, append, prepend, ref, remove, removeFromSet, subtract } from '@/updates'
import { z } from 'zod/v4'

describe('update command', () => {
  const dynamoClient = new DynamoDBClient()
  const dynamoMock = mockClient(DynamoDBDocumentClient)

  beforeEach(() => {
    dynamoMock.reset()
  })

  const testTable: DynamoTable = new DynamoTable({
    tableName: 'UpdateTestTable',
    documentClient: DynamoDBDocumentClient.from(dynamoClient),
  })

  const testEntity = new DynamoEntity({
    table: testTable,
    schema: z.object({
      id: z.string(),
      counter: z.number().optional(),
      tags: z.array(z.string()).optional(),
      scores: z.array(z.number()).optional(),
      info: z
        .object({
          views: z.number().optional(),
          likes: z.number().optional(),
        })
        .optional(),
    }),
    partitionKey: item => key('UPDATE_TEST', item.id),
    sortKey: () => 'METADATA',
  })

  it('updates a simple attribute', async () => {
    dynamoMock.on(UpdateCommand).resolves({
      $metadata: {},
    })

    const updateCommand = new Update({
      key: { id: '123' },
      update: {
        counter: 42,
      },
    })

    const result = await testEntity.send(updateCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(result.responseMetadata).toBeDefined()
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'UpdateTestTable',
      Key: {
        PK: 'UPDATE_TEST#123',
        SK: 'METADATA',
      },
      UpdateExpression: 'SET #counter = :v1',
      ExpressionAttributeNames: {
        '#counter': 'counter',
      },
      ExpressionAttributeValues: {
        ':v1': 42,
      },
    })
  })

  it('updates nested attributes', async () => {
    dynamoMock.on(UpdateCommand).resolves({})

    const updateCommand = new Update({
      key: { id: '123' },
      update: {
        'info.views': 100,
        'info.likes': 25,
      },
    })

    await testEntity.send(updateCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'UpdateTestTable',
      Key: {
        PK: 'UPDATE_TEST#123',
        SK: 'METADATA',
      },
      UpdateExpression: 'SET #info.#views = :v1, #info.#likes = :v2',
      ExpressionAttributeNames: {
        '#info': 'info',
        '#views': 'views',
        '#likes': 'likes',
      },
      ExpressionAttributeValues: {
        ':v1': 100,
        ':v2': 25,
      },
    })
  })

  it('adds to a number using add()', async () => {
    dynamoMock.on(UpdateCommand).resolves({})

    const updateCommand = new Update({
      key: { id: '123' },
      update: {
        counter: add(10),
      },
    })

    await testEntity.send(updateCommand)

    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'UpdateTestTable',
      Key: {
        PK: 'UPDATE_TEST#123',
        SK: 'METADATA',
      },
      UpdateExpression: 'SET #counter = #counter + :v1',
      ExpressionAttributeNames: {
        '#counter': 'counter',
      },
      ExpressionAttributeValues: {
        ':v1': 10,
      },
    })
  })

  it('subtracts from a number using subtract()', async () => {
    dynamoMock.on(UpdateCommand).resolves({})

    const updateCommand = new Update({
      key: { id: '123' },
      update: {
        counter: subtract(5),
      },
    })

    await testEntity.send(updateCommand)

    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'UpdateTestTable',
      Key: {
        PK: 'UPDATE_TEST#123',
        SK: 'METADATA',
      },
      UpdateExpression: 'SET #counter = #counter - :v1',
      ExpressionAttributeNames: {
        '#counter': 'counter',
      },
      ExpressionAttributeValues: {
        ':v1': 5,
      },
    })
  })

  it('appends to a list using append()', async () => {
    dynamoMock.on(UpdateCommand).resolves({})

    const updateCommand = new Update({
      key: { id: '123' },
      update: {
        tags: append(['new-tag']),
      },
    })

    await testEntity.send(updateCommand)

    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'UpdateTestTable',
      Key: {
        PK: 'UPDATE_TEST#123',
        SK: 'METADATA',
      },
      UpdateExpression: 'SET #tags = list_append(#tags, :v1)',
      ExpressionAttributeNames: {
        '#tags': 'tags',
      },
      ExpressionAttributeValues: {
        ':v1': ['new-tag'],
      },
    })
  })

  it('prepends to a list using prepend()', async () => {
    dynamoMock.on(UpdateCommand).resolves({})

    const updateCommand = new Update({
      key: { id: '123' },
      update: {
        tags: prepend(['first-tag']),
      },
    })

    await testEntity.send(updateCommand)

    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'UpdateTestTable',
      Key: {
        PK: 'UPDATE_TEST#123',
        SK: 'METADATA',
      },
      UpdateExpression: 'SET #tags = list_append(:v1, #tags)',
      ExpressionAttributeNames: {
        '#tags': 'tags',
      },
      ExpressionAttributeValues: {
        ':v1': ['first-tag'],
      },
    })
  })

  it('adds to a set using addToSet()', async () => {
    dynamoMock.on(UpdateCommand).resolves({})

    const updateCommand = new Update({
      key: { id: '123' },
      update: {
        scores: addToSet([1, 2, 3]),
      },
    })

    await testEntity.send(updateCommand)

    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'UpdateTestTable',
      Key: {
        PK: 'UPDATE_TEST#123',
        SK: 'METADATA',
      },
      UpdateExpression: 'ADD #scores :v1',
      ExpressionAttributeNames: {
        '#scores': 'scores',
      },
      ExpressionAttributeValues: {
        ':v1': [1, 2, 3],
      },
    })
  })

  it('removes from a set using removeFromSet()', async () => {
    dynamoMock.on(UpdateCommand).resolves({})

    const updateCommand = new Update({
      key: { id: '123' },
      update: {
        scores: removeFromSet([2, 3]),
      },
    })

    await testEntity.send(updateCommand)

    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'UpdateTestTable',
      Key: {
        PK: 'UPDATE_TEST#123',
        SK: 'METADATA',
      },
      UpdateExpression: 'DELETE #scores :v1',
      ExpressionAttributeNames: {
        '#scores': 'scores',
      },
      ExpressionAttributeValues: {
        ':v1': [2, 3],
      },
    })
  })

  it('removes an attribute using remove()', async () => {
    dynamoMock.on(UpdateCommand).resolves({})

    const updateCommand = new Update({
      key: { id: '123' },
      update: {
        counter: remove(),
      },
    })

    await testEntity.send(updateCommand)

    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'UpdateTestTable',
      Key: {
        PK: 'UPDATE_TEST#123',
        SK: 'METADATA',
      },
      UpdateExpression: 'REMOVE #counter',
      ExpressionAttributeNames: {
        '#counter': 'counter',
      },
    })
  })

  it('references another attribute using ref()', async () => {
    dynamoMock.on(UpdateCommand).resolves({})

    const updateCommand = new Update({
      key: { id: '123' },
      update: {
        'info.views': ref('info.likes'),
      },
    })

    await testEntity.send(updateCommand)

    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'UpdateTestTable',
      Key: {
        PK: 'UPDATE_TEST#123',
        SK: 'METADATA',
      },
      UpdateExpression: 'SET #info.#views = #info.#likes',
      ExpressionAttributeNames: {
        '#info': 'info',
        '#views': 'views',
        '#likes': 'likes',
      },
    })
  })

  it('combines multiple update operations', async () => {
    dynamoMock.on(UpdateCommand).resolves({})

    const updateCommand = new Update({
      key: { id: '123' },
      update: {
        counter: add(5),
        tags: append(['new-tag']),
        'info.views': 100,
        scores: addToSet([10]),
      },
    })

    await testEntity.send(updateCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toEqual({
      TableName: 'UpdateTestTable',
      Key: {
        PK: 'UPDATE_TEST#123',
        SK: 'METADATA',
      },
      UpdateExpression:
        'SET #counter = #counter + :v1, #tags = list_append(#tags, :v2), #info.#views = :v3 ADD #scores :v4',
      ExpressionAttributeNames: {
        '#counter': 'counter',
        '#tags': 'tags',
        '#info': 'info',
        '#views': 'views',
        '#scores': 'scores',
      },
      ExpressionAttributeValues: {
        ':v1': 5,
        ':v2': ['new-tag'],
        ':v3': 100,
        ':v4': [10],
      },
    })
  })

  it('returns consumed capacity when provided', async () => {
    dynamoMock.on(UpdateCommand).resolves({
      ConsumedCapacity: {
        TableName: 'UpdateTestTable',
        CapacityUnits: 5,
        WriteCapacityUnits: 5,
      },
    })

    const updateCommand = new Update({
      key: { id: '123' },
      update: {
        counter: 50,
      },
    })

    const result = await testEntity.send(updateCommand)

    expect(result.consumedCapacity).toEqual({
      TableName: 'UpdateTestTable',
      CapacityUnits: 5,
      WriteCapacityUnits: 5,
    })
  })
})
