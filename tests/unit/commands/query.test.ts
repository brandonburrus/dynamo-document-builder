import { describe, it, expect, beforeEach } from 'vitest'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, QueryCommand } from '@aws-sdk/lib-dynamodb'
import { DynamoEntity, DynamoTable, key } from '@/core'
import { Query, ProjectedQuery } from '@/commands'
import { mockClient } from 'aws-sdk-client-mock'
import { z, ZodError } from 'zod'
import { beginsWith } from '@/conditions'
import { DocumentBuilderError } from '@/errors'

const dynamoClient = new DynamoDBClient()
const documentClient = DynamoDBDocumentClient.from(dynamoClient)
const dynamoMock = mockClient(DynamoDBDocumentClient)

describe('Query Command', () => {
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
    partitionKey: item => key('ITEM', item.id),
    sortKey: item => key('NAME', item.name),
  })

  it('should build a Query operation', async () => {
    const queryCommand = new Query({
      key: {
        id: '123',
      },
    })

    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          id: '123',
          name: 'TestName1',
        },
        {
          id: '123',
          name: 'TestName2',
        },
      ],
    })

    const result = await entity.send(queryCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        KeyConditionExpression: '#PK = :v1',
        ExpressionAttributeNames: {
          '#PK': 'PK',
        },
        ExpressionAttributeValues: {
          ':v1': 'ITEM#123',
        },
      }),
    )

    expect(result.items).toEqual([
      {
        id: '123',
        name: 'TestName1',
      },
      {
        id: '123',
        name: 'TestName2',
      },
    ])
  })

  it('should build a Query with a key condition', async () => {
    const queryCommand = new Query({
      key: {
        id: '123',
      },
      sortKeyCondition: {
        SK: beginsWith('NAME#Test'),
      },
    })

    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          id: '123',
          name: 'TestName1',
        },
      ],
    })

    const result = await entity.send(queryCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        KeyConditionExpression: '#PK = :v1 AND begins_with(#SK, :v2)',
        ExpressionAttributeNames: {
          '#PK': 'PK',
          '#SK': 'SK',
        },
        ExpressionAttributeValues: {
          ':v1': 'ITEM#123',
          ':v2': 'NAME#Test',
        },
      }),
    )

    expect(result.items).toEqual([
      {
        id: '123',
        name: 'TestName1',
      },
    ])
  })

  it('should build a Query with a filter', async () => {
    const queryCommand = new Query({
      key: {
        id: '123',
      },
      filter: {
        name: 'TestName1',
      },
    })

    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          id: '123',
          name: 'TestName1',
        },
      ],
    })

    const result = await entity.send(queryCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        KeyConditionExpression: '#PK = :v1',
        FilterExpression: '#name = :v2',
        ExpressionAttributeNames: {
          '#PK': 'PK',
          '#name': 'name',
        },
        ExpressionAttributeValues: {
          ':v1': 'ITEM#123',
          ':v2': 'TestName1',
        },
      }),
    )

    expect(result.items).toEqual([
      {
        id: '123',
        name: 'TestName1',
      },
    ])
  })

  it('should build a Query with a key condition and filter', async () => {
    const queryCommand = new Query({
      key: {
        id: '123',
      },
      sortKeyCondition: {
        SK: beginsWith('NAME#Test'),
      },
      filter: {
        name: 'TestName1',
      },
    })

    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          id: '123',
          name: 'TestName1',
        },
      ],
    })

    const result = await entity.send(queryCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        KeyConditionExpression: '#PK = :v1 AND begins_with(#SK, :v2)',
        FilterExpression: '#name = :v3',
        ExpressionAttributeNames: {
          '#PK': 'PK',
          '#SK': 'SK',
          '#name': 'name',
        },
        ExpressionAttributeValues: {
          ':v1': 'ITEM#123',
          ':v2': 'NAME#Test',
          ':v3': 'TestName1',
        },
      }),
    )

    expect(result.items).toEqual([
      {
        id: '123',
        name: 'TestName1',
      },
    ])
  })

  it('should build a Query with a limit', async () => {
    const queryCommand = new Query({
      key: {
        id: '123',
      },
      limit: 5,
    })

    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          id: '123',
          name: 'TestName1',
        },
      ],
    })

    const result = await entity.send(queryCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        KeyConditionExpression: '#PK = :v1',
        ExpressionAttributeNames: {
          '#PK': 'PK',
        },
        ExpressionAttributeValues: {
          ':v1': 'ITEM#123',
        },
        Limit: 5,
      }),
    )

    expect(result.items).toEqual([
      {
        id: '123',
        name: 'TestName1',
      },
    ])
  })

  it('should build a consistent read Query', async () => {
    const queryCommand = new Query({
      key: {
        id: '123',
      },
      consistent: true,
    })

    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          id: '123',
          name: 'TestName1',
        },
      ],
    })

    const result = await entity.send(queryCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        KeyConditionExpression: '#PK = :v1',
        ExpressionAttributeNames: {
          '#PK': 'PK',
        },
        ExpressionAttributeValues: {
          ':v1': 'ITEM#123',
        },
        ConsistentRead: true,
      }),
    )

    expect(result.items).toEqual([
      {
        id: '123',
        name: 'TestName1',
      },
    ])
  })

  it('should build a Query with a reverse index scan', async () => {
    const queryCommand = new Query({
      key: {
        id: '123',
      },
      reverseIndexScan: true,
    })

    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          id: '123',
          name: 'TestName1',
        },
      ],
    })

    const result = await entity.send(queryCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        KeyConditionExpression: '#PK = :v1',
        ExpressionAttributeNames: {
          '#PK': 'PK',
        },
        ExpressionAttributeValues: {
          ':v1': 'ITEM#123',
        },
        ScanIndexForward: false,
      }),
    )

    expect(result.items).toEqual([
      {
        id: '123',
        name: 'TestName1',
      },
    ])
  })

  it('should be able to query secondary indexes', async () => {
    const tableWithGSI = new DynamoTable({
      tableName: 'TestTable',
      documentClient,
      keyNames: {
        partitionKey: 'PK',
        sortKey: 'SK',
        globalSecondaryIndexes: {
          GSI1: {
            partitionKey: 'GSI1PK',
            sortKey: 'GSI1SK',
          },
        },
      },
    })

    const entityWithGSI = new DynamoEntity({
      table: tableWithGSI,
      schema: z.object({
        id: z.string(),
        category: z.string(),
        value: z.number(),
      }),
      partitionKey: item => key('ITEM', item.id),
      sortKey: item => key('category', item.category),
      globalSecondaryIndexes: {
        GSI1: {
          partitionKey: item => key('CATEGORY', item.category),
          sortKey: item => key('VALUE', item.value),
        },
      },
    })

    const queryCommand = new Query({
      index: {
        GSI1: {
          category: 'TestCategory',
        },
      },
      sortKeyCondition: {
        GSI1SK: beginsWith('VALUE#'),
      },
    })

    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          id: '123',
          category: 'TestCategory',
          value: 42,
        },
      ],
    })

    const result = await entityWithGSI.send(queryCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        IndexName: 'GSI1',
        KeyConditionExpression: '#GSI1PK = :v1 AND begins_with(#GSI1SK, :v2)',
        ExpressionAttributeNames: {
          '#GSI1PK': 'GSI1PK',
          '#GSI1SK': 'GSI1SK',
        },
        ExpressionAttributeValues: {
          ':v1': 'CATEGORY#TestCategory',
          ':v2': 'VALUE#',
        },
      }),
    )

    expect(result.items).toEqual([
      {
        id: '123',
        category: 'TestCategory',
        value: 42,
      },
    ])
  })

  it('should be able to paginate results', async () => {
    const queryCommand = new Query({
      key: {
        id: '123',
      },
      limit: 1,
    })

    dynamoMock
      .on(QueryCommand)
      .resolvesOnce({
        Items: [
          {
            id: '123',
            name: 'TestName1',
          },
        ],
        LastEvaluatedKey: {
          PK: 'ITEM#123',
          SK: 'NAME#TestName1',
        },
      })
      .resolvesOnce({
        Items: [
          {
            id: '123',
            name: 'TestName2',
          },
        ],
      })

    const paginator = entity.paginate(queryCommand)

    const results = []
    let pageCount = 0
    for await (const page of paginator) {
      results.push(...page.items)
      pageCount++
    }

    expect(pageCount).toBe(2)
    expect(results).toEqual([
      {
        id: '123',
        name: 'TestName1',
      },
      {
        id: '123',
        name: 'TestName2',
      },
    ])
  })

  it('should return the last evaluated key for manual pagination', async () => {
    const queryCommand = new Query({
      key: {
        id: '123',
      },
      limit: 1,
    })

    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          id: '123',
          name: 'TestName1',
        },
      ],
      LastEvaluatedKey: {
        PK: 'ITEM#123',
        SK: 'NAME#TestName1',
      },
    })

    const result = await entity.send(queryCommand)

    expect(result.items).toEqual([
      {
        id: '123',
        name: 'TestName1',
      },
    ])

    expect(result.lastEvaluatedKey).toEqual({
      PK: 'ITEM#123',
      SK: 'NAME#TestName1',
    })
  })

  it('should accept an exclusive start key for manual pagination', async () => {
    const queryCommand = new Query({
      key: {
        id: '123',
      },
      exclusiveStartKey: {
        PK: 'ITEM#123',
        SK: 'NAME#TestName1',
      },
    })

    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          id: '123',
          name: 'TestName2',
        },
      ],
    })

    const result = await entity.send(queryCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        KeyConditionExpression: '#PK = :v1',
        ExpressionAttributeNames: {
          '#PK': 'PK',
        },
        ExpressionAttributeValues: {
          ':v1': 'ITEM#123',
        },
        ExclusiveStartKey: {
          PK: 'ITEM#123',
          SK: 'NAME#TestName1',
        },
      }),
    )

    expect(result.items).toEqual([
      {
        id: '123',
        name: 'TestName2',
      },
    ])
  })

  it('should skip validation when configured', async () => {
    const queryCommand = new Query({
      key: {
        id: '123',
      },
      skipValidation: true,
    })

    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          id: '123',
          name: 'TestName1',
          extraField: 'This should be ignored',
        },
      ],
    })

    const result = await entity.send(queryCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        KeyConditionExpression: '#PK = :v1',
        ExpressionAttributeNames: {
          '#PK': 'PK',
        },
        ExpressionAttributeValues: {
          ':v1': 'ITEM#123',
        },
      }),
    )

    expect(result.items).toEqual([
      {
        id: '123',
        name: 'TestName1',
        extraField: 'This should be ignored',
      },
    ])
  })

  it('should handle abort signal and timeout', async () => {
    const abortController = new AbortController()

    const queryCommand = new Query({
      key: {
        id: '123',
      },
      abortController,
      timeoutMs: 5000,
    })

    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          id: '123',
          name: 'TestName1',
        },
      ],
    })

    const result = await entity.send(queryCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        KeyConditionExpression: '#PK = :v1',
        ExpressionAttributeNames: {
          '#PK': 'PK',
        },
        ExpressionAttributeValues: {
          ':v1': 'ITEM#123',
        },
      }),
    )

    expect(result.items).toEqual([
      {
        id: '123',
        name: 'TestName1',
      },
    ])
  })

  it('should throw validation error for invalid items', async () => {
    const queryCommand = new Query({
      key: {
        id: '123',
      },
    })

    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          id: '123',
          name: 'TestName1',
        },
        {
          id: '123',
          name: 456, // Invalid type
        },
      ],
    })

    await expect(entity.send(queryCommand)).rejects.toThrow(ZodError)
  })

  it('should return response metadata and consumed capacity', async () => {
    const queryCommand = new Query({
      key: {
        id: '123',
      },
    })

    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          id: '123',
          name: 'TestName1',
        },
      ],
      ConsumedCapacity: {
        TableName: 'TestTable',
        CapacityUnits: 1,
      },
      $metadata: {
        requestId: 'test-request-id',
        httpStatusCode: 200,
      },
    })

    const result = await entity.send(queryCommand)

    expect(result.items).toEqual([
      {
        id: '123',
        name: 'TestName1',
      },
    ])
    expect(result.consumedCapacity).toEqual({
      TableName: 'TestTable',
      CapacityUnits: 1,
    })
    expect(result.responseMetadata).toEqual({
      requestId: 'test-request-id',
      httpStatusCode: 200,
    })
  })

  it('should throw an error for an invalid index', async () => {
    const queryCommand = new Query({
      index: {
        NonExistentIndex: {
          someKey: 'someValue',
        },
      },
    })

    await expect(entity.send(queryCommand)).rejects.toThrow(DocumentBuilderError)
  })

  it('should throw an error if key and index are both missing', async () => {
    // biome-ignore lint/suspicious/noExplicitAny: test case
    const queryCommand = new Query({} as any)

    await expect(entity.send(queryCommand)).rejects.toThrow(DocumentBuilderError)
  })

  it('should throw an error if the index isnt specified correctly', async () => {
    const queryCommand = new Query({
      index: {},
    })

    await expect(entity.send(queryCommand)).rejects.toThrow(DocumentBuilderError)
  })

  it('returns an empty array when no items are found', async () => {
    const queryCommand = new Query({
      key: {
        id: 'non-existent-id',
      },
    })

    dynamoMock.on(QueryCommand).resolves({})

    const result = await entity.send(queryCommand)

    expect(result.items).toEqual([])
  })
})

describe('Projected Query Command', () => {
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
      age: z.number(),
      email: z.string().optional(),
    }),
    partitionKey: item => key('ITEM', item.id),
    sortKey: item => key('NAME', item.name),
  })

  it('should build a Projected Query operation', async () => {
    const queryCommand = new ProjectedQuery({
      key: {
        id: '123',
      },
      projection: ['name', 'age'],
      projectionSchema: z.object({
        name: z.string(),
        age: z.number(),
      }),
    })

    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          name: 'TestName1',
          age: 30,
        },
        {
          name: 'TestName2',
          age: 25,
        },
      ],
    })

    const result = await entity.send(queryCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        KeyConditionExpression: '#PK = :v1',
        ProjectionExpression: '#name, #age',
        ExpressionAttributeNames: {
          '#PK': 'PK',
          '#name': 'name',
          '#age': 'age',
        },
        ExpressionAttributeValues: {
          ':v1': 'ITEM#123',
        },
      }),
    )

    expect(result.items).toEqual([
      {
        name: 'TestName1',
        age: 30,
      },
      {
        name: 'TestName2',
        age: 25,
      },
    ])
  })
})
