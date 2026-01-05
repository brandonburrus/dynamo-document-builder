import { describe, it, expect, beforeEach } from 'vitest'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, UpdateCommand } from '@aws-sdk/lib-dynamodb'
import { DynamoEntity, DynamoTable, key } from '@/core'
import { Update, ConditionalUpdate } from '@/commands'
import { mockClient } from 'aws-sdk-client-mock'
import { z, ZodError } from 'zod'
import { ref, remove, add, subtract, append, prepend, addToSet, removeFromSet } from '@/updates'

const dynamoClient = new DynamoDBClient()
const documentClient = DynamoDBDocumentClient.from(dynamoClient)
const dynamoMock = mockClient(DynamoDBDocumentClient)

describe('Update Command', () => {
  beforeEach(() => dynamoMock.reset())

  const table = new DynamoTable({
    tableName: 'TestTable',
    documentClient,
  })

  const entity = new DynamoEntity({
    table,
    schema: z.object({
      id: z.string(),
      value: z.number(),
    }),
    partitionKey: item => key('ITEM', item.id),
    sortKey: item => key('VALUE', item.value),
  })

  it('should build an UpdateItem operation', async () => {
    const updateCommand = new Update({
      key: {
        id: '456',
        value: 100,
      },
      update: {
        value: 200,
      },
    })

    dynamoMock.on(UpdateCommand).resolves({})

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#456',
          SK: 'VALUE#100',
        },
        UpdateExpression: 'SET #value = :v1',
        ExpressionAttributeNames: {
          '#value': 'value',
        },
        ExpressionAttributeValues: {
          ':v1': 200,
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should update multiple attributes', async () => {
    const updateCommand = new Update({
      key: {
        id: '789',
        value: 300,
      },
      update: {
        value: 400,
        extra: 'additional',
      },
    })

    dynamoMock.on(UpdateCommand).resolves({})

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#789',
          SK: 'VALUE#300',
        },
        UpdateExpression: 'SET #value = :v1, #extra = :v2',
        ExpressionAttributeNames: {
          '#value': 'value',
          '#extra': 'extra',
        },
        ExpressionAttributeValues: {
          ':v1': 400,
          ':v2': 'additional',
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should update nested attributes', async () => {
    const nestedAttrEntity = new DynamoEntity({
      table,
      schema: z.object({
        id: z.string(),
        details: z.object({
          age: z.number(),
          address: z.string(),
        }),
      }),
      partitionKey: item => key('ITEM', item.id),
    })

    const updateCommand = new Update({
      key: {
        id: '101',
      },
      update: {
        'details.age': 30,
        'details.address': '123 Main St',
      },
    })

    dynamoMock.on(UpdateCommand).resolves({})

    const result = await nestedAttrEntity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#101',
        },
        UpdateExpression: 'SET #details.#age = :v1, #details.#address = :v2',
        ExpressionAttributeNames: {
          '#details': 'details',
          '#age': 'age',
          '#address': 'address',
        },
        ExpressionAttributeValues: {
          ':v1': 30,
          ':v2': '123 Main St',
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should update one attribute to reference another attribute', async () => {
    const updateCommand = new Update({
      key: {
        id: '202',
        value: 500,
      },
      update: {
        value: ref('other-attr'),
      },
    })

    dynamoMock.on(UpdateCommand).resolves({})

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#202',
          SK: 'VALUE#500',
        },
        UpdateExpression: 'SET #value = #other-attr',
        ExpressionAttributeNames: {
          '#value': 'value',
          '#other-attr': 'other-attr',
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should update one attribute to reference another attribute with a default value', async () => {
    const updateCommand = new Update({
      key: {
        id: '303',
        value: 600,
      },
      update: {
        value: ref('missing-attr', 999),
      },
    })

    dynamoMock.on(UpdateCommand).resolves({})

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#303',
          SK: 'VALUE#600',
        },
        UpdateExpression: 'SET #value = if_not_exists(#missing-attr, :v1)',
        ExpressionAttributeNames: {
          '#value': 'value',
          '#missing-attr': 'missing-attr',
        },
        ExpressionAttributeValues: {
          ':v1': 999,
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should update one attribute to reference another nested attribute', async () => {
    const nestedAttrEntity = new DynamoEntity({
      table,
      schema: z.object({
        id: z.string(),
        details: z.object({
          age: z.number(),
          address: z.string(),
        }),
      }),
      partitionKey: item => key('ITEM', item.id),
    })

    const updateCommand = new Update({
      key: {
        id: '404',
      },
      update: {
        'details.age': ref('details.address'),
      },
    })

    dynamoMock.on(UpdateCommand).resolves({})

    const result = await nestedAttrEntity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#404',
        },
        UpdateExpression: 'SET #details.#age = #details.#address',
        ExpressionAttributeNames: {
          '#details': 'details',
          '#age': 'age',
          '#address': 'address',
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should update one attribute to an attribute nested in a list', async () => {
    const updateCommand = new Update({
      key: {
        id: '505',
        value: 700,
      },
      update: {
        value: ref('listAttr[2]'),
      },
    })

    dynamoMock.on(UpdateCommand).resolves({})

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#505',
          SK: 'VALUE#700',
        },
        UpdateExpression: 'SET #value = #listAttr[2]',
        ExpressionAttributeNames: {
          '#value': 'value',
          '#listAttr': 'listAttr',
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should remove an attribute', async () => {
    const updateCommand = new Update({
      key: {
        id: '606',
        value: 800,
      },
      update: {
        extra: remove(),
      },
    })

    dynamoMock.on(UpdateCommand).resolves({})

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#606',
          SK: 'VALUE#800',
        },
        UpdateExpression: 'REMOVE #extra',
        ExpressionAttributeNames: {
          '#extra': 'extra',
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should increment a numeric attribute', async () => {
    const updateCommand = new Update({
      key: {
        id: '707',
        value: 900,
      },
      update: {
        value: add(10),
      },
    })

    dynamoMock.on(UpdateCommand).resolves({})

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#707',
          SK: 'VALUE#900',
        },
        UpdateExpression: 'SET #value = #value + :v1',
        ExpressionAttributeNames: {
          '#value': 'value',
        },
        ExpressionAttributeValues: {
          ':v1': 10,
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should decrement a numeric attribute', async () => {
    const updateCommand = new Update({
      key: {
        id: '808',
        value: 1000,
      },
      update: {
        value: subtract(5),
      },
    })

    dynamoMock.on(UpdateCommand).resolves({})

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#808',
          SK: 'VALUE#1000',
        },
        UpdateExpression: 'SET #value = #value - :v1',
        ExpressionAttributeNames: {
          '#value': 'value',
        },
        ExpressionAttributeValues: {
          ':v1': 5,
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should append items to a list attribute', async () => {
    const updateCommand = new Update({
      key: {
        id: '909',
        value: 1100,
      },
      update: {
        tags: append(['newTag1', 'newTag2']),
      },
    })

    dynamoMock.on(UpdateCommand).resolves({})

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#909',
          SK: 'VALUE#1100',
        },
        UpdateExpression: 'SET #tags = list_append(#tags, :v1)',
        ExpressionAttributeNames: {
          '#tags': 'tags',
        },
        ExpressionAttributeValues: {
          ':v1': ['newTag1', 'newTag2'],
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should prepend items to a list attribute', async () => {
    const updateCommand = new Update({
      key: {
        id: '010',
        value: 1200,
      },
      update: {
        tags: prepend(['startTag1', 'startTag2']),
      },
    })

    dynamoMock.on(UpdateCommand).resolves({})

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#010',
          SK: 'VALUE#1200',
        },
        UpdateExpression: 'SET #tags = list_append(:v1, #tags)',
        ExpressionAttributeNames: {
          '#tags': 'tags',
        },
        ExpressionAttributeValues: {
          ':v1': ['startTag1', 'startTag2'],
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should add items to a set attribute', async () => {
    const updateCommand = new Update({
      key: {
        id: '111',
        value: 1300,
      },
      update: {
        colors: addToSet(['red', 'blue']),
      },
    })

    dynamoMock.on(UpdateCommand).resolves({})

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#111',
          SK: 'VALUE#1300',
        },
        UpdateExpression: 'ADD #colors :v1',
        ExpressionAttributeNames: {
          '#colors': 'colors',
        },
        ExpressionAttributeValues: {
          ':v1': new Set(['red', 'blue']),
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should remove items from a set attribute', async () => {
    const updateCommand = new Update({
      key: {
        id: '222',
        value: 1400,
      },
      update: {
        colors: removeFromSet(['green', 'yellow']),
      },
    })

    dynamoMock.on(UpdateCommand).resolves({})

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#222',
          SK: 'VALUE#1400',
        },
        UpdateExpression: 'DELETE #colors :v1',
        ExpressionAttributeNames: {
          '#colors': 'colors',
        },
        ExpressionAttributeValues: {
          ':v1': new Set(['green', 'yellow']),
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should return the updated item', async () => {
    const updateCommand = new Update({
      key: {
        id: '333',
        value: 1500,
      },
      update: {
        value: 1600,
      },
      returnValues: 'ALL_NEW',
    })

    dynamoMock.on(UpdateCommand).resolves({
      Attributes: {
        id: '333',
        value: 1600,
      },
    })

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#333',
          SK: 'VALUE#1500',
        },
        UpdateExpression: 'SET #value = :v1',
        ExpressionAttributeNames: {
          '#value': 'value',
        },
        ExpressionAttributeValues: {
          ':v1': 1600,
        },
        ReturnValues: 'ALL_NEW',
      }),
    )

    expect(result.updatedItem).toEqual({
      id: '333',
      value: 1600,
    })
  })

  it('should return the old item', async () => {
    const updateCommand = new Update({
      key: {
        id: '444',
        value: 1700,
      },
      update: {
        value: 1800,
      },
      returnValues: 'ALL_OLD',
    })

    dynamoMock.on(UpdateCommand).resolves({
      Attributes: {
        id: '444',
        value: 1700,
      },
    })

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#444',
          SK: 'VALUE#1700',
        },
        UpdateExpression: 'SET #value = :v1',
        ExpressionAttributeNames: {
          '#value': 'value',
        },
        ExpressionAttributeValues: {
          ':v1': 1800,
        },
        ReturnValues: 'ALL_OLD',
      }),
    )

    expect(result.updatedItem).toEqual({
      id: '444',
      value: 1700,
    })
  })

  it('should throw a validation error for invalid updated item', async () => {
    const updateCommand = new Update({
      key: {
        id: '555',
        value: 1900,
      },
      update: {
        value: 'invalid-number',
      },
    })

    dynamoMock.on(UpdateCommand).resolves({
      Attributes: {
        id: '555',
        value: 'invalid-number',
      },
    })

    await expect(entity.send(updateCommand)).rejects.toThrow(ZodError)
  })

  it('should handle skipping validation', async () => {
    const updateCommand = new Update({
      key: {
        id: '123',
        value: 1000,
      },
      update: {
        value: 'bypass-validation',
      },
      skipValidation: true,
    })

    dynamoMock.on(UpdateCommand).resolves({
      Attributes: {
        id: '123',
        value: 'bypass-validation',
      },
    })

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#123',
          SK: 'VALUE#1000',
        },
        UpdateExpression: 'SET #value = :v1',
        ExpressionAttributeNames: {
          '#value': 'value',
        },
        ExpressionAttributeValues: {
          ':v1': 'bypass-validation',
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should handle consumed capacity and item collection metrics', async () => {
    const updateCommand = new Update({
      key: {
        id: '666',
        value: 2000,
      },
      update: {
        value: 2100,
      },
      returnConsumedCapacity: 'TOTAL',
      returnItemCollectionMetrics: 'SIZE',
    })

    dynamoMock.on(UpdateCommand).resolves({
      ConsumedCapacity: {
        TableName: 'TestTable',
        CapacityUnits: 1,
      },
      ItemCollectionMetrics: {
        ItemCollectionKey: {
          PK: 'ITEM#666',
        },
        SizeEstimateRangeGB: [0.0, 1.0],
      },
    })

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#666',
          SK: 'VALUE#2000',
        },
        UpdateExpression: 'SET #value = :v1',
        ExpressionAttributeNames: {
          '#value': 'value',
        },
        ExpressionAttributeValues: {
          ':v1': 2100,
        },
        ReturnConsumedCapacity: 'TOTAL',
        ReturnItemCollectionMetrics: 'SIZE',
      }),
    )

    expect(result.consumedCapacity).toEqual({
      TableName: 'TestTable',
      CapacityUnits: 1,
    })

    expect(result.itemCollectionMetrics).toEqual({
      ItemCollectionKey: {
        PK: 'ITEM#666',
      },
      SizeEstimateRangeGB: [0.0, 1.0],
    })
  })

  it('should handle abort signal and timeout', async () => {
    const abortController = new AbortController()

    const updateCommand = new Update({
      key: {
        id: '777',
        value: 2200,
      },
      update: {
        value: 2300,
      },
      abortController,
      timeoutMs: 5000,
    })

    dynamoMock.on(UpdateCommand).resolves({})

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#777',
          SK: 'VALUE#2200',
        },
        UpdateExpression: 'SET #value = :v1',
        ExpressionAttributeNames: {
          '#value': 'value',
        },
        ExpressionAttributeValues: {
          ':v1': 2300,
        },
      }),
    )

    expect(result).toBeDefined()
  })
})

describe('Conditional Update Command', () => {
  beforeEach(() => dynamoMock.reset())

  const table = new DynamoTable({
    tableName: 'TestTable',
    documentClient,
  })

  const entity = new DynamoEntity({
    table,
    schema: z.object({
      id: z.string(),
      value: z.number(),
    }),
    partitionKey: item => key('ITEM', item.id),
    sortKey: item => key('VALUE', item.value),
  })

  it('should build a conditional UpdateItem operation', async () => {
    const updateCommand = new ConditionalUpdate({
      key: {
        id: '888',
        value: 2400,
      },
      update: {
        value: 2500,
      },
      condition: {
        value: 2400,
      },
    })

    dynamoMock.on(UpdateCommand).resolves({})

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#888',
          SK: 'VALUE#2400',
        },
        UpdateExpression: 'SET #value = :v1',
        ConditionExpression: '#value = :v2',
        ExpressionAttributeNames: {
          '#value': 'value',
        },
        ExpressionAttributeValues: {
          ':v1': 2500,
          ':v2': 2400,
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should throw an error if the condition is not met', async () => {})

  it('should skip validation when specified', async () => {
    const updateCommand = new ConditionalUpdate({
      key: {
        id: '999',
        value: 2600,
      },
      update: {
        value: 'invalid-number',
      },
      condition: {
        value: 2600,
      },
      skipValidation: true,
    })

    dynamoMock.on(UpdateCommand).resolves({})

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#999',
          SK: 'VALUE#2600',
        },
        UpdateExpression: 'SET #value = :v1',
        ConditionExpression: '#value = :v2',
        ExpressionAttributeNames: {
          '#value': 'value',
        },
        ExpressionAttributeValues: {
          ':v1': 'invalid-number',
          ':v2': 2600,
        },
      }),
    )

    expect(result).toBeDefined()
  })

  it('should return the updated item when condition is met', async () => {
    const updateCommand = new ConditionalUpdate({
      key: {
        id: '112',
        value: 2700,
      },
      update: {
        value: 2800,
      },
      condition: {
        value: 2700,
      },
      returnValues: 'ALL_NEW',
    })

    dynamoMock.on(UpdateCommand).resolves({
      Attributes: {
        id: '112',
        value: 2800,
      },
    })

    const result = await entity.send(updateCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        Key: {
          PK: 'ITEM#112',
          SK: 'VALUE#2700',
        },
        UpdateExpression: 'SET #value = :v1',
        ConditionExpression: '#value = :v2',
        ExpressionAttributeNames: {
          '#value': 'value',
        },
        ExpressionAttributeValues: {
          ':v1': 2800,
          ':v2': 2700,
        },
        ReturnValues: 'ALL_NEW',
      }),
    )

    expect(result.updatedItem).toEqual({
      id: '112',
      value: 2800,
    })
  })
})
