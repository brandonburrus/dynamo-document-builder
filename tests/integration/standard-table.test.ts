import { describe, it, expect } from 'vitest'
import { id, documentClient, ttl, wait } from './test-utils'
import { standardTable } from './test-tables'
import {
  beginsWith,
  DynamoEntity,
  Get,
  key,
  Query,
  Put,
  Delete,
  Update,
  BatchGet,
  BatchWrite,
  TransactGet,
  TransactWrite,
} from '@/index'
import {
  PutCommand,
  DeleteCommand,
  BatchWriteCommand,
  BatchGetCommand,
} from '@aws-sdk/lib-dynamodb'
import { z } from 'zod'

describe('standard table', () => {
  const testId = id()
  console.log(`standard table test id=${testId}`)

  const standardEntity = new DynamoEntity({
    table: standardTable,
    schema: z.object({
      primaryId: z.string(),
      itemId: z.string(),
      content: z.string(),
      TTL: z.number().optional(),
    }),
    partitionKey: entity => key('TEST', testId, 'STANDARD', entity.primaryId),
    sortKey: entity => key('ITEM_ID', entity.itemId),
  })

  describe('get command', () => {
    it('gets a standard item by its key', async () => {
      const primaryId = 'test2pk'
      const itemId = 'test2sk'
      const TTL = ttl()

      const put = await documentClient.send(
        new PutCommand({
          TableName: standardTable.tableName,
          Item: {
            PK: `TEST#${testId}#STANDARD#${primaryId}`,
            SK: `ITEM_ID#${itemId}`,
            primaryId,
            itemId,
            content: 'standard item test',
            TTL,
          },
        }),
      )

      expect(put.$metadata.httpStatusCode).toBe(200)

      const getResult = await standardEntity.send(
        new Get({
          key: {
            primaryId,
            itemId,
          },
        }),
      )

      expect(getResult.responseMetadata?.httpStatusCode).toBe(200)
      expect(getResult.item).toEqual({
        primaryId,
        itemId,
        content: 'standard item test',
        TTL,
      })

      const deleteResult = await documentClient.send(
        new DeleteCommand({
          TableName: standardTable.tableName,
          Key: {
            PK: `TEST#${testId}#STANDARD#${primaryId}`,
            SK: `ITEM_ID#${itemId}`,
          },
        }),
      )

      expect(deleteResult.$metadata.httpStatusCode).toBe(200)
    })
  })

  describe('query command', () => {
    it('queries standard items by their partition key', async () => {
      const primaryId = 'querytestpk'
      const TTL = ttl()

      const items = Array.from({ length: 3 }).map((_, i) => ({
        PK: `TEST#${testId}#STANDARD#${primaryId}`,
        SK: `ITEM_ID#item${i}`,
        primaryId,
        itemId: `item${i}`,
        content: `standard item ${i}`,
        TTL,
      }))

      const batchWrite = await documentClient.send(
        new BatchWriteCommand({
          RequestItems: {
            [standardTable.tableName]: items.map(item => ({
              PutRequest: { Item: item },
            })),
          },
        }),
      )

      expect(batchWrite.$metadata.httpStatusCode).toBe(200)

      await wait(500) // half a sec for eventual consistency

      const queryResult = await standardEntity.send(
        new Query({
          key: {
            primaryId,
          },
          sortKeyCondition: {
            SK: beginsWith('ITEM_ID#'),
          },
        }),
      )

      expect(queryResult.responseMetadata?.httpStatusCode).toBe(200)
      expect(queryResult.count).toBe(3)
      expect(queryResult.items).toEqual(
        items.map(item => ({
          primaryId,
          itemId: item.itemId,
          content: item.content,
          TTL,
        })),
      )

      const batchDelete = await documentClient.send(
        new BatchWriteCommand({
          RequestItems: {
            [standardTable.tableName]: items.map(item => ({
              DeleteRequest: { Key: { PK: item.PK, SK: item.SK } },
            })),
          },
        }),
      )

      expect(batchDelete.$metadata.httpStatusCode).toBe(200)
    })
  })

  describe('put command', () => {
    it('puts a standard item', async () => {
      const primaryId = 'puttestpk'
      const itemId = 'puttestsk'
      const TTL = ttl()

      const putResult = await standardEntity.send(
        new Put({
          item: {
            primaryId,
            itemId,
            content: 'put standard item',
            TTL,
          },
        }),
      )

      expect(putResult.responseMetadata?.httpStatusCode).toBe(200)

      const getResult = await standardEntity.send(
        new Get({
          key: {
            primaryId,
            itemId,
          },
        }),
      )

      expect(getResult.responseMetadata?.httpStatusCode).toBe(200)
      expect(getResult.item).toEqual({
        primaryId,
        itemId,
        content: 'put standard item',
        TTL,
      })

      const deleteResult = await standardEntity.send(
        new Delete({
          key: {
            primaryId,
            itemId,
          },
        }),
      )

      expect(deleteResult.responseMetadata?.httpStatusCode).toBe(200)
    })
  })

  describe('delete command', () => {
    it('deletes a standard item', async () => {
      const primaryId = 'deletetestpk'
      const itemId = 'deletetestsk'

      const put = await documentClient.send(
        new PutCommand({
          TableName: standardTable.tableName,
          Item: {
            PK: `TEST#${testId}#STANDARD#${primaryId}`,
            SK: `ITEM_ID#${itemId}`,
            primaryId,
            itemId,
            content: 'standard item to delete',
            TTL: ttl(),
          },
        }),
      )

      expect(put.$metadata.httpStatusCode).toBe(200)

      const deleteResult = await standardEntity.send(
        new Delete({
          key: {
            primaryId,
            itemId,
          },
        }),
      )

      expect(deleteResult.responseMetadata?.httpStatusCode).toBe(200)

      const getResult = await standardEntity.send(
        new Get({
          key: {
            primaryId,
            itemId,
          },
        }),
      )

      expect(getResult.responseMetadata?.httpStatusCode).toBe(200)
      expect(getResult.item).toBeUndefined()
    })
  })

  describe('update command', () => {
    it('updates a standard item', async () => {
      const primaryId = 'updatetestpk'
      const itemId = 'updatetestsk'
      const TTL = ttl()

      const put = await documentClient.send(
        new PutCommand({
          TableName: standardTable.tableName,
          Item: {
            PK: `TEST#${testId}#STANDARD#${primaryId}`,
            SK: `ITEM_ID#${itemId}`,
            primaryId,
            itemId,
            content: 'standard item to update',
            TTL,
          },
        }),
      )

      expect(put.$metadata.httpStatusCode).toBe(200)

      const updateResult = await standardEntity.send(
        new Update({
          key: {
            primaryId,
            itemId,
          },
          update: {
            content: 'updated standard item',
          },
        }),
      )

      expect(updateResult.responseMetadata?.httpStatusCode).toBe(200)

      const getResult = await standardEntity.send(
        new Get({
          key: {
            primaryId,
            itemId,
          },
        }),
      )

      expect(getResult.responseMetadata?.httpStatusCode).toBe(200)
      expect(getResult.item).toEqual({
        primaryId,
        itemId,
        content: 'updated standard item',
        TTL,
      })

      const deleteResult = await standardEntity.send(
        new Delete({
          key: {
            primaryId,
            itemId,
          },
        }),
      )

      expect(deleteResult.responseMetadata?.httpStatusCode).toBe(200)
    })
  })

  describe('batch get command', () => {
    it('batch gets standard items', async () => {
      const primaryId = 'batchgettestpk'
      const TTL = ttl()

      const items = Array.from({ length: 3 }).map((_, i) => ({
        PK: `TEST#${testId}#STANDARD#${primaryId}`,
        SK: `ITEM_ID#item${i}`,
        primaryId,
        itemId: `item${i}`,
        content: `standard item ${i}`,
        TTL,
      }))

      const batchWrite = await documentClient.send(
        new BatchWriteCommand({
          RequestItems: {
            [standardTable.tableName]: items.map(item => ({
              PutRequest: { Item: item },
            })),
          },
        }),
      )

      expect(batchWrite.$metadata.httpStatusCode).toBe(200)

      await wait(500) // half a sec for eventual consistency

      const batchGetResult = await standardEntity.send(
        new BatchGet({
          keys: items.map(item => ({
            primaryId,
            itemId: item.itemId,
          })),
        }),
      )

      expect(batchGetResult.responseMetadata?.httpStatusCode).toBe(200)
      expect(batchGetResult.items).toHaveLength(3)
      expect(batchGetResult.items).toEqual(
        expect.arrayContaining(
          items.map(item => ({
            primaryId,
            itemId: item.itemId,
            content: item.content,
            TTL,
          })),
        ),
      )

      const batchDelete = await documentClient.send(
        new BatchWriteCommand({
          RequestItems: {
            [standardTable.tableName]: items.map(item => ({
              DeleteRequest: { Key: { PK: item.PK, SK: item.SK } },
            })),
          },
        }),
      )

      expect(batchDelete.$metadata.httpStatusCode).toBe(200)
    })
  })

  describe('batch write command', () => {
    it('batch writes puts standard items', async () => {
      const primaryId = 'batchwritetestpk'
      const TTL = ttl()

      const items = Array.from({ length: 3 }).map((_, i) => ({
        primaryId,
        itemId: `item${i}`,
        content: `standard item ${i}`,
        TTL,
      }))

      const batchWriteResult = await standardEntity.send(
        new BatchWrite({
          puts: items,
        }),
      )

      expect(batchWriteResult.responseMetadata?.httpStatusCode).toBe(200)

      await wait(500) // half a sec for eventual consistency

      const batchGetResult = await documentClient.send(
        new BatchGetCommand({
          RequestItems: {
            [standardTable.tableName]: {
              Keys: items.map(item => ({
                PK: `TEST#${testId}#STANDARD#${primaryId}`,
                SK: `ITEM_ID#${item.itemId}`,
              })),
            },
          },
        }),
      )

      const returnedItems = batchGetResult.Responses?.[standardTable.tableName] ?? []

      expect(returnedItems).toHaveLength(3)
      expect(returnedItems).toEqual(
        expect.arrayContaining(
          items.map(item => ({
            PK: `TEST#${testId}#STANDARD#${primaryId}`,
            SK: `ITEM_ID#${item.itemId}`,
            primaryId,
            itemId: item.itemId,
            content: item.content,
            TTL,
          })),
        ),
      )

      const batchDelete = await standardEntity.send(
        new BatchWrite({
          deletes: items.map(item => ({
            primaryId,
            itemId: item.itemId,
          })),
        }),
      )

      expect(batchDelete.responseMetadata?.httpStatusCode).toBe(200)
    })

    it('batch writes deletes standard items', async () => {
      const primaryId = 'batchwritedeletetestpk'
      const TTL = ttl()

      const items = Array.from({ length: 3 }).map((_, i) => ({
        PK: `TEST#${testId}#STANDARD#${primaryId}`,
        SK: `ITEM_ID#item${i}`,
        primaryId,
        itemId: `item${i}`,
        content: `standard item ${i}`,
        TTL,
      }))

      const batchWrite = await documentClient.send(
        new BatchWriteCommand({
          RequestItems: {
            [standardTable.tableName]: items.map(item => ({
              PutRequest: { Item: item },
            })),
          },
        }),
      )

      expect(batchWrite.$metadata.httpStatusCode).toBe(200)

      await wait(500) // half a sec for eventual consistency

      const batchDeleteResult = await standardEntity.send(
        new BatchWrite({
          deletes: items.map(item => ({
            primaryId,
            itemId: item.itemId,
          })),
        }),
      )

      expect(batchDeleteResult.responseMetadata?.httpStatusCode).toBe(200)

      const batchGetResult = await documentClient.send(
        new BatchGetCommand({
          RequestItems: {
            [standardTable.tableName]: {
              Keys: items.map(item => ({
                PK: item.PK,
                SK: item.SK,
              })),
            },
          },
        }),
      )

      const returnedItems = batchGetResult.Responses?.[standardTable.tableName] ?? []

      expect(returnedItems).toHaveLength(0)
    })
  })

  describe('transact get command', () => {
    it('transact gets standard items', async () => {
      const primaryId = 'transactgettestpk'
      const TTL = ttl()

      const items = Array.from({ length: 2 }).map((_, i) => ({
        PK: `TEST#${testId}#STANDARD#${primaryId}`,
        SK: `ITEM_ID#item${i}`,
        primaryId,
        itemId: `item${i}`,
        content: `standard item ${i}`,
        TTL,
      }))

      const batchWrite = await documentClient.send(
        new BatchWriteCommand({
          RequestItems: {
            [standardTable.tableName]: items.map(item => ({
              PutRequest: { Item: item },
            })),
          },
        }),
      )

      expect(batchWrite.$metadata.httpStatusCode).toBe(200)

      await wait(500) // half a sec for eventual consistency

      const transactGetResult = await standardEntity.send(
        new TransactGet({
          keys: items.map(item => ({
            primaryId,
            itemId: item.itemId,
          })),
        }),
      )

      expect(transactGetResult.responseMetadata?.httpStatusCode).toBe(200)
      expect(transactGetResult.items).toHaveLength(2)
      expect(transactGetResult.items).toEqual(
        items.map(item => ({
          primaryId,
          itemId: item.itemId,
          content: item.content,
          TTL,
        })),
      )

      const batchDelete = await documentClient.send(
        new BatchWriteCommand({
          RequestItems: {
            [standardTable.tableName]: items.map(item => ({
              DeleteRequest: { Key: { PK: item.PK, SK: item.SK } },
            })),
          },
        }),
      )

      expect(batchDelete.$metadata.httpStatusCode).toBe(200)
    })
  })

  describe('transact write command', () => {
    it('transact writes puts standard items', async () => {
      const primaryId = 'transactwritetestpk'
      const TTL = ttl()

      const items = Array.from({ length: 2 }).map((_, i) => ({
        primaryId,
        itemId: `item${i}`,
        content: `standard item ${i}`,
        TTL,
      }))

      const transactWriteResult = await standardEntity.send(
        new TransactWrite({
          writes: items.map(
            item =>
              new Put({
                item,
              }),
          ),
        }),
      )

      expect(transactWriteResult.responseMetadata?.httpStatusCode).toBe(200)

      await wait(500) // half a sec for eventual consistency

      const batchGetResult = await documentClient.send(
        new BatchGetCommand({
          RequestItems: {
            [standardTable.tableName]: {
              Keys: items.map(item => ({
                PK: `TEST#${testId}#STANDARD#${primaryId}`,
                SK: `ITEM_ID#${item.itemId}`,
              })),
            },
          },
        }),
      )

      const returnedItems = batchGetResult.Responses?.[standardTable.tableName] ?? []

      expect(returnedItems).toHaveLength(2)
      expect(returnedItems).toEqual(
        expect.arrayContaining(
          items.map(item => ({
            PK: `TEST#${testId}#STANDARD#${primaryId}`,
            SK: `ITEM_ID#${item.itemId}`,
            primaryId,
            itemId: item.itemId,
            content: item.content,
            TTL,
          })),
        ),
      )

      const batchDelete = await documentClient.send(
        new BatchWriteCommand({
          RequestItems: {
            [standardTable.tableName]: items.map(item => ({
              DeleteRequest: {
                Key: { PK: `TEST#${testId}#STANDARD#${primaryId}`, SK: `ITEM_ID#${item.itemId}` },
              },
            })),
          },
        }),
      )

      expect(batchDelete.$metadata.httpStatusCode).toBe(200)
    })
  })
})
