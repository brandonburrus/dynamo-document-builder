import { describe, it, expect } from 'vitest'
import { id, documentClient, ttl } from './test-utils'
import { simpleTable } from './test-tables'
import { DynamoEntity, Get, key, Put, Update, Delete } from '@/index'
import { PutCommand, DeleteCommand, GetCommand } from '@aws-sdk/lib-dynamodb'
import { z } from 'zod'

describe('simple table', () => {
  const testId = id()
  console.log(`simple table test id=${testId}`)

  const simpleEntity = new DynamoEntity({
    table: simpleTable,
    schema: z.object({
      id: z.string(),
      title: z.string(),
    }),
    partitionKey: entity => key('TEST', testId, 'SIMPLE', entity.id),
  })

  describe('get command', () => {
    it('gets a simple item by its key', async () => {
      const itemId = 'test1'

      const put = await documentClient.send(
        new PutCommand({
          TableName: simpleTable.tableName,
          Item: {
            Key: `TEST#${testId}#SIMPLE#${itemId}`,
            id: itemId,
            title: 'simple item test',
            TTL: ttl(),
          },
        }),
      )

      expect(put.$metadata.httpStatusCode).toBe(200)

      const getResult = await simpleEntity.send(
        new Get({
          key: {
            id: itemId,
          },
        }),
      )

      expect(getResult.responseMetadata?.httpStatusCode).toBe(200)
      expect(getResult.item).toEqual({
        id: itemId,
        title: 'simple item test',
      })

      const deleteResult = await documentClient.send(
        new DeleteCommand({
          TableName: simpleTable.tableName,
          Key: {
            Key: `TEST#${testId}#SIMPLE#${itemId}`,
          },
        }),
      )

      expect(deleteResult.$metadata.httpStatusCode).toBe(200)
    })

    it('returns undefined for a non-existing simple item', async () => {
      const getResult = await simpleEntity.send(
        new Get({
          key: {
            id: 'non-existing-id',
          },
        }),
      )

      expect(getResult.responseMetadata?.httpStatusCode).toBe(200)
      expect(getResult.item).toBeUndefined()
    })
  })

  describe('put command', () => {
    it('puts a simple item', async () => {
      const itemId = 'test-put-1'

      const putResult = await simpleEntity.send(
        new Put({
          item: {
            id: itemId,
            title: 'simple put item test',
          },
        }),
      )

      expect(putResult.responseMetadata?.httpStatusCode).toBe(200)

      const getResult = await documentClient.send(
        new GetCommand({
          TableName: simpleTable.tableName,
          Key: {
            Key: `TEST#${testId}#SIMPLE#${itemId}`,
          },
        }),
      )

      expect(getResult.$metadata.httpStatusCode).toBe(200)
      expect(getResult.Item).toEqual({
        Key: `TEST#${testId}#SIMPLE#${itemId}`,
        id: itemId,
        title: 'simple put item test',
      })

      const deleteResult = await documentClient.send(
        new DeleteCommand({
          TableName: simpleTable.tableName,
          Key: {
            Key: `TEST#${testId}#SIMPLE#${itemId}`,
          },
        }),
      )

      expect(deleteResult.$metadata.httpStatusCode).toBe(200)
    })
  })

  describe('update command', () => {
    it('updates a simple item', async () => {
      const itemId = 'test-update-1'
      const itemTTL = ttl()

      const put = await documentClient.send(
        new PutCommand({
          TableName: simpleTable.tableName,
          Item: {
            Key: `TEST#${testId}#SIMPLE#${itemId}`,
            id: itemId,
            title: 'simple item to be updated',
            TTL: itemTTL,
          },
        }),
      )

      expect(put.$metadata.httpStatusCode).toBe(200)

      const updateResult = await simpleEntity.send(
        new Update({
          key: {
            id: itemId,
          },
          update: {
            title: 'simple item has been updated',
          },
        }),
      )

      expect(updateResult.responseMetadata?.httpStatusCode).toBe(200)

      const getResult = await documentClient.send(
        new GetCommand({
          TableName: simpleTable.tableName,
          Key: {
            Key: `TEST#${testId}#SIMPLE#${itemId}`,
          },
        }),
      )

      expect(getResult.$metadata.httpStatusCode).toBe(200)
      expect(getResult.Item).toEqual({
        Key: `TEST#${testId}#SIMPLE#${itemId}`,
        id: itemId,
        title: 'simple item has been updated',
        TTL: itemTTL,
      })

      const deleteResult = await documentClient.send(
        new DeleteCommand({
          TableName: simpleTable.tableName,
          Key: {
            Key: `TEST#${testId}#SIMPLE#${itemId}`,
          },
        }),
      )

      expect(deleteResult.$metadata.httpStatusCode).toBe(200)
    })
  })

  describe('delete command', () => {
    it('deletes a simple item', async () => {
      const itemId = 'test-delete-1'

      const put = await documentClient.send(
        new PutCommand({
          TableName: simpleTable.tableName,
          Item: {
            Key: `TEST#${testId}#SIMPLE#${itemId}`,
            id: itemId,
            title: 'simple item to be deleted',
            TTL: ttl(),
          },
        }),
      )

      expect(put.$metadata.httpStatusCode).toBe(200)

      const deleteResult = await simpleEntity.send(
        new Delete({
          key: {
            id: itemId,
          },
        }),
      )

      expect(deleteResult.responseMetadata?.httpStatusCode).toBe(200)

      const getResult = await documentClient.send(
        new GetCommand({
          TableName: simpleTable.tableName,
          Key: {
            Key: `TEST#${testId}#SIMPLE#${itemId}`,
          },
        }),
      )

      expect(getResult.$metadata.httpStatusCode).toBe(200)
      expect(getResult.Item).toBeUndefined()
    })
  })
})
