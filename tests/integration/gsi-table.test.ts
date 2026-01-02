import { describe, it, expect } from 'vitest'
import { DynamoEntity, Get, key, Query } from '@/index'
import { id, documentClient, ttl, wait } from './test-utils'
import { gsiTable } from './test-tables'
import { PutCommand, DeleteCommand, BatchWriteCommand } from '@aws-sdk/lib-dynamodb'
import { z } from 'zod'

describe('gsi table', () => {
  const testId = id()
  console.log(`gsi table test id=${testId}`)

  const gsiEntity = new DynamoEntity({
    table: gsiTable,
    schema: z.object({
      id: z.string(),
      sortId: z.string(),
      gsi1Pk: z.string(),
      gsi1Sk: z.string(),
      content: z.string(),
    }),
    partitionKey: entity => key('TEST', testId, 'ID', entity.id),
    sortKey: entity => key('SORT', entity.sortId),
    globalSecondaryIndexes: {
      GSI1: {
        partitionKey: entity => key('GSI1PK', testId, entity.gsi1Pk),
        sortKey: entity => key('GSI1SK', entity.gsi1Sk),
      },
    },
  })

  describe('get command', () => {
    it('gets a gsi item by its primary key', async () => {
      const itemId = 'test3'
      const sortId = 'sort3'
      const gsi1Pk = 'gsi1pk3'
      const gsi1Sk = 'gsi1sk3'

      const put = await documentClient.send(
        new PutCommand({
          TableName: gsiTable.tableName,
          Item: {
            PK: `TEST#${testId}#ID#${itemId}`,
            SK: `SORT#${sortId}`,
            GSI1PK: `GSI1PK#${testId}#${gsi1Pk}`,
            GSI1SK: `GSI1SK#${gsi1Sk}`,
            id: itemId,
            sortId,
            gsi1Pk,
            gsi1Sk,
            content: 'gsi item test',
            TTL: ttl(),
          },
        }),
      )

      expect(put.$metadata.httpStatusCode).toBe(200)

      const getResult = await gsiEntity.send(
        new Get({
          key: {
            id: itemId,
            sortId,
          },
        }),
      )

      expect(getResult.responseMetadata?.httpStatusCode).toBe(200)
      expect(getResult.item).toEqual({
        id: itemId,
        sortId,
        gsi1Pk,
        gsi1Sk,
        content: 'gsi item test',
      })

      const deleteResult = await documentClient.send(
        new DeleteCommand({
          TableName: gsiTable.tableName,
          Key: {
            PK: `TEST#${testId}#ID#${itemId}`,
            SK: `SORT#${sortId}`,
          },
        }),
      )

      expect(deleteResult.$metadata.httpStatusCode).toBe(200)
    })

    // you would think GetItem would support secondary indexes, but nope lol
  })

  describe('query command', () => {
    it('queries a gsi item by its gsi key', async () => {
      const itemId = 'gsiquerytestpk'
      const sortIdPrefix = 'gsiquerysort'
      const gsi1Pk = 'gsi1pk-querytest'

      const items = Array.from({ length: 3 }).map((_, i) => ({
        PK: `TEST#${testId}#ID#${itemId}-${i}`,
        SK: `SORT#${sortIdPrefix}${i}`,
        GSI1PK: `GSI1PK#${testId}#${gsi1Pk}`,
        GSI1SK: `GSI1SK#gsiquerysk${i}`,
        id: `${itemId}-${i}`,
        sortId: `${sortIdPrefix}${i}`,
        gsi1Pk,
        gsi1Sk: `gsiquerysk${i}`,
        content: `gsi item ${i}`,
        TTL: ttl(),
      }))

      const batchWrite = await documentClient.send(
        new BatchWriteCommand({
          RequestItems: {
            [gsiTable.tableName]: items.map(item => ({
              PutRequest: { Item: item },
            })),
          },
        }),
      )

      expect(batchWrite.$metadata.httpStatusCode).toBe(200)

      await wait(500) // half a sec for eventual consistency

      const queryResult = await gsiEntity.send(
        new Query({
          index: {
            GSI1: {
              gsi1Pk,
            },
          },
        }),
      )

      expect(queryResult.responseMetadata?.httpStatusCode).toBe(200)
      expect(queryResult.items).toHaveLength(3)
      expect(queryResult.items).toEqual([
        {
          id: `${itemId}-0`,
          sortId: `${sortIdPrefix}0`,
          gsi1Pk,
          gsi1Sk: 'gsiquerysk0',
          content: 'gsi item 0',
        },
        {
          id: `${itemId}-1`,
          sortId: `${sortIdPrefix}1`,
          gsi1Pk,
          gsi1Sk: 'gsiquerysk1',
          content: 'gsi item 1',
        },
        {
          id: `${itemId}-2`,
          sortId: `${sortIdPrefix}2`,
          gsi1Pk,
          gsi1Sk: 'gsiquerysk2',
          content: 'gsi item 2',
        },
      ])

      const batchDelete = await documentClient.send(
        new BatchWriteCommand({
          RequestItems: {
            [gsiTable.tableName]: items.map(item => ({
              DeleteRequest: { Key: { PK: item.PK, SK: item.SK } },
            })),
          },
        }),
      )

      expect(batchDelete.$metadata.httpStatusCode).toBe(200)
    })
  })
})
