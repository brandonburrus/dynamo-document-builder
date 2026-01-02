import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { and, DynamoEntity, greaterThan, key, lessThan, Scan } from '@/index'
import { id, documentClient, ttl, wait } from './test-utils'
import { scanTable } from './test-tables'
import { BatchWriteCommand } from '@aws-sdk/lib-dynamodb'
import { z } from 'zod'

describe('scan table', () => {
  const testId = id()
  console.log(`scan table test id=${testId}`)

  const scanEntity = new DynamoEntity({
    table: scanTable,
    schema: z.object({
      primaryId: z.string(),
      itemId: z.string(),
      content: z.string(),
      value: z.number(),
    }),
    partitionKey: entity => key('TEST', testId, 'STANDARD', entity.primaryId),
    sortKey: entity => key('ITEM_ID', entity.itemId),
    globalSecondaryIndexes: {
      GSI1: {
        partitionKey: entity => key('GSI1PK', entity.primaryId, 'TEST', testId),
        sortKey: entity => key('GSI1SK', entity.itemId),
      },
    },
  })

  describe('scan command', () => {
    const testItems = Array.from({ length: 10 }).map((_, i) => ({
      primaryId: `scanpk${i}`,
      itemId: `scansk${i}`,
      content: `scan item ${i}`,
      value: i,
    }))

    beforeAll(async () => {
      await documentClient.send(
        new BatchWriteCommand({
          RequestItems: {
            [scanTable.tableName]: testItems.map(item => ({
              PutRequest: {
                Item: {
                  PK: `TEST#${testId}#STANDARD#${item.primaryId}`,
                  SK: `ITEM_ID#${item.itemId}`,
                  GSI1PK: `GSI1PK#${item.primaryId}#TEST#${testId}`,
                  GSI1SK: `GSI1SK#${item.itemId}`,
                  ...item,
                  TTL: ttl(),
                },
              },
            })),
          },
        }),
      )

      await wait(1_000) // 1 sec for eventual consistency
    })

    afterAll(async () => {
      await documentClient.send(
        new BatchWriteCommand({
          RequestItems: {
            [scanTable.tableName]: testItems.map(item => ({
              DeleteRequest: {
                Key: {
                  PK: `TEST#${testId}#STANDARD#${item.primaryId}`,
                  SK: `ITEM_ID#${item.itemId}`,
                },
              },
            })),
          },
        }),
      )
    })

    it('scans items', async () => {
      const scanResult = await scanEntity.send(new Scan())

      expect(scanResult.responseMetadata?.httpStatusCode).toBe(200)
      expect(scanResult.count).toBe(testItems.length)
      expect(scanResult.items).toHaveLength(testItems.length)
      expect(scanResult.items).toEqual(expect.arrayContaining(testItems))
    })

    it('scans items with pagination', async () => {
      const paginator = scanEntity.paginate(
        new Scan({
          pageSize: 4,
        }),
      )

      const allItems = []
      const expectedItemsPerPage = [4, 4, 2]
      for await (const scanResult of paginator) {
        expect(scanResult.responseMetadata?.httpStatusCode).toBe(200)
        expect(scanResult.items).toHaveLength(scanResult.count)
        expect(scanResult.count).toBe(expectedItemsPerPage.shift())
        allItems.push(...scanResult.items)
      }
      expect(allItems).toHaveLength(testItems.length)
      expect(allItems).toEqual(expect.arrayContaining(testItems))
    })

    it('scans items and applies a filter', async () => {
      const scanResult = await scanEntity.send(
        new Scan({
          filter: and({ value: greaterThan(2) }, { value: lessThan(5) }),
        }),
      )

      expect(scanResult.responseMetadata?.httpStatusCode).toBe(200)
      expect(scanResult.count).toBe(2)
      expect(scanResult.items).toHaveLength(2)
      expect(scanResult.items).toEqual(
        expect.arrayContaining([
          {
            primaryId: 'scanpk3',
            itemId: 'scansk3',
            content: 'scan item 3',
            value: 3,
          },
          {
            primaryId: 'scanpk4',
            itemId: 'scansk4',
            content: 'scan item 4',
            value: 4,
          },
        ]),
      )
    })

    it('scans a secondary index', async () => {
      const scanResult = await scanEntity.send(
        new Scan({
          indexName: 'GSI1',
        }),
      )

      expect(scanResult.responseMetadata?.httpStatusCode).toBe(200)
      expect(scanResult.count).toBe(testItems.length)
      expect(scanResult.items).toHaveLength(testItems.length)
      const sortedItems = testItems.sort((a, b) => a.value - b.value)
      expect(sortedItems).toEqual(expect.arrayContaining(testItems))
    })
  })
})
