import { describe, it, expect, beforeEach } from 'vitest'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, ScanCommand } from '@aws-sdk/lib-dynamodb'
import { DynamoEntity, DynamoTable, key } from '@/core'
import { Scan, ProjectedScan } from '@/commands'
import { mockClient } from 'aws-sdk-client-mock'
import { z, ZodError } from 'zod'
import { greaterThan } from '@/conditions'

const dynamoClient = new DynamoDBClient()
const documentClient = DynamoDBDocumentClient.from(dynamoClient)
const dynamoMock = mockClient(DynamoDBDocumentClient)

describe('Scan Command', () => {
  beforeEach(() => dynamoMock.reset())

  const table = new DynamoTable({
    tableName: 'TestTable',
    documentClient,
    keyNames: {
      partitionKey: 'ScanPK',
      sortKey: 'ScanSK',
      globalSecondaryIndexes: {
        GSI1: {
          partitionKey: 'GSI1PK',
          sortKey: 'GSI1SK',
        },
      },
    },
  })

  const entity = new DynamoEntity({
    table,
    schema: z.object({
      id: z.string(),
      value: z.number(),
      category: z.string().optional(),
    }),
    partitionKey: item => key('ID', item.id),
    sortKey: item => item.value,
    globalSecondaryIndexes: {
      GSI1: {
        partitionKey: item => key('CATEGORY', item.category ?? 'UNKNOWN'),
        sortKey: item => item.value,
      },
    },
  })

  it('should build a Scan operation', async () => {
    const scanCommand = new Scan()

    dynamoMock.on(ScanCommand).resolves({
      Items: [
        { ScanPK: 'ID#1', ScanSK: 10, id: '1', value: 10 },
        { ScanPK: 'ID#2', ScanSK: 20, id: '2', value: 20 },
      ],
      Count: 2,
      ScannedCount: 2,
    })

    const result = await entity.send(scanCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
      }),
    )

    expect(result.items).toHaveLength(2)
    expect(result.items).toEqual([
      { id: '1', value: 10 },
      { id: '2', value: 20 },
    ])
    expect(result.count).toBe(2)
    expect(result.scannedCount).toBe(2)
  })

  it('should build a Scan with a filter', async () => {
    const scanCommand = new Scan({
      filter: {
        value: greaterThan(15),
      },
    })

    dynamoMock.on(ScanCommand).resolves({
      Items: [{ ScanPK: 'ID#2', ScanSK: 20, id: '2', value: 20 }],
      Count: 1,
      ScannedCount: 2,
    })

    const result = await entity.send(scanCommand)

    expect(result.items).toHaveLength(1)
    expect(result.items).toEqual([{ id: '2', value: 20 }])
    expect(result.count).toBe(1)
    expect(result.scannedCount).toBe(2)
  })

  it('should build a Scan with a limit', async () => {
    const scanCommand = new Scan({
      limit: 1,
    })

    dynamoMock.on(ScanCommand).resolves({
      Items: [{ ScanPK: 'ID#1', ScanSK: 10, id: '1', value: 10 }],
      Count: 1,
      ScannedCount: 2,
      LastEvaluatedKey: { ScanPK: 'ID#1', ScanSK: 10 },
    })

    const result = await entity.send(scanCommand)

    expect(result.items).toHaveLength(1)
    expect(result.items).toEqual([{ id: '1', value: 10 }])
    expect(result.count).toBe(1)
    expect(result.scannedCount).toBe(2)
    expect(result.lastEvaluatedKey).toEqual({
      ScanPK: 'ID#1',
      ScanSK: 10,
    })
  })

  it('should build a Scan with a consistent read', async () => {
    const scanCommand = new Scan({
      consistent: true,
    })

    dynamoMock.on(ScanCommand).resolves({
      Items: [
        { ScanPK: 'ID#1', ScanSK: 10, id: '1', value: 10 },
        { ScanPK: 'ID#2', ScanSK: 20, id: '2', value: 20 },
      ],
      Count: 2,
      ScannedCount: 2,
    })

    const result = await entity.send(scanCommand)

    expect(result.items).toHaveLength(2)
    expect(result.items).toEqual([
      { id: '1', value: 10 },
      { id: '2', value: 20 },
    ])
    expect(result.count).toBe(2)
    expect(result.scannedCount).toBe(2)
  })

  it('should build a Scan on a GSI', async () => {
    const scanCommand = new Scan({
      indexName: 'GSI1',
    })

    dynamoMock.on(ScanCommand).resolves({
      Items: [
        {
          ScanPK: 'ID#1',
          ScanSK: 10,
          GSI1PK: 'CATEGORY#A',
          GSI1SK: 10,
          id: '1',
          value: 10,
          category: 'A',
        },
        {
          ScanPK: 'ID#2',
          ScanSK: 20,
          GSI1PK: 'CATEGORY#B',
          GSI1SK: 20,
          id: '2',
          value: 20,
          category: 'B',
        },
      ],
      Count: 2,
      ScannedCount: 2,
    })

    const result = await entity.send(scanCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        IndexName: 'GSI1',
      }),
    )

    expect(result.items).toHaveLength(2)
    expect(result.items).toEqual([
      { id: '1', value: 10, category: 'A' },
      { id: '2', value: 20, category: 'B' },
    ])
    expect(result.count).toBe(2)
    expect(result.scannedCount).toBe(2)
  })

  it('should be able to paginate results', async () => {
    const scanCommand = new Scan()

    dynamoMock
      .on(ScanCommand)
      .resolvesOnce({
        Items: [{ ScanPK: 'ID#1', ScanSK: 10, id: '1', value: 10 }],
        Count: 1,
        ScannedCount: 2,
        LastEvaluatedKey: { ScanPK: 'ID#1', ScanSK: 10 },
      })
      .resolvesOnce({
        Items: [{ ScanPK: 'ID#2', ScanSK: 20, id: '2', value: 20 }],
        Count: 1,
        ScannedCount: 1,
      })

    const paginator = entity.paginate(scanCommand)

    const results = []
    let pageCount = 0

    for await (const page of paginator) {
      results.push(...page.items)
      pageCount++
    }

    expect(pageCount).toBe(2)
    expect(results).toHaveLength(2)
    expect(results).toEqual([
      { id: '1', value: 10 },
      { id: '2', value: 20 },
    ])
  })

  it('should return the last evaluated key for manual pagination', async () => {
    const scanCommand = new Scan()

    dynamoMock.on(ScanCommand).resolves({
      Items: [{ ScanPK: 'ID#1', ScanSK: 10, id: '1', value: 10 }],
      Count: 1,
      ScannedCount: 2,
      LastEvaluatedKey: { ScanPK: 'ID#1', ScanSK: 10 },
    })

    const result = await entity.send(scanCommand)

    expect(result.items).toHaveLength(1)
    expect(result.items).toEqual([{ id: '1', value: 10 }])
    expect(result.count).toBe(1)
    expect(result.scannedCount).toBe(2)
    expect(result.lastEvaluatedKey).toEqual({
      ScanPK: 'ID#1',
      ScanSK: 10,
    })
  })

  it('should accept an exclusive start key for manual pagination', async () => {
    const scanCommand = new Scan({
      exclusiveStartKey: {
        ScanPK: 'ID#1',
        ScanSK: 10,
      },
    })

    dynamoMock.on(ScanCommand).resolves({
      Items: [{ ScanPK: 'ID#2', ScanSK: 20, id: '2', value: 20 }],
      Count: 1,
      ScannedCount: 1,
    })

    const result = await entity.send(scanCommand)

    expect(dynamoMock.calls()[0].args[0].input).toEqual(
      expect.objectContaining({
        TableName: 'TestTable',
        ExclusiveStartKey: {
          ScanPK: 'ID#1',
          ScanSK: 10,
        },
      }),
    )

    expect(result.items).toHaveLength(1)
  })

  it('should skip validation when configured', async () => {
    const scanCommand = new Scan({
      skipValidation: true,
    })

    dynamoMock.on(ScanCommand).resolves({
      Items: [{ ScanPK: 'ID#1', ScanSK: 10, id: '1', value: 'INVALID_NUMBER' }],
      Count: 1,
      ScannedCount: 1,
    })

    const result = await entity.send(scanCommand)

    expect(result.items).toHaveLength(1)
    expect(result.items).toEqual([
      {
        ScanPK: 'ID#1',
        ScanSK: 10,
        id: '1',
        value: 'INVALID_NUMBER',
      },
    ])
  })

  it('should handle abort signal and timeout', async () => {
    const abortController = new AbortController()
    const scanCommand = new Scan({
      abortController,
      timeoutMs: 5000,
    })

    dynamoMock.on(ScanCommand).resolves({
      Items: [],
      Count: 0,
      ScannedCount: 0,
    })

    const result = await entity.send(scanCommand)

    expect(result.items).toHaveLength(0)
    expect(result.count).toBe(0)
    expect(result.scannedCount).toBe(0)
  })

  it('should throw validation error for invalid items', async () => {
    const scanCommand = new Scan()

    dynamoMock.on(ScanCommand).resolves({
      Items: [{ ScanPK: 'ID#1', ScanSK: 10, id: '1', value: 'INVALID_NUMBER' }],
      Count: 1,
      ScannedCount: 1,
    })

    await expect(entity.send(scanCommand)).rejects.toThrow(ZodError)
  })
})

describe('Projected Scan Command', () => {
  beforeEach(() => dynamoMock.reset())

  const table = new DynamoTable({
    tableName: 'TestTable',
    documentClient,
    keyNames: {
      partitionKey: 'ProjScanPK',
      sortKey: 'ProjScanSK',
    },
  })

  const entity = new DynamoEntity({
    table,
    schema: z.object({
      id: z.string(),
      value: z.number(),
      category: z.string().optional(),
    }),
    partitionKey: item => key('ID', item.id),
    sortKey: item => item.value,
  })

  it('should build a Projected Scan operation', async () => {
    const projectedScanCommand = new ProjectedScan({
      projection: ['id', 'category'],
      projectionSchema: z.object({
        id: z.string(),
        category: z.string().optional(),
      }),
    })

    dynamoMock.on(ScanCommand).resolves({
      Items: [
        { ProjScanPK: 'ID#1', ProjScanSK: 10, id: '1', category: 'A' },
        { ProjScanPK: 'ID#2', ProjScanSK: 20, id: '2', category: 'B' },
      ],
      Count: 2,
      ScannedCount: 2,
    })

    const result = await entity.send(projectedScanCommand)

    expect(result.items).toHaveLength(2)
    expect(result.items).toEqual([
      { id: '1', category: 'A' },
      { id: '2', category: 'B' },
    ])
    expect(result.count).toBe(2)
    expect(result.scannedCount).toBe(2)
  })
})
