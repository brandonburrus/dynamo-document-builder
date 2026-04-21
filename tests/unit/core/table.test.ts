import { describe, it, expect } from 'vitest'
import { DynamoTable } from '@/core'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'

describe('DynamoTable', () => {
  const dynamoClient = new DynamoDBClient()
  const documentClient = DynamoDBDocumentClient.from(dynamoClient)

  it('should instantiate', () => {
    const table = new DynamoTable({
      tableName: 'TestTable',
      documentClient,
    })

    expect(table).toBeDefined()
    expect(table.tableName).toBe('TestTable')
    expect(table.documentClient).toBeDefined()
  })

  it('should default to PK and SK for key names', () => {
    const table = new DynamoTable({
      tableName: 'TestTable',
      documentClient,
    })

    expect(table.partitionKeyName).toBe('PK')
    expect(table.sortKeyName).toBe('SK')
  })

  it('should accept custom key names', () => {
    const table = new DynamoTable({
      tableName: 'TestTable',
      documentClient,
      keyNames: {
        partitionKey: 'CustomPK',
        sortKey: 'CustomSK',
        globalSecondaryIndexes: {
          GSI1: {
            partitionKey: 'GSI1PK',
            sortKey: 'GSI1SK',
          },
          GSI2: {
            partitionKey: 'CustomGSI2PK',
            sortKey: 'CustomGSI2SK',
          },
        },
        localSecondaryIndexes: {
          LSI1: {
            sortKey: 'CustomLSI1SK',
          },
        },
      },
    })

    expect(table.partitionKeyName).toBe('CustomPK')
    expect(table.sortKeyName).toBe('CustomSK')
    expect(table.globalSecondaryIndexKeyNames).toEqual({
      GSI1: {
        partitionKey: 'GSI1PK',
        sortKey: 'GSI1SK',
      },
      GSI2: {
        partitionKey: 'CustomGSI2PK',
        sortKey: 'CustomGSI2SK',
      },
    })
    expect(table.localSecondaryIndexKeyNames).toEqual({
      LSI1: {
        sortKey: 'CustomLSI1SK',
      },
    })
  })

  it('should provide access to its document client', () => {
    const table = new DynamoTable({
      tableName: 'TestTable',
      documentClient,
    })

    expect(table.documentClient).toBe(documentClient)
  })

  it('should be able to handle a simple primary key', () => {
    const table = new DynamoTable({
      tableName: 'TestTable',
      documentClient,
      keyNames: {
        partitionKey: 'UserId',
        sortKey: null,
      },
    })

    expect(table.partitionKeyName).toBe('UserId')
    expect(table.sortKeyName).toBeNull()
  })

  it('should handle GSIs with simple keys', () => {
    const table = new DynamoTable({
      tableName: 'TestTable',
      documentClient,
      keyNames: {
        partitionKey: 'PK',
        sortKey: 'SK',
        globalSecondaryIndexes: {
          GSI1: {
            partitionKey: 'GSI1PK',
          },
        },
      },
    })

    expect(table.globalSecondaryIndexKeyNames).toEqual({
      GSI1: {
        partitionKey: 'GSI1PK',
        sortKey: undefined,
      },
    })
  })

  describe('lazy client initialization', () => {
    it('should allow construction without a documentClient', () => {
      const table = new DynamoTable({ tableName: 'TestTable' })

      expect(table).toBeDefined()
      expect(table.tableName).toBe('TestTable')
    })

    it('should throw when accessing documentClient before initialization', () => {
      const table = new DynamoTable({ tableName: 'TestTable' })

      expect(() => table.documentClient).toThrow(
        'documentClient has not been set on this DynamoTable',
      )
    })

    it('should allow setting the client via initClient', () => {
      const table = new DynamoTable({ tableName: 'TestTable' })

      table.initClient(documentClient)

      expect(table.documentClient).toBe(documentClient)
    })

    it('should allow overriding the client via initClient', () => {
      const table = new DynamoTable({ tableName: 'TestTable', documentClient })
      const otherClient = DynamoDBDocumentClient.from(new DynamoDBClient())

      table.initClient(otherClient)

      expect(table.documentClient).toBe(otherClient)
    })
  })
})
