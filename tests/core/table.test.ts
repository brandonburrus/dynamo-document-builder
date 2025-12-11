import { describe, expect, it } from 'vitest'
import { DynamoTable } from '@/core/table'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'

describe('DynamoTable', () => {
  const dynamoClient = new DynamoDBClient()
  const documentClient = DynamoDBDocumentClient.from(dynamoClient)

  describe('constructor', () => {
    it('creates table with default key names', () => {
      const table = new DynamoTable({
        tableName: 'TestTable',
        documentClient,
      })

      expect(table.tableName).toBe('TestTable')
      expect(table.documentClient).toBe(documentClient)
      expect(table.partitionKeyName).toBe('PK')
      expect(table.sortKeyName).toBe('SK')
    })

    it('creates table with custom partition key name', () => {
      const table = new DynamoTable({
        tableName: 'TestTable',
        documentClient,
        keyNames: {
          partitionKey: 'CustomPK',
        },
      })

      expect(table.partitionKeyName).toBe('CustomPK')
      expect(table.sortKeyName).toBe('SK')
    })

    it('creates table with custom sort key name', () => {
      const table = new DynamoTable({
        tableName: 'TestTable',
        documentClient,
        keyNames: {
          partitionKey: 'PK',
          sortKey: 'CustomSK',
        },
      })

      expect(table.partitionKeyName).toBe('PK')
      expect(table.sortKeyName).toBe('CustomSK')
    })

    it('creates table with both custom key names', () => {
      const table = new DynamoTable({
        tableName: 'TestTable',
        documentClient,
        keyNames: {
          partitionKey: 'UserId',
          sortKey: 'Timestamp',
        },
      })

      expect(table.partitionKeyName).toBe('UserId')
      expect(table.sortKeyName).toBe('Timestamp')
    })

    it('creates table with single table design key names', () => {
      const table = new DynamoTable({
        tableName: 'SingleTableDesign',
        documentClient,
        keyNames: {
          partitionKey: 'pk',
          sortKey: 'sk',
        },
      })

      expect(table.partitionKeyName).toBe('pk')
      expect(table.sortKeyName).toBe('sk')
    })
  })

  describe('getters', () => {
    it('returns table name', () => {
      const table = new DynamoTable({
        tableName: 'MyTable',
        documentClient,
      })

      expect(table.tableName).toBe('MyTable')
    })

    it('returns document client', () => {
      const table = new DynamoTable({
        tableName: 'TestTable',
        documentClient,
      })

      expect(table.documentClient).toBe(documentClient)
    })

    it('returns partition key name', () => {
      const table = new DynamoTable({
        tableName: 'TestTable',
        documentClient,
        keyNames: {
          partitionKey: 'id',
        },
      })

      expect(table.partitionKeyName).toBe('id')
    })

    it('returns sort key name', () => {
      const table = new DynamoTable({
        tableName: 'TestTable',
        documentClient,
        keyNames: {
          partitionKey: 'PK',
          sortKey: 'range',
        },
      })

      expect(table.sortKeyName).toBe('range')
    })
  })
})
