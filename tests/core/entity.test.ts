import { describe, expect, it } from 'vitest'
import { z } from 'zod/v4'
import { DynamoEntity } from '@/core/entity'
import { DynamoTable } from '@/core/table'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import { key } from '@/core/key'
import { isoDatetime } from '@/codec/iso-datetime'

describe('DynamoEntity', () => {
  const dynamoClient = new DynamoDBClient()
  const documentClient = DynamoDBDocumentClient.from(dynamoClient)

  describe('constructor and basic setup', () => {
    it('creates entity with default table key names', () => {
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
        partitionKey: item => item.id,
        sortKey: item => item.name,
      })

      expect(entity.table).toBe(table)
      expect(entity.schema).toBeDefined()
    })

    it('creates entity with custom table key names', () => {
      const table = new DynamoTable({
        tableName: 'TestTable',
        documentClient,
        keyNames: {
          partitionKey: 'UserId',
          sortKey: 'Timestamp',
        },
      })

      const entity = new DynamoEntity({
        table,
        schema: z.object({
          userId: z.string(),
          timestamp: z.string(),
        }),
        partitionKey: item => item.userId,
        sortKey: item => item.timestamp,
      })

      expect(entity.table.partitionKeyName).toBe('UserId')
      expect(entity.table.sortKeyName).toBe('Timestamp')
    })
  })

  describe('partition key only (no sort key)', () => {
    const table = new DynamoTable({
      tableName: 'PartitionOnlyTable',
      documentClient,
    })

    const schema = z.object({
      id: z.string(),
      name: z.string(),
      email: z.string(),
    })

    const entity = new DynamoEntity({
      table,
      schema,
      partitionKey: item => key('USER', item.id),
    })

    it('builds partition key without sort key', () => {
      const item = { id: '123', name: 'John', email: 'john@example.com' }

      const pk = entity.buildPartitionKey(item)
      expect(pk).toBe('USER#123')
    })

    it('returns undefined for sort key when not defined', () => {
      const item = { id: '123', name: 'John', email: 'john@example.com' }

      const sk = entity.buildSortKey(item)
      expect(sk).toBeUndefined()
    })

    it('builds primary key with undefined sort key', () => {
      const item = { id: '123', name: 'John', email: 'john@example.com' }

      const primaryKey = entity.buildPrimaryKey(item)
      expect(primaryKey).toEqual({
        PK: 'USER#123',
        SK: undefined,
      })
    })
  })

  describe('partition key with simple sort key', () => {
    const table = new DynamoTable({
      tableName: 'SimpleKeysTable',
      documentClient,
    })

    const schema = z.object({
      userId: z.string(),
      itemId: z.string(),
      name: z.string(),
    })

    const entity = new DynamoEntity({
      table,
      schema,
      partitionKey: item => key('USER', item.userId),
      sortKey: item => key('ITEM', item.itemId),
    })

    it('builds partition key', () => {
      const item = { userId: '123', itemId: '456', name: 'Test Item' }

      const pk = entity.buildPartitionKey(item)
      expect(pk).toBe('USER#123')
    })

    it('builds sort key', () => {
      const item = { userId: '123', itemId: '456', name: 'Test Item' }

      const sk = entity.buildSortKey(item)
      expect(sk).toBe('ITEM#456')
    })

    it('builds complete primary key', () => {
      const item = { userId: '123', itemId: '456', name: 'Test Item' }

      const primaryKey = entity.buildPrimaryKey(item)
      expect(primaryKey).toEqual({
        PK: 'USER#123',
        SK: 'ITEM#456',
      })
    })
  })

  describe('partition key with timestamp sort key', () => {
    const table = new DynamoTable({
      tableName: 'TimestampSortTable',
      documentClient,
    })

    const schema = z.object({
      userId: z.string(),
      timestamp: isoDatetime(),
      content: z.string(),
    })

    const entity = new DynamoEntity({
      table,
      schema,
      partitionKey: item => key('USER', item.userId),
      sortKey: item => item.timestamp.toISOString(),
    })

    it('builds partition key with timestamp sort key', () => {
      const item = {
        userId: '123',
        timestamp: new Date('2024-01-01T00:00:00.000Z'),
        content: 'Test',
      }

      const pk = entity.buildPartitionKey(item)
      expect(pk).toBe('USER#123')
    })

    it('builds sort key from timestamp', () => {
      const item = {
        userId: '123',
        timestamp: new Date('2024-01-01T12:30:45.123Z'),
        content: 'Test',
      }

      const sk = entity.buildSortKey(item)
      expect(sk).toBe('2024-01-01T12:30:45.123Z')
    })

    it('builds primary key with timestamp', () => {
      const item = {
        userId: '123',
        timestamp: new Date('2024-01-01T00:00:00.000Z'),
        content: 'Test',
      }

      const primaryKey = entity.buildPrimaryKey(item)
      expect(primaryKey).toEqual({
        PK: 'USER#123',
        SK: '2024-01-01T00:00:00.000Z',
      })
    })
  })

  describe('partition key with numeric sort key', () => {
    const table = new DynamoTable({
      tableName: 'NumericSortTable',
      documentClient,
    })

    const schema = z.object({
      accountId: z.string(),
      version: z.number(),
      data: z.string(),
    })

    const entity = new DynamoEntity({
      table,
      schema,
      partitionKey: item => key('ACCOUNT', item.accountId),
      sortKey: item => item.version,
    })

    it('builds partition key with numeric sort key', () => {
      const item = { accountId: 'acc-123', version: 1, data: 'v1 data' }

      const pk = entity.buildPartitionKey(item)
      expect(pk).toBe('ACCOUNT#acc-123')
    })

    it('builds numeric sort key', () => {
      const item = { accountId: 'acc-123', version: 42, data: 'v42 data' }

      const sk = entity.buildSortKey(item)
      expect(sk).toBe(42)
    })

    it('builds primary key with numeric sort key', () => {
      const item = { accountId: 'acc-123', version: 100, data: 'v100 data' }

      const primaryKey = entity.buildPrimaryKey(item)
      expect(primaryKey).toEqual({
        PK: 'ACCOUNT#acc-123',
        SK: 100,
      })
    })
  })

  describe('composite partition key', () => {
    const table = new DynamoTable({
      tableName: 'CompositeTable',
      documentClient,
    })

    const schema = z.object({
      tenantId: z.string(),
      orgId: z.string(),
      userId: z.string(),
      name: z.string(),
    })

    const entity = new DynamoEntity({
      table,
      schema,
      partitionKey: item => key(item.tenantId, item.orgId, 'USER', item.userId),
      sortKey: item => key('PROFILE'),
    })

    it('builds composite partition key', () => {
      const item = {
        tenantId: 'tenant-1',
        orgId: 'org-2',
        userId: 'user-3',
        name: 'John',
      }

      const pk = entity.buildPartitionKey(item)
      expect(pk).toBe('tenant-1#org-2#USER#user-3')
    })

    it('builds simple sort key with composite partition', () => {
      const item = {
        tenantId: 'tenant-1',
        orgId: 'org-2',
        userId: 'user-3',
        name: 'John',
      }

      const sk = entity.buildSortKey(item)
      expect(sk).toBe('PROFILE')
    })
  })

  describe('hierarchical sort key', () => {
    const table = new DynamoTable({
      tableName: 'HierarchicalTable',
      documentClient,
    })

    const schema = z.object({
      organizationId: z.string(),
      departmentId: z.string(),
      teamId: z.string(),
      employeeId: z.string(),
    })

    const entity = new DynamoEntity({
      table,
      schema,
      partitionKey: item => key('ORG', item.organizationId),
      sortKey: item => key('DEPT', item.departmentId, 'TEAM', item.teamId, 'EMP', item.employeeId),
    })

    it('builds hierarchical sort key', () => {
      const item = {
        organizationId: 'org-1',
        departmentId: 'dept-2',
        teamId: 'team-3',
        employeeId: 'emp-4',
      }

      const sk = entity.buildSortKey(item)
      expect(sk).toBe('DEPT#dept-2#TEAM#team-3#EMP#emp-4')
    })

    it('enables hierarchical queries with sort key prefix', () => {
      const item = {
        organizationId: 'org-1',
        departmentId: 'dept-2',
        teamId: 'team-3',
        employeeId: 'emp-4',
      }

      const pk = entity.buildPartitionKey(item)
      const sk = entity.buildSortKey(item)

      expect(pk).toBe('ORG#org-1')
      expect(sk).toContain('DEPT#dept-2')
      expect(sk).toContain('TEAM#team-3')
      expect(sk).toContain('EMP#emp-4')
    })
  })

  describe('custom key names', () => {
    const table = new DynamoTable({
      tableName: 'CustomKeysTable',
      documentClient,
      keyNames: {
        partitionKey: 'id',
        sortKey: 'range',
      },
    })

    const schema = z.object({
      userId: z.string(),
      timestamp: z.string(),
    })

    const entity = new DynamoEntity({
      table,
      schema,
      partitionKey: item => item.userId,
      sortKey: item => item.timestamp,
    })

    it('builds primary key with custom key names', () => {
      const item = { userId: 'user-123', timestamp: '2024-01-01' }

      const primaryKey = entity.buildPrimaryKey(item)
      expect(primaryKey).toEqual({
        id: 'user-123',
        range: '2024-01-01',
      })
    })

    it('uses custom partition key name', () => {
      expect(entity.table.partitionKeyName).toBe('id')
    })

    it('uses custom sort key name', () => {
      expect(entity.table.sortKeyName).toBe('range')
    })
  })

  describe('single table design keys', () => {
    const table = new DynamoTable({
      tableName: 'SingleTable',
      documentClient,
      keyNames: {
        partitionKey: 'pk',
        sortKey: 'sk',
      },
    })

    const schema = z.object({
      userId: z.string(),
      itemType: z.string(),
      itemId: z.string(),
    })

    const entity = new DynamoEntity({
      table,
      schema,
      partitionKey: item => key('USER', item.userId),
      sortKey: item => key(item.itemType, item.itemId),
    })

    it('builds keys for single table design', () => {
      const item = {
        userId: 'user-123',
        itemType: 'ORDER',
        itemId: 'order-456',
      }

      const primaryKey = entity.buildPrimaryKey(item)
      expect(primaryKey).toEqual({
        pk: 'USER#user-123',
        sk: 'ORDER#order-456',
      })
    })
  })

  describe('partial item key building', () => {
    const table = new DynamoTable({
      tableName: 'TestTable',
      documentClient,
    })

    const schema = z.object({
      userId: z.string(),
      timestamp: z.string(),
      data: z.string(),
    })

    const entity = new DynamoEntity({
      table,
      schema,
      partitionKey: item => key('USER', item.userId),
      sortKey: item => item.timestamp,
    })

    it('builds partition key from partial item', () => {
      const partialItem = { userId: '123' }

      const pk = entity.buildPartitionKey(partialItem)
      expect(pk).toBe('USER#123')
    })

    it('builds sort key from partial item', () => {
      const partialItem = { timestamp: '2024-01-01' }

      const sk = entity.buildSortKey(partialItem)
      expect(sk).toBe('2024-01-01')
    })

    it('builds primary key from partial item with both keys', () => {
      const partialItem = { userId: '123', timestamp: '2024-01-01' }

      const primaryKey = entity.buildPrimaryKey(partialItem)
      expect(primaryKey).toEqual({
        PK: 'USER#123',
        SK: '2024-01-01',
      })
    })
  })

  describe('validation', () => {
    const table = new DynamoTable({
      tableName: 'TestTable',
      documentClient,
    })

    const schema = z.object({
      id: z.string(),
      name: z.string(),
      age: z.number(),
    })

    const entity = new DynamoEntity({
      table,
      schema,
      partitionKey: item => item.id,
    })

    it('validates a valid item', () => {
      const item = { id: '123', name: 'John', age: 30 }

      const validated = entity.validate(item)
      expect(validated).toEqual(item)
    })

    it('throws error for invalid item', () => {
      const item = { id: '123', name: 'John', age: 'invalid' }

      expect(() => entity.validate(item)).toThrow()
    })

    it('validates item asynchronously', async () => {
      const item = { id: '123', name: 'John', age: 30 }

      const validated = await entity.validateAsync(item)
      expect(validated).toEqual(item)
    })

    it('rejects invalid item asynchronously', async () => {
      const item = { id: '123', name: 'John', age: 'invalid' }

      await expect(entity.validateAsync(item)).rejects.toThrow()
    })

    it('validates partial item', () => {
      const partialItem = { id: '123' }

      const validated = entity.validatePartial(partialItem)
      expect(validated).toEqual({ id: '123' })
    })

    it('validates partial item with multiple fields', () => {
      const partialItem = { id: '123', name: 'John' }

      const validated = entity.validatePartial(partialItem)
      expect(validated).toEqual({ id: '123', name: 'John' })
    })

    it('validates partial item asynchronously', async () => {
      const partialItem = { id: '123', name: 'John' }

      const validated = await entity.validatePartialAsync(partialItem)
      expect(validated).toEqual({ id: '123', name: 'John' })
    })
  })

  describe('encoding', () => {
    const table = new DynamoTable({
      tableName: 'TestTable',
      documentClient,
    })

    const schema = z.object({
      id: z.string(),
      timestamp: isoDatetime(),
    })

    const entity = new DynamoEntity({
      table,
      schema,
      partitionKey: item => item.id,
    })

    it('encodes item with codec', () => {
      const item = {
        id: '123',
        timestamp: new Date('2024-01-01T00:00:00.000Z'),
      }

      const encoded = entity.encode(item)
      expect(encoded.id).toBe('123')
      expect(encoded.timestamp).toBe('2024-01-01T00:00:00.000Z')
    })

    it('encodes item asynchronously', async () => {
      const item = {
        id: '123',
        timestamp: new Date('2024-01-01T00:00:00.000Z'),
      }

      const encoded = await entity.encodeAsync(item)
      expect(encoded.id).toBe('123')
      expect(encoded.timestamp).toBe('2024-01-01T00:00:00.000Z')
    })
  })

  describe('Buffer as partition key', () => {
    const table = new DynamoTable({
      tableName: 'BufferKeyTable',
      documentClient,
    })

    const schema = z.object({
      binaryId: z.instanceof(Buffer),
      data: z.string(),
    })

    const entity = new DynamoEntity({
      table,
      schema,
      partitionKey: item => item.binaryId,
    })

    it('builds partition key from Buffer', () => {
      const buffer = Buffer.from('test-id')
      const item = { binaryId: buffer, data: 'test data' }

      const pk = entity.buildPartitionKey(item)
      expect(pk).toBeInstanceOf(Buffer)
      expect(pk).toEqual(buffer)
    })
  })

  describe('complex multi-entity scenarios', () => {
    const table = new DynamoTable({
      tableName: 'MultiEntityTable',
      documentClient,
      keyNames: {
        partitionKey: 'pk',
        sortKey: 'sk',
      },
    })

    it('supports user entity', () => {
      const userSchema = z.object({
        userId: z.string(),
        email: z.string(),
        name: z.string(),
      })

      const userEntity = new DynamoEntity({
        table,
        schema: userSchema,
        partitionKey: item => key('USER', item.userId),
        sortKey: () => key('PROFILE'),
      })

      const user = {
        userId: 'user-123',
        email: 'user@example.com',
        name: 'John Doe',
      }

      const primaryKey = userEntity.buildPrimaryKey(user)
      expect(primaryKey).toEqual({
        pk: 'USER#user-123',
        sk: 'PROFILE',
      })
    })

    it('supports order entity with user relationship', () => {
      const orderSchema = z.object({
        userId: z.string(),
        orderId: z.string(),
        amount: z.number(),
      })

      const orderEntity = new DynamoEntity({
        table,
        schema: orderSchema,
        partitionKey: item => key('USER', item.userId),
        sortKey: item => key('ORDER', item.orderId),
      })

      const order = {
        userId: 'user-123',
        orderId: 'order-456',
        amount: 99.99,
      }

      const primaryKey = orderEntity.buildPrimaryKey(order)
      expect(primaryKey).toEqual({
        pk: 'USER#user-123',
        sk: 'ORDER#order-456',
      })
    })

    it('supports payment entity with order relationship', () => {
      const paymentSchema = z.object({
        orderId: z.string(),
        paymentId: z.string(),
        amount: z.number(),
      })

      const paymentEntity = new DynamoEntity({
        table,
        schema: paymentSchema,
        partitionKey: item => key('ORDER', item.orderId),
        sortKey: item => key('PAYMENT', item.paymentId),
      })

      const payment = {
        orderId: 'order-456',
        paymentId: 'pay-789',
        amount: 99.99,
      }

      const primaryKey = paymentEntity.buildPrimaryKey(payment)
      expect(primaryKey).toEqual({
        pk: 'ORDER#order-456',
        sk: 'PAYMENT#pay-789',
      })
    })
  })
})
