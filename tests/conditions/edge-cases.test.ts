import { beforeEach, describe, expect, it } from 'vitest'
import { DeleteCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import { ConditionalDelete } from '@/commands/conditional-delete'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoEntity } from '@/core/entity'
import { DynamoTable } from '@/core/table'
import { and, or, size, greaterThan, equals } from '@/conditions'
import { isoDatetime } from '@/codec'
import { key } from '@/core/key'
import { mockClient } from 'aws-sdk-client-mock'
import { z } from 'zod/v4'

describe('condition parser edge cases', () => {
  const dynamoClient = new DynamoDBClient()
  const dynamoMock = mockClient(DynamoDBDocumentClient)

  beforeEach(() => {
    dynamoMock.reset()
  })

  const testTable: DynamoTable = new DynamoTable({
    tableName: 'TestTable',
    documentClient: DynamoDBDocumentClient.from(dynamoClient),
  })

  const testEntity = new DynamoEntity({
    table: testTable,
    schema: z.object({
      id: z.string(),
      name: z.string(),
      timestamp: isoDatetime(),
    }),
    partitionKey: item => key('TEST', item.id),
    sortKey: item => item.timestamp.toISOString(),
  })

  it('handles size expression in value expression path', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: equals(size('otherField')),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name = size(#otherField)',
      ExpressionAttributeNames: {
        '#name': 'name',
        '#otherField': 'otherField',
      },
    })
    expect(dynamoMock.call(0).args[0].input).not.toHaveProperty('ExpressionAttributeValues')
  })

  it('handles logical expression with single-condition template (no wrapping)', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: or({ name: equals('A') }, { id: equals('B') }),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name = :v1 OR #id = :v2',
    })
  })

  it('handles logical expression with multi-condition template (with wrapping)', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: or({ name: equals('A'), id: equals('1') }, { name: equals('B') }),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    const conditionExpression = dynamoMock.call(0).args[0].input.ConditionExpression
    // Multi-condition template should be wrapped in parentheses
    expect(conditionExpression).toMatch(/\(.*AND.*\) OR/)
  })

  it('handles nested AND within OR with single condition', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: or(and({ name: equals('Test') }, { id: equals('123') }), {
        name: equals('Other'),
      }),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '(#name = :v1 AND #id = :v2) OR #name = :v3',
    })
  })

  it('handles mixed size and value expressions in comparison', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: and({ name: greaterThan(size('minLength')) }, { id: greaterThan(100) }),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    expect(dynamoMock.call(0).args[0].input).toMatchObject({
      ConditionExpression: '#name > size(#minLength) AND #id > :v1',
      ExpressionAttributeNames: {
        '#name': 'name',
        '#minLength': 'minLength',
        '#id': 'id',
      },
      ExpressionAttributeValues: {
        ':v1': 100,
      },
    })
  })

  it('handles condition template with mixed value types', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: {
        name: equals('Test'),
        id: greaterThan(100),
      },
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    const input = dynamoMock.call(0).args[0].input
    expect(input.ConditionExpression).toContain('#name = :v1')
    expect(input.ConditionExpression).toContain('#id > :v2')
    expect(input.ConditionExpression).toContain(' AND ')
  })

  it('handles value placeholder reuse in parseValueExpression', async () => {
    dynamoMock.on(DeleteCommand).resolves({})

    // Use the same value multiple times to test placeholder reuse
    const deleteCommand = new ConditionalDelete({
      key: {
        id: '123',
        timestamp: new Date('2000-01-01T00:00:00Z'),
      },
      condition: and({ name: greaterThan('Test') }, { id: greaterThan('Test') }),
    })

    await testEntity.send(deleteCommand)

    expect(dynamoMock.calls()).toHaveLength(1)
    const input = dynamoMock.call(0).args[0].input
    // Should reuse the same placeholder :v1 for both "Test" values
    expect(input.ConditionExpression).toBe('#name > :v1 AND #id > :v1')
    expect(input.ExpressionAttributeValues).toEqual({ ':v1': 'Test' })
  })
})
