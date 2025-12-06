import { describe, expect, it } from 'vitest'
import { z } from 'zod/v4'
import { DynamoEntity } from '@/core/entity'
import { DynamoTable } from '@/core/table'
import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'

describe('DynamoEntity', () => {
  it('should be defined', () => {
    const table = new DynamoTable({
      tableName: 'TestTable',
      documentClient: {} as DynamoDBDocumentClient,
    })

    const entity = new DynamoEntity({
      table,
      schema: z.object({
        id: z.string(),
        hello: z.string(),
      }),
      partitionKey: item => `TestEntity#${item.id}`,
      sortKey: item => `META#${item.id}`,
    })

    expect(entity).toBeDefined()
  })
})
