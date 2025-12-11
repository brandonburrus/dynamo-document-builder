import { describe, expect, it } from 'vitest'
import { EntityCommand } from '@/commands/base-entity-command'
import { DynamoEntity } from '@/core/entity'
import { DynamoTable } from '@/core/table'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import { z } from 'zod/v4'

describe('EntityCommand', () => {
  const dynamoClient = new DynamoDBClient()
  const testTable = new DynamoTable({
    tableName: 'TestTable',
    documentClient: DynamoDBDocumentClient.from(dynamoClient),
  })

  const testSchema = z.object({
    id: z.string(),
    name: z.string(),
  })

  const testEntity = new DynamoEntity({
    table: testTable,
    schema: testSchema,
    partitionKey: item => `TEST#${item.id}`,
    sortKey: item => `META#${item.id}`,
  })

  it('should be extendable as an abstract class', async () => {
    class TestCommand extends EntityCommand<string, typeof testSchema> {
      async execute(entity: DynamoEntity<typeof testSchema>): Promise<string> {
        return `Executed for table: ${entity.table.tableName}`
      }
    }

    const command = new TestCommand()
    const result = await command.execute(testEntity)
    expect(result).toBe('Executed for table: TestTable')
  })

  it('should allow different output types', async () => {
    class NumberCommand extends EntityCommand<number, typeof testSchema> {
      async execute(): Promise<number> {
        return 42
      }
    }

    const command = new NumberCommand()
    const result = await command.execute(testEntity)
    expect(result).toBe(42)
  })

  it('should allow complex return types', async () => {
    type ComplexOutput = {
      success: boolean
      data: string[]
    }

    class ComplexCommand extends EntityCommand<ComplexOutput, typeof testSchema> {
      async execute(): Promise<ComplexOutput> {
        return {
          success: true,
          data: ['item1', 'item2'],
        }
      }
    }

    const command = new ComplexCommand()
    const result = await command.execute(testEntity)
    expect(result).toEqual({
      success: true,
      data: ['item1', 'item2'],
    })
  })
})
