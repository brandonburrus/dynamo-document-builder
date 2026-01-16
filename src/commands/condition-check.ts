import type { Condition } from '@/conditions'
import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema, TransactWriteOperation } from '@/core'
import type { ReturnValuesOnConditionCheckFailure } from '@aws-sdk/client-dynamodb'
import type { WriteTransactable } from '@/commands'
import type { ZodObject } from 'zod/v4'
import { parseCondition } from '@/conditions/condition-parser'

/**
 * Configuration for the ConditionCheck command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 */
export type ConditionCheckConfig<Schema extends ZodObject> = {
  key: Partial<EntitySchema<Schema>>
  condition: Condition
  returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure
}

/**
 * Command to verify a condition without modifying data in a transaction.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 *
 * @example
 * ```typescript
 * import { DynamoTable, DynamoEntity, key, TransactWrite, ConditionCheck, Update } from 'dynamo-document-builder';
 *
 * const table = new DynamoTable({
 *   tableName: 'ExampleTable',
 *   documentClient,
 * });
 *
 * const userEntity = new DynamoEntity({
 *   table,
 *   schema: z.object({
 *     userId: z.string(),
 *     balance: z.number(),
 *   }),
 *   partitionKey: user => key('USER', user.userId),
 *   sortKey: () => 'METADATA',
 * });
 *
 * const transactWriteCommand = new TransactWrite({
 *   writes: [
 *     new ConditionCheck({
 *       key: { userId: 'user1' },
 *       condition: { balance: greaterThan(100) },
 *     }),
 *     new Update({ key: { userId: 'user2' }, update: { balance: add(50) } }),
 *   ],
 * });
 *
 * await userEntity.send(transactWriteCommand);
 * ```
 */
export class ConditionCheck<Schema extends ZodObject> implements WriteTransactable<Schema> {
  #config: ConditionCheckConfig<Schema>

  constructor(config: ConditionCheckConfig<Schema>) {
    this.#config = config
  }

  public async prepareWriteTransaction(
    entity: DynamoEntity<Schema>,
  ): Promise<TransactWriteOperation> {
    const { conditionExpression, attributeExpressionMap } = parseCondition(this.#config.condition)
    return {
      ConditionCheck: {
        TableName: entity.table.tableName,
        Key: entity.buildPrimaryKey(this.#config.key),
        ConditionExpression: conditionExpression,
        ...attributeExpressionMap.toDynamoAttributeExpression(),
        ReturnValuesOnConditionCheckFailure: this.#config.returnValuesOnConditionCheckFailure,
      },
    }
  }
}
