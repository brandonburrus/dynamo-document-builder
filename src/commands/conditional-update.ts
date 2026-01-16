import type { Condition } from '@/conditions'
import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema, TransactWriteOperation } from '@/core'
import type {
  ItemCollectionMetrics,
  ReturnValuesOnConditionCheckFailure,
} from '@aws-sdk/client-dynamodb'
import type { UpdateConfig } from '@/commands/update'
import type { ZodObject } from 'zod/v4'
import { AttributeExpressionMap } from '@/attributes'
import { UpdateCommand } from '@aws-sdk/lib-dynamodb'
import { parseCondition } from '@/conditions/condition-parser'
import { parseUpdate } from '@/updates/update-parser'
import type { BaseResult, BaseCommand, WriteTransactable } from '@/commands'

/**
 * Configuration for the ConditionalUpdate command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 */
export type ConditionalUpdateConfig<Schema extends ZodObject> = UpdateConfig<Schema> & {
  condition: Condition
  returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure
}

/**
 * Result of the ConditionalUpdate command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 */
export type ConditionalUpdateResult<Schema extends ZodObject> = BaseResult & {
  updatedItem?: EntitySchema<Schema> | undefined
  itemCollectionMetrics?: ItemCollectionMetrics
}

/**
 * Command to modify existing item attributes with a condition expression.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 *
 * @example
 * ```typescript
 * import { DynamoTable, DynamoEntity, key, ConditionalUpdate, add } from 'dynamo-document-builder';
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
 *     status: z.string(),
 *   }),
 *   partitionKey: user => key('USER', user.userId),
 *   sortKey: () => 'METADATA',
 * });
 *
 * const conditionalUpdateCommand = new ConditionalUpdate({
 *   key: { userId: 'user123' },
 *   update: { balance: add(50) },
 *   condition: { status: 'active' },
 *   returnValues: 'ALL_NEW',
 * });
 *
 * const { updatedItem } = await userEntity.send(conditionalUpdateCommand);
 * ```
 */
export class ConditionalUpdate<Schema extends ZodObject>
  implements BaseCommand<ConditionalUpdateResult<Schema>, Schema>, WriteTransactable<Schema>
{
  #config: ConditionalUpdateConfig<Schema>

  constructor(config: ConditionalUpdateConfig<Schema>) {
    this.#config = config
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<ConditionalUpdateResult<Schema>> {
    const attributeExpressionMap = new AttributeExpressionMap()

    const { updateExpression } = parseUpdate(this.#config.update, attributeExpressionMap)
    const { conditionExpression } = parseCondition(this.#config.condition, attributeExpressionMap)

    const updateItem = new UpdateCommand({
      TableName: entity.table.tableName,
      Key: entity.buildPrimaryKey(this.#config.key),
      UpdateExpression: updateExpression,
      ConditionExpression: conditionExpression,
      ...attributeExpressionMap.toDynamoAttributeExpression(),
      ReturnValues: this.#config.returnValues,
      ReturnItemCollectionMetrics: this.#config.returnItemCollectionMetrics,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
      ReturnValuesOnConditionCheckFailure: this.#config.returnValuesOnConditionCheckFailure,
    })

    const updateResult = await entity.table.documentClient.send(updateItem, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    let updatedItem: EntitySchema<Schema> | undefined
    if (updateResult.Attributes) {
      if (this.#config.skipValidation) {
        updatedItem = updateResult.Attributes as EntitySchema<Schema>
      } else {
        updatedItem = await entity.schema.parseAsync(updateResult.Attributes)
      }
    }

    return {
      updatedItem,
      responseMetadata: updateResult.$metadata,
      consumedCapacity: updateResult.ConsumedCapacity,
      itemCollectionMetrics: updateResult.ItemCollectionMetrics,
    }
  }

  public async prepareWriteTransaction(
    entity: DynamoEntity<Schema>,
  ): Promise<TransactWriteOperation> {
    const attributeExpressionMap = new AttributeExpressionMap()
    const { updateExpression } = parseUpdate(this.#config.update, attributeExpressionMap)
    const { conditionExpression } = parseCondition(this.#config.condition, attributeExpressionMap)
    return {
      Update: {
        TableName: entity.table.tableName,
        Key: entity.buildPrimaryKey(this.#config.key),
        UpdateExpression: updateExpression,
        ConditionExpression: conditionExpression,
        ...attributeExpressionMap.toDynamoAttributeExpression(),
        ReturnValuesOnConditionCheckFailure: this.#config.returnValuesOnConditionCheckFailure,
      },
    }
  }
}
