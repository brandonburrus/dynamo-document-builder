import type { Condition } from '@/conditions/condition-types'
import type { DeleteConfig } from '@/commands/delete'
import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema, TransactWriteOperation } from '@/core/core-types'
import type {
  ItemCollectionMetrics,
  ReturnValuesOnConditionCheckFailure,
} from '@aws-sdk/client-dynamodb'
import type { ZodObject } from 'zod/v4'
import { DeleteCommand } from '@aws-sdk/lib-dynamodb'
import { parseCondition } from '@/conditions/condition-parser'
import type { BaseResult, BaseCommand, WriteTransactable } from '@/commands/base-command'

export type ConditionalDeleteConfig<Schema extends ZodObject> = DeleteConfig<Schema> & {
  condition: Condition
  returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure
}

export type ConditionalDeleteResult<Schema extends ZodObject> = BaseResult & {
  deletedItem?: Partial<EntitySchema<Schema>> | undefined
  itemCollectionMetrics?: ItemCollectionMetrics
}

export class ConditionalDelete<Schema extends ZodObject>
  implements BaseCommand<ConditionalDeleteResult<Schema>, Schema>, WriteTransactable<Schema>
{
  #config: ConditionalDeleteConfig<Schema>

  constructor(config: ConditionalDeleteConfig<Schema>) {
    this.#config = config
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<ConditionalDeleteResult<Schema>> {
    const { conditionExpression, attributeExpressionMap } = parseCondition(this.#config.condition)

    const deleteCmd = new DeleteCommand({
      TableName: entity.table.tableName,
      Key: entity.buildPrimaryKey(this.#config.key),
      ConditionExpression: conditionExpression,
      ...attributeExpressionMap.toDynamoAttributeExpression(),
      ReturnValues: this.#config.returnValues,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
      ReturnItemCollectionMetrics: this.#config.returnItemCollectionMetrics,
      ReturnValuesOnConditionCheckFailure: this.#config.returnValuesOnConditionCheckFailure,
    })

    const deleteResult = await entity.table.documentClient.send(deleteCmd, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    let deletedItem: Partial<EntitySchema<Schema>> | undefined
    if (deleteResult.Attributes) {
      if (this.#config.skipValidation) {
        deletedItem = deleteResult.Attributes as Partial<EntitySchema<Schema>>
      } else {
        deletedItem = (await entity.schema
          .partial()
          .parseAsync(deleteResult.Attributes)) as Partial<EntitySchema<Schema>>
      }
    }

    return {
      deletedItem,
      responseMetadata: deleteResult.$metadata,
      consumedCapacity: deleteResult.ConsumedCapacity,
      itemCollectionMetrics: deleteResult.ItemCollectionMetrics,
    }
  }

  public async prepareWriteTransaction(
    entity: DynamoEntity<Schema>,
  ): Promise<TransactWriteOperation> {
    const { conditionExpression, attributeExpressionMap } = parseCondition(this.#config.condition)
    return {
      Delete: {
        TableName: entity.table.tableName,
        Key: entity.buildPrimaryKey(this.#config.key),
        ConditionExpression: conditionExpression,
        ...attributeExpressionMap.toDynamoAttributeExpression(),
        ReturnValuesOnConditionCheckFailure: this.#config.returnValuesOnConditionCheckFailure,
      },
    }
  }
}
