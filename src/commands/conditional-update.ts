import type { Condition } from '@/conditions/condition-types'
import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema } from '@/core/core-types'
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
import { type BaseResult, EntityCommand } from '@/commands/base-entity-command'

export type ConditionalUpdateConfig<Schema extends ZodObject> = UpdateConfig<Schema> & {
  condition: Condition
  returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure
}

export type ConditionalUpdateResult<Schema extends ZodObject> = BaseResult & {
  updatedItem?: EntitySchema<Schema> | undefined
  itemCollectionMetrics?: ItemCollectionMetrics
}

export class ConditionalUpdate<Schema extends ZodObject> extends EntityCommand<
  ConditionalUpdateResult<Schema>,
  Schema
> {
  #config: ConditionalUpdateConfig<Schema>

  constructor(config: ConditionalUpdateConfig<Schema>) {
    super()
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
}
