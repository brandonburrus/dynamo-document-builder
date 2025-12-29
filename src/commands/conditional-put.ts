import type { Condition } from '@/conditions/condition-types'
import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema, TransactWriteOperation } from '@/core/core-types'
import type {
  ItemCollectionMetrics,
  ReturnValuesOnConditionCheckFailure,
} from '@aws-sdk/client-dynamodb'
import type { PutConfig } from '@/commands/put'
import type { ZodObject } from 'zod/v4'
import { PutCommand } from '@aws-sdk/lib-dynamodb'
import { parseCondition } from '@/conditions/condition-parser'
import type { BaseResult, BaseCommand, WriteTransactable } from '@/commands/base-command'

export type ConditionalPutConfig<Schema extends ZodObject> = PutConfig<Schema> & {
  condition: Condition
  returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure
}

export type ConditionalPutResult<Schema extends ZodObject> = BaseResult & {
  returnItem: EntitySchema<Schema> | undefined
  itemCollectionMetrics: ItemCollectionMetrics | undefined
}

export class ConditionalPut<Schema extends ZodObject>
  implements BaseCommand<ConditionalPutResult<Schema>, Schema>, WriteTransactable<Schema>
{
  #config: ConditionalPutConfig<Schema>

  constructor(config: ConditionalPutConfig<Schema>) {
    this.#config = config
  }

  public async buildItem(entity: DynamoEntity<Schema>) {
    const encodedData = this.#config.skipValidation
      ? this.#config.item
      : await entity.schema.encodeAsync(this.#config.item)

    return {
      ...encodedData,
      ...entity.buildAllKeys(this.#config.item),
    }
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<ConditionalPutResult<Schema>> {
    const item = await this.buildItem(entity)
    const { conditionExpression, attributeExpressionMap } = parseCondition(this.#config.condition)

    const put = new PutCommand({
      TableName: entity.table.tableName,
      Item: item,
      ConditionExpression: conditionExpression,
      ...attributeExpressionMap.toDynamoAttributeExpression(),
      ReturnValues: this.#config.returnValues,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
      ReturnItemCollectionMetrics: this.#config.returnItemCollectionMetrics,
      ReturnValuesOnConditionCheckFailure: this.#config.returnValuesOnConditionCheckFailure,
    })

    const putResult = await entity.table.documentClient.send(put, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    if (putResult.Attributes) {
      const returnItem: EntitySchema<Schema> = this.#config.skipValidation
        ? (putResult.Attributes as EntitySchema<Schema>)
        : await entity.schema.parseAsync(putResult.Attributes)

      return {
        returnItem,
        responseMetadata: putResult.$metadata,
        consumedCapacity: putResult.ConsumedCapacity,
        itemCollectionMetrics: putResult.ItemCollectionMetrics,
      }
    }

    return {
      returnItem: undefined,
      responseMetadata: putResult.$metadata,
      consumedCapacity: putResult.ConsumedCapacity,
      itemCollectionMetrics: putResult.ItemCollectionMetrics,
    }
  }

  public async prepareWriteTransaction(
    entity: DynamoEntity<Schema>,
  ): Promise<TransactWriteOperation> {
    const item = await this.buildItem(entity)
    const { conditionExpression, attributeExpressionMap } = parseCondition(this.#config.condition)
    return {
      Put: {
        TableName: entity.table.tableName,
        Item: item,
        ConditionExpression: conditionExpression,
        ...attributeExpressionMap.toDynamoAttributeExpression(),
        ReturnValuesOnConditionCheckFailure: this.#config.returnValuesOnConditionCheckFailure,
      },
    }
  }
}
