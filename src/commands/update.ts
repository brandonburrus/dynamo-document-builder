import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema, TransactWriteOperation } from '@/core/core-types'
import type {
  ItemCollectionMetrics,
  ReturnItemCollectionMetrics,
  ReturnValue,
} from '@aws-sdk/client-dynamodb'
import type { UpdateValues } from '@/updates/update-types'
import type { ZodObject } from 'zod/v4'
import { UpdateCommand } from '@aws-sdk/lib-dynamodb'
import { parseUpdate } from '@/updates/update-parser'
import type {
  BaseConfig,
  BaseCommand,
  BaseResult,
  WriteTransactable,
} from '@/commands/base-command'

export type UpdateConfig<Schema extends ZodObject> = BaseConfig & {
  key: Partial<EntitySchema<Schema>>
  update: UpdateValues
  returnValues?: ReturnValue
  returnItemCollectionMetrics?: ReturnItemCollectionMetrics
}

export type UpdateResult<Schema extends ZodObject> = BaseResult & {
  updatedItem?: Partial<EntitySchema<Schema>> | undefined
  itemCollectionMetrics?: ItemCollectionMetrics
}

export class Update<Schema extends ZodObject>
  implements BaseCommand<UpdateResult<Schema>, Schema>, WriteTransactable<Schema>
{
  #config: UpdateConfig<Schema>

  constructor(config: UpdateConfig<Schema>) {
    this.#config = config
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<UpdateResult<Schema>> {
    const { updateExpression, attributeExpressionMap } = parseUpdate(this.#config.update)

    const updateItem = new UpdateCommand({
      TableName: entity.table.tableName,
      Key: entity.buildPrimaryKey(this.#config.key),
      UpdateExpression: updateExpression,
      ...attributeExpressionMap.toDynamoAttributeExpression(),
      ReturnValues: this.#config.returnValues,
      ReturnItemCollectionMetrics: this.#config.returnItemCollectionMetrics,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
    })

    const updateResult = await entity.table.documentClient.send(updateItem, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    let updatedItem: Partial<EntitySchema<Schema>> | undefined
    if (updateResult.Attributes) {
      if (this.#config.skipValidation) {
        updatedItem = updateResult.Attributes as Partial<EntitySchema<Schema>>
      } else {
        updatedItem = (await entity.schema
          .partial()
          .parseAsync(updateResult.Attributes)) as Partial<EntitySchema<Schema>>
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
    const { updateExpression, attributeExpressionMap } = parseUpdate(this.#config.update)
    return {
      Update: {
        TableName: entity.table.tableName,
        Key: entity.buildPrimaryKey(this.#config.key),
        UpdateExpression: updateExpression,
        ...attributeExpressionMap.toDynamoAttributeExpression(),
      },
    }
  }
}
