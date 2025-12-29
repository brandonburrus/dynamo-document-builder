import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema, TransactWriteOperation } from '@/core/core-types'
import type {
  ItemCollectionMetrics,
  ReturnItemCollectionMetrics,
  ReturnValue,
} from '@aws-sdk/client-dynamodb'
import type { ZodObject } from 'zod/v4'
import { DeleteCommand } from '@aws-sdk/lib-dynamodb'
import type {
  BaseConfig,
  BaseCommand,
  BaseResult,
  WriteTransactable,
} from '@/commands/base-command'

export type DeleteConfig<Schema extends ZodObject> = BaseConfig & {
  key: Partial<EntitySchema<Schema>>
  returnValues?: ReturnValue
  returnItemCollectionMetrics?: ReturnItemCollectionMetrics
}

export type DeleteResult<Schema extends ZodObject> = BaseResult & {
  deletedItem?: EntitySchema<Schema> | undefined
  itemCollectionMetrics?: ItemCollectionMetrics
}

export class Delete<Schema extends ZodObject>
  implements BaseCommand<DeleteResult<Schema>, Schema>, WriteTransactable<Schema>
{
  #config: DeleteConfig<Schema>

  constructor(config: DeleteConfig<Schema>) {
    this.#config = config
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<DeleteResult<Schema>> {
    const deleteCmd = new DeleteCommand({
      TableName: entity.table.tableName,
      Key: entity.buildPrimaryKey(this.#config.key),
      ReturnValues: this.#config.returnValues,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
      ReturnItemCollectionMetrics: this.#config.returnItemCollectionMetrics,
    })

    const deleteResult = await entity.table.documentClient.send(deleteCmd, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    let deletedItem: EntitySchema<Schema> | undefined
    if (deleteResult.Attributes) {
      if (this.#config.skipValidation) {
        deletedItem = deleteResult.Attributes as EntitySchema<Schema>
      } else {
        deletedItem = await entity.schema.parseAsync(deleteResult.Attributes)
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
    return {
      Delete: {
        TableName: entity.table.tableName,
        Key: entity.buildPrimaryKey(this.#config.key),
      },
    }
  }
}
