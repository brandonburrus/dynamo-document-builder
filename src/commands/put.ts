import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema, TransactWriteOperation } from '@/core/core-types'
import type {
  ItemCollectionMetrics,
  ReturnItemCollectionMetrics,
  ReturnValue,
} from '@aws-sdk/client-dynamodb'
import type { ZodObject } from 'zod/v4'
import { PutCommand } from '@aws-sdk/lib-dynamodb'
import type {
  BaseConfig,
  BaseCommand,
  BaseResult,
  WriteTransactable,
} from '@/commands/base-command'

export type PutConfig<Schema extends ZodObject> = BaseConfig & {
  item: EntitySchema<Schema>
  returnValues?: ReturnValue
  returnItemCollectionMetrics?: ReturnItemCollectionMetrics
}

export type PutResult<Schema extends ZodObject> = BaseResult & {
  returnItem: EntitySchema<Schema> | undefined
  itemCollectionMetrics?: ItemCollectionMetrics
}

export class Put<Schema extends ZodObject>
  implements BaseCommand<PutResult<Schema>, Schema>, WriteTransactable<Schema>
{
  #config: PutConfig<Schema>

  constructor(config: PutConfig<Schema>) {
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

  public async execute(entity: DynamoEntity<Schema>): Promise<PutResult<Schema>> {
    const item = await this.buildItem(entity)
    const put = new PutCommand({
      TableName: entity.table.tableName,
      Item: item,
      ReturnValues: this.#config.returnValues,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
      ReturnItemCollectionMetrics: this.#config.returnItemCollectionMetrics,
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
    return {
      Put: {
        TableName: entity.table.tableName,
        Item: item,
      },
    }
  }
}
