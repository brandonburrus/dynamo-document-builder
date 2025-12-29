import { TransactGetCommand } from '@aws-sdk/lib-dynamodb'
import type { DynamoEntity } from '@/core/entity'
import type { BaseConfig, BaseCommand, BaseResult } from '@/commands/base-command'
import type { ZodObject } from 'zod/v4'
import type { EntitySchema } from '@/core/core-types'
import { TRANSACTION_GET_VALIDATION_CONCURRENCY } from '@/internal-constants'
import pMap from 'p-map'

export type TransactGetConfig<Schema extends ZodObject> = BaseConfig & {
  keys: Array<Partial<EntitySchema<Schema>>>
}

export type TransactGetResult<Schema extends ZodObject> = BaseResult & {
  items: Array<EntitySchema<Schema> | undefined>
}

export class TransactGet<Schema extends ZodObject>
  implements BaseCommand<TransactGetResult<Schema>, Schema>
{
  #config: TransactGetConfig<Schema>

  constructor(config: TransactGetConfig<Schema>) {
    this.#config = config
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<TransactGetResult<Schema>> {
    const transactItems = this.#config.keys.map(key => ({
      Get: {
        TableName: entity.table.tableName,
        Key: entity.buildPrimaryKey(key),
      },
    }))

    const transactGet = new TransactGetCommand({
      TransactItems: transactItems,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
    })

    const transactGetResult = await entity.table.documentClient.send(transactGet, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    const rawItems = transactGetResult.Responses ?? []
    let items: Array<EntitySchema<Schema> | undefined> = []
    if (rawItems.length > 0) {
      if (this.#config.skipValidation) {
        items = rawItems.map(response => response.Item as EntitySchema<Schema> | undefined)
      } else {
        items = await pMap(
          rawItems,
          async response => {
            if (response.Item) {
              return entity.schema.parseAsync(response.Item)
            }
            return undefined
          },
          {
            concurrency: TRANSACTION_GET_VALIDATION_CONCURRENCY,
            signal: this.#config.abortController?.signal,
          },
        )
      }
    }

    return {
      items,
      responseMetadata: transactGetResult.$metadata,
      consumedCapacity: transactGetResult.ConsumedCapacity?.[0],
    }
  }
}
