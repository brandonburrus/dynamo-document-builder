import { BatchWriteCommand } from '@aws-sdk/lib-dynamodb'
import type { DynamoEntity } from '@/core/entity'
import type { BaseConfig, BaseCommand, BaseResult } from '@/commands/base-command'
import type { ZodObject } from 'zod/v4'
import type { EntitySchema } from '@/core/core-types'
import pMap from 'p-map'
import { BATCH_WRITE_VALIDATION_CONCURRENCY } from '@/internal-constants'
import type { ItemCollectionMetrics, ReturnItemCollectionMetrics } from '@aws-sdk/client-dynamodb'

export type BatchWriteConfig<Schema extends ZodObject> = BaseConfig & {
  puts?: Array<EntitySchema<Schema>>
  deletes?: Array<Partial<EntitySchema<Schema>>>
  returnItemCollectionMetrics?: ReturnItemCollectionMetrics
}

export type BatchWriteResult<Schema extends ZodObject> = BaseResult & {
  unprocessedPuts?: Array<EntitySchema<Schema>>
  unprocessedDeletes?: Array<Partial<EntitySchema<Schema>>>
  itemColectionMetrics?: ItemCollectionMetrics
}

export class BatchWrite<Schema extends ZodObject>
  implements BaseCommand<BatchWriteResult<Schema>, Schema>
{
  #config: BatchWriteConfig<Schema>

  constructor(config: BatchWriteConfig<Schema>) {
    this.#config = config
  }

  public async buildPutRequests(entity: DynamoEntity<Schema>) {
    if (!this.#config.puts || this.#config.puts.length === 0) {
      return []
    }
    if (this.#config.skipValidation) {
      return this.#config.puts.map(item => ({
        PutRequest: {
          Item: {
            ...item,
            ...entity.buildAllKeys(item),
          },
        },
      }))
    }
    return pMap(
      this.#config.puts,
      async item => {
        const encodedData = await entity.schema.encodeAsync(item)
        return {
          PutRequest: {
            Item: {
              ...encodedData,
              ...entity.buildAllKeys(item),
            },
          },
        }
      },
      {
        concurrency: BATCH_WRITE_VALIDATION_CONCURRENCY,
        signal: this.#config.abortController?.signal,
      },
    )
  }

  public async buildDeleteRequests(entity: DynamoEntity<Schema>) {
    if (!this.#config.deletes || this.#config.deletes.length === 0) {
      return []
    }
    return this.#config.deletes.map(key => ({
      DeleteRequest: {
        Key: entity.buildPrimaryKey(key),
      },
    }))
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<BatchWriteResult<Schema>> {
    const [putRequests, deleteRequests] = await Promise.all([
      this.buildPutRequests(entity),
      this.buildDeleteRequests(entity),
    ])
    const batchWrite = new BatchWriteCommand({
      RequestItems: {
        [entity.table.tableName]: [...putRequests, ...deleteRequests],
      },
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
      ReturnItemCollectionMetrics: this.#config.returnItemCollectionMetrics,
    })
    const batchWriteResult = await entity.table.documentClient.send(batchWrite, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    const unprocessedItems = batchWriteResult.UnprocessedItems?.[entity.table.tableName] ?? []
    const unprocessedPuts: Array<EntitySchema<Schema>> = []
    const unprocessedDeletes: Array<Partial<EntitySchema<Schema>>> = []

    for (const unprocessedItem of unprocessedItems) {
      if (unprocessedItem.PutRequest) {
        unprocessedPuts.push(unprocessedItem.PutRequest.Item as EntitySchema<Schema>)
      } else if (unprocessedItem.DeleteRequest) {
        unprocessedDeletes.push(unprocessedItem.DeleteRequest.Key as Partial<EntitySchema<Schema>>)
      }
    }

    return {
      unprocessedPuts: unprocessedPuts.length > 0 ? unprocessedPuts : undefined,
      unprocessedDeletes: unprocessedDeletes.length > 0 ? unprocessedDeletes : undefined,
      responseMetadata: batchWriteResult.$metadata,
      consumedCapacity: batchWriteResult.ConsumedCapacity?.[0],
      itemColectionMetrics: batchWriteResult.ItemCollectionMetrics,
    }
  }
}
