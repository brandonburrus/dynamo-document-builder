import type { BaseConfig, BaseCommand, BaseResult } from '@/commands'
import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema } from '@/core'
import type { ZodObject } from 'zod/v4'
import { BATCH_GET_VALIDATION_CONCURRENCY } from '@/internal-constants'
import { BatchGetCommand } from '@aws-sdk/lib-dynamodb'
import pMap from 'p-map'

/**
 * Configuration for the BatchGet command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 */
export type BatchGetConfig<Schema extends ZodObject> = BaseConfig & {
  keys: Array<Partial<EntitySchema<Schema>>>
  consistent?: boolean
}

/**
 * Result of the BatchGet command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 */
export type BatchGetResult<Schema extends ZodObject> = BaseResult & {
  items: Array<EntitySchema<Schema>>
  unprocessedKeys?: Array<Partial<EntitySchema<Schema>>>
}

/**
 * Command to perform a batch get operation on a DynamoDB table.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 *
 * @example
 * ```typescript
 * import { DynamoTable, DynamoEntity, key, BatchGet } from 'dynamo-document-builder';
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
 *     name: z.string(),
 *     age: z.number(),
 *   }),
 *   partitionKey: user => key('USER', user.userId),
 *   sortKey: () => 'USER',
 * });
 *
 * const batchGetCommand = new BatchGet({
 *   keys: [
 *     { userId: 'user1' },
 *     { userId: 'user2' },
 *   ],
 *   consistent: true,
 * });
 *
 * const { items } = await batchGetCommand.execute(userEntity);
 * ```
 */
export class BatchGet<Schema extends ZodObject>
  implements BaseCommand<BatchGetResult<Schema>, Schema>
{
  #config: BatchGetConfig<Schema>

  constructor(config: BatchGetConfig<Schema>) {
    this.#config = config
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<BatchGetResult<Schema>> {
    const batchGet = new BatchGetCommand({
      RequestItems: {
        [entity.table.tableName]: {
          Keys: this.#config.keys.map(key => entity.buildPrimaryKey(key)),
          ConsistentRead: this.#config.consistent ?? false,
        },
      },
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
    })

    const batchGetResult = await entity.table.documentClient.send(batchGet, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    const rawItems = batchGetResult.Responses?.[entity.table.tableName] ?? []
    let items: Array<EntitySchema<Schema>> = []

    if (rawItems.length > 0) {
      if (this.#config.skipValidation) {
        items = rawItems as Array<EntitySchema<Schema>>
      } else {
        items = await pMap(rawItems, item => entity.schema.parseAsync(item), {
          concurrency: BATCH_GET_VALIDATION_CONCURRENCY,
          signal: this.#config.abortController?.signal,
        })
      }
    }

    return {
      items,
      unprocessedKeys: batchGetResult.UnprocessedKeys?.[entity.table.tableName]?.Keys as
        | Array<Partial<EntitySchema<Schema>>>
        | undefined,
      responseMetadata: batchGetResult.$metadata,
      consumedCapacity: batchGetResult.ConsumedCapacity?.[0],
    }
  }
}
