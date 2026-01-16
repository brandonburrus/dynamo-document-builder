import { TransactGetCommand } from '@aws-sdk/lib-dynamodb'
import type { DynamoEntity } from '@/core/entity'
import type { BaseConfig, BaseCommand, BaseResult } from '@/commands'
import type { ZodObject } from 'zod/v4'
import type { EntitySchema } from '@/core'
import { TRANSACTION_GET_VALIDATION_CONCURRENCY } from '@/internal-constants'
import pMap from 'p-map'

/**
 * Configuration for the TransactGet command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 */
export type TransactGetConfig<Schema extends ZodObject> = BaseConfig & {
  keys: Array<Partial<EntitySchema<Schema>>>
}

/**
 * Result of the TransactGet command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 */
export type TransactGetResult<Schema extends ZodObject> = BaseResult & {
  items: Array<EntitySchema<Schema> | undefined>
}

/**
 * Command to perform a transactional read of multiple items (all-or-nothing, strongly consistent).
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 *
 * @example
 * ```typescript
 * import { DynamoTable, DynamoEntity, key, TransactGet } from 'dynamo-document-builder';
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
 *   }),
 *   partitionKey: user => key('USER', user.userId),
 *   sortKey: () => 'METADATA',
 * });
 *
 * const transactGetCommand = new TransactGet({
 *   keys: [
 *     { userId: 'user1' },
 *     { userId: 'user2' },
 *   ],
 * });
 *
 * const { items } = await userEntity.send(transactGetCommand);
 * // items array has same order as keys, undefined if not found
 * ```
 */
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
