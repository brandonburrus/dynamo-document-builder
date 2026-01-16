import { GetCommand } from '@aws-sdk/lib-dynamodb'
import type { DynamoEntity } from '@/core/entity'
import type { BaseConfig, BaseCommand, BaseResult } from '@/commands'
import type { ZodObject } from 'zod/v4'
import type { EntitySchema } from '@/core'

/**
 * Configuration for the Get command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 */
export type GetConfig<Schema extends ZodObject> = BaseConfig & {
  key: Partial<EntitySchema<Schema>>
  consistent?: boolean
}

/**
 * Result of the Get command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 */
export type GetResult<Schema extends ZodObject> = BaseResult & {
  item: EntitySchema<Schema> | undefined
}

/**
 * Command to retrieve a single item by primary key from a DynamoDB table.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 *
 * @example
 * ```typescript
 * import { DynamoTable, DynamoEntity, key, Get } from 'dynamo-document-builder';
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
 *     email: z.string(),
 *   }),
 *   partitionKey: user => key('USER', user.userId),
 *   sortKey: () => 'METADATA',
 * });
 *
 * const getCommand = new Get({
 *   key: { userId: 'user123' },
 *   consistent: true,
 * });
 *
 * const { item } = await userEntity.send(getCommand);
 * ```
 */
export class Get<Schema extends ZodObject> implements BaseCommand<GetResult<Schema>, Schema> {
  #config: GetConfig<Schema>

  constructor(config: GetConfig<Schema>) {
    this.#config = config
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<GetResult<Schema>> {
    const getItem = new GetCommand({
      TableName: entity.table.tableName,
      Key: entity.buildPrimaryKey(this.#config.key),
      ConsistentRead: this.#config.consistent ?? false,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
    })

    const getResult = await entity.table.documentClient.send(getItem, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    if (getResult.Item) {
      const item = this.#config.skipValidation
        ? (getResult.Item as EntitySchema<Schema>)
        : await entity.schema.parseAsync(getResult.Item)

      return {
        item,
        responseMetadata: getResult.$metadata,
        consumedCapacity: getResult.ConsumedCapacity,
      }
    }

    return {
      item: undefined,
      responseMetadata: getResult.$metadata,
      consumedCapacity: getResult.ConsumedCapacity,
    }
  }
}
