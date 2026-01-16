import type { BaseConfig, BaseCommand, BaseResult } from '@/commands'
import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema } from '@/core'
import type { Projection } from '@/projections'
import type { ZodObject } from 'zod/v4'
import { BATCH_GET_VALIDATION_CONCURRENCY } from '@/internal-constants'
import { BatchGetCommand } from '@aws-sdk/lib-dynamodb'
import { parseProjection } from '@/projections/projection-parser'
import pMap from 'p-map'

/**
 * Configuration for the BatchProjectedGet command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 * @template ProjectionSchema - The Zod schema defining the structure of the projected attributes.
 */
export type BatchProjectedGetConfig<
  Schema extends ZodObject,
  ProjectionSchema extends ZodObject,
> = BaseConfig & {
  keys: Array<Partial<EntitySchema<Schema>>>
  consistent?: boolean
  projection: Projection
  projectionSchema: ProjectionSchema
}

/**
 * Result of the BatchProjectedGet command.
 *
 * @template ProjectionSchema - The Zod schema defining the structure of the projected attributes.
 */
export type BatchProjectedGetResult<ProjectionSchema extends ZodObject> = BaseResult & {
  items: Array<EntitySchema<ProjectionSchema>>
  unprocessedKeys?: Array<Partial<EntitySchema<ProjectionSchema>>>
}

/**
 * Command to retrieve specific attributes of multiple items by primary keys in a single operation.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 * @template ProjectionSchema - The Zod schema defining the structure of the projected attributes.
 *
 * @example
 * ```typescript
 * import { DynamoTable, DynamoEntity, key, BatchProjectedGet } from 'dynamo-document-builder';
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
 *     age: z.number(),
 *   }),
 *   partitionKey: user => key('USER', user.userId),
 *   sortKey: () => 'METADATA',
 * });
 *
 * const batchProjectedGetCommand = new BatchProjectedGet({
 *   keys: [
 *     { userId: 'user1' },
 *     { userId: 'user2' },
 *   ],
 *   projection: ['name', 'email'],
 *   projectionSchema: z.object({
 *     name: z.string(),
 *     email: z.string(),
 *   }),
 * });
 *
 * const { items } = await userEntity.send(batchProjectedGetCommand);
 * ```
 */
export class BatchProjectedGet<Schema extends ZodObject, ProjectionSchema extends ZodObject>
  implements BaseCommand<BatchProjectedGetResult<ProjectionSchema>, Schema>
{
  #config: BatchProjectedGetConfig<Schema, ProjectionSchema>

  constructor(config: BatchProjectedGetConfig<Schema, ProjectionSchema>) {
    this.#config = config
  }

  public async execute(
    entity: DynamoEntity<Schema>,
  ): Promise<BatchProjectedGetResult<ProjectionSchema>> {
    const { projectionExpression, attributeExpressionMap } = parseProjection(
      this.#config.projection,
    )

    const batchGet = new BatchGetCommand({
      RequestItems: {
        [entity.table.tableName]: {
          Keys: this.#config.keys.map(key => entity.buildPrimaryKey(key)),
          ConsistentRead: this.#config.consistent ?? false,
          ProjectionExpression: projectionExpression,
          ExpressionAttributeNames: attributeExpressionMap.toDynamoAttributeNames(),
        },
      },
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
    })

    const batchGetResult = await entity.table.documentClient.send(batchGet, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    const rawItems = batchGetResult.Responses?.[entity.table.tableName] ?? []
    let items: Array<EntitySchema<ProjectionSchema>> = []

    if (rawItems.length > 0) {
      if (this.#config.skipValidation) {
        items = rawItems as Array<EntitySchema<ProjectionSchema>>
      } else {
        items = await pMap(rawItems, item => this.#config.projectionSchema.parseAsync(item), {
          concurrency: BATCH_GET_VALIDATION_CONCURRENCY,
          signal: this.#config.abortController?.signal,
        })
      }
    }

    return {
      items,
      unprocessedKeys: batchGetResult.UnprocessedKeys?.[entity.table.tableName]?.Keys as
        | Array<Partial<EntitySchema<ProjectionSchema>>>
        | undefined,
      responseMetadata: batchGetResult.$metadata,
      consumedCapacity: batchGetResult.ConsumedCapacity?.[0],
    }
  }
}
