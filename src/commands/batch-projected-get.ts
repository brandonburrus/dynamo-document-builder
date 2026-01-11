import type { BaseConfig, BaseCommand, BaseResult } from '@/commands/base-command'
import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema } from '@/core/core-types'
import type { Projection } from '@/projections/projection-types'
import type { ZodObject } from 'zod/v4'
import { BATCH_GET_VALIDATION_CONCURRENCY } from '@/internal-constants'
import { BatchGetCommand } from '@aws-sdk/lib-dynamodb'
import { parseProjection } from '@/projections/projection-parser'
import pMap from 'p-map'

export type BatchProjectedGetConfig<
  Schema extends ZodObject,
  ProjectionSchema extends ZodObject,
> = BaseConfig & {
  keys: Array<Partial<EntitySchema<Schema>>>
  consistent?: boolean
  projection: Projection
  projectionSchema: ProjectionSchema
}

export type BatchProjectedGetResult<ProjectionSchema extends ZodObject> = BaseResult & {
  items: Array<EntitySchema<ProjectionSchema>>
  unprocessedKeys?: Array<Partial<EntitySchema<ProjectionSchema>>>
}

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
