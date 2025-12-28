import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema } from '@/core/core-types'
import type { GetConfig } from '@/commands/get'
import type { Projection } from '@/projections/projection-types'
import type { ZodObject } from 'zod/v4'
import { GetCommand } from '@aws-sdk/lib-dynamodb'
import { parseProjection } from '@/projections/projection-parser'
import type { BaseResult, BaseCommand } from '@/commands/base-command'

export type ProjectedGetConfig<
  Schema extends ZodObject,
  ProjectionSchema extends ZodObject,
> = GetConfig<Schema> & {
  projection: Projection
  projectionSchema: ProjectionSchema
}

export type ProjectedGetResult<ProjectionSchema extends ZodObject> = BaseResult & {
  item: EntitySchema<ProjectionSchema> | undefined
}

export class ProjectedGet<Schema extends ZodObject, ProjectionSchema extends ZodObject>
  implements BaseCommand<ProjectedGetResult<ProjectionSchema>, Schema>
{
  #config: ProjectedGetConfig<Schema, ProjectionSchema>

  constructor(config: ProjectedGetConfig<Schema, ProjectionSchema>) {
    this.#config = config
  }

  public async execute(
    entity: DynamoEntity<Schema>,
  ): Promise<ProjectedGetResult<ProjectionSchema>> {
    const { projectionExpression, attributeExpressionMap } = parseProjection(
      this.#config.projection,
    )

    const getCmd = new GetCommand({
      TableName: entity.table.tableName,
      Key: entity.buildPrimaryOrIndexKey(this.#config),
      ProjectionExpression: projectionExpression,
      ExpressionAttributeNames: attributeExpressionMap.toDynamoAttributeNames(),
      ConsistentRead: this.#config.consistent ?? false,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
    })

    const getResult = await entity.table.documentClient.send(getCmd, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    if (getResult.Item) {
      const item = this.#config.skipValidation
        ? (getResult.Item as EntitySchema<ProjectionSchema>)
        : await this.#config.projectionSchema.parseAsync(getResult.Item)

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
