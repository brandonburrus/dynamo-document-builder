import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema } from '@/core'
import type { GetConfig } from '@/commands/get'
import type { Projection } from '@/projections'
import type { ZodObject } from 'zod/v4'
import { GetCommand } from '@aws-sdk/lib-dynamodb'
import { parseProjection } from '@/projections/projection-parser'
import type { BaseResult, BaseCommand } from '@/commands'

/**
 * Configuration for the ProjectedGet command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 * @template ProjectionSchema - The Zod schema defining the structure of the projected attributes.
 */
export type ProjectedGetConfig<
  Schema extends ZodObject,
  ProjectionSchema extends ZodObject,
> = GetConfig<Schema> & {
  projection: Projection
  projectionSchema: ProjectionSchema
}

/**
 * Result of the ProjectedGet command.
 *
 * @template ProjectionSchema - The Zod schema defining the structure of the projected attributes.
 */
export type ProjectedGetResult<ProjectionSchema extends ZodObject> = BaseResult & {
  item: EntitySchema<ProjectionSchema> | undefined
}

/**
 * Command to retrieve specific attributes of a single item by primary key.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 * @template ProjectionSchema - The Zod schema defining the structure of the projected attributes.
 *
 * @example
 * ```typescript
 * import { DynamoTable, DynamoEntity, key, ProjectedGet } from 'dynamo-document-builder';
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
 * const projectedGetCommand = new ProjectedGet({
 *   key: { userId: 'user123' },
 *   projection: ['name', 'email'],
 *   projectionSchema: z.object({
 *     name: z.string(),
 *     email: z.string(),
 *   }),
 *   consistent: true,
 * });
 *
 * const { item } = await userEntity.send(projectedGetCommand);
 * ```
 */
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

    const getItem = new GetCommand({
      TableName: entity.table.tableName,
      Key: entity.buildPrimaryKey(this.#config.key),
      ProjectionExpression: projectionExpression,
      ExpressionAttributeNames: attributeExpressionMap.toDynamoAttributeNames(),
      ConsistentRead: this.#config.consistent ?? false,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
    })

    const getResult = await entity.table.documentClient.send(getItem, {
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
