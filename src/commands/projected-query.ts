import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema } from '@/core/core-types'
import type { Projection } from '@/projections/projection-types'
import type { QueryConfig } from '@/commands/query'
import type { ZodObject } from 'zod/v4'
import { AttributeExpressionMap } from '@/attributes/attribute-map'
import { PROJECTED_QUERY_VALIDATION_CONCURRENCY } from '@/internal-constants'
import { parseCondition } from '@/conditions/condition-parser'
import { parseProjection } from '@/projections/projection-parser'
import { type BaseResult, EntityCommand } from '@/commands/base-entity-command'
import { type NativeAttributeValue, QueryCommand } from '@aws-sdk/lib-dynamodb'
import pMap from 'p-map'

export type ProjectedQueryConfig<
  Schema extends ZodObject,
  ProjectionSchema extends ZodObject,
> = QueryConfig<Schema> & {
  projection: Projection
  projectionSchema: ProjectionSchema
}

export type ProjectedQueryResult<
  Schema extends ZodObject,
  ProjectionSchema extends ZodObject,
> = BaseResult & {
  items: EntitySchema<ProjectionSchema>[]
  count: number
  scannedCount: number
  lastEvaluatedKey?: Partial<EntitySchema<Schema>> | undefined
}

export class ProjectedQuery<
  Schema extends ZodObject,
  ProjectedSchema extends ZodObject,
> extends EntityCommand<ProjectedQueryResult<Schema, ProjectedSchema>, Schema> {
  #config: ProjectedQueryConfig<Schema, ProjectedSchema>

  constructor(config: ProjectedQueryConfig<Schema, ProjectedSchema>) {
    super()
    this.#config = config
  }

  public async execute(
    entity: DynamoEntity<Schema>,
  ): Promise<ProjectedQueryResult<Schema, ProjectedSchema>> {
    const attributeExpressionMap = new AttributeExpressionMap()

    const keyConditionExpression = parseCondition(
      this.#config.keyCondition,
      attributeExpressionMap,
    ).conditionExpression

    let filterExpression: string | undefined
    if (this.#config.filter) {
      filterExpression = parseCondition(
        this.#config.filter,
        attributeExpressionMap,
      ).conditionExpression
    }

    const { projectionExpression } = parseProjection(
      this.#config.projection,
      attributeExpressionMap,
    )

    const query = new QueryCommand({
      TableName: entity.table.tableName,
      KeyConditionExpression: keyConditionExpression,
      FilterExpression: filterExpression,
      ProjectionExpression: projectionExpression,
      ...attributeExpressionMap.toDynamoAttributeExpression(),
      Select: this.#config.select,
      Limit: this.#config.limit,
      ConsistentRead: this.#config.consistent ?? false,
      IndexName: this.#config.queryIndexName,
      ScanIndexForward: !this.#config.reverseIndexScan,
      ExclusiveStartKey: this.#config.exclusiveStartKey as
        | Record<string, NativeAttributeValue>
        | undefined,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
    })

    const queryResult = await entity.table.documentClient.send(query, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    let items: EntitySchema<ProjectedSchema>[] = []
    if (queryResult.Items) {
      if (this.#config.skipValidation) {
        items = queryResult.Items as EntitySchema<ProjectedSchema>[]
      } else {
        items = await pMap(
          queryResult.Items,
          item => this.#config.projectionSchema.parseAsync(item),
          {
            concurrency:
              this.#config.validationConcurrency ?? PROJECTED_QUERY_VALIDATION_CONCURRENCY,
          },
        )
      }
    }

    return {
      items,
      count: queryResult.Count ?? 0,
      scannedCount: queryResult.ScannedCount ?? 0,
      lastEvaluatedKey: queryResult.LastEvaluatedKey as Partial<EntitySchema<Schema>> | undefined,
      responseMetadata: queryResult.$metadata,
      consumedCapacity: queryResult.ConsumedCapacity,
    }
  }
}
