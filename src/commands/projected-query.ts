import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema } from '@/core'
import type { Projection } from '@/projections'
import type { QueryConfig } from '@/commands/query'
import type { ZodObject } from 'zod/v4'
import { AttributeExpressionMap } from '@/attributes/attribute-map'
import { PROJECTED_QUERY_VALIDATION_CONCURRENCY } from '@/internal-constants'
import { parseCondition } from '@/conditions/condition-parser'
import { parseProjection } from '@/projections/projection-parser'
import type { BaseResult, BaseCommand, BasePaginatable } from '@/commands'
import { DocumentBuilderError } from '@/errors'
import {
  type NativeAttributeValue,
  QueryCommand,
  type QueryCommandInput,
  type QueryCommandOutput,
  paginateQuery,
} from '@aws-sdk/lib-dynamodb'
import pMap from 'p-map'

/**
 * Configuration for the ProjectedQuery command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 * @template ProjectionSchema - The Zod schema defining the structure of the projected attributes.
 */
export type ProjectedQueryConfig<
  Schema extends ZodObject,
  ProjectionSchema extends ZodObject,
> = QueryConfig<Schema> & {
  projection: Projection
  projectionSchema: ProjectionSchema
}

/**
 * Result of the ProjectedQuery command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 * @template ProjectionSchema - The Zod schema defining the structure of the projected attributes.
 */
export type ProjectedQueryResult<
  Schema extends ZodObject,
  ProjectionSchema extends ZodObject,
> = BaseResult & {
  items: EntitySchema<ProjectionSchema>[]
  count: number
  scannedCount: number
  lastEvaluatedKey?: Partial<EntitySchema<Schema>> | undefined
}

/**
 * Command to retrieve specific attributes of multiple items by partition key with optional sort key conditions.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 * @template ProjectionSchema - The Zod schema defining the structure of the projected attributes.
 *
 * @example
 * ```typescript
 * import { DynamoTable, DynamoEntity, key, ProjectedQuery, beginsWith } from 'dynamo-document-builder';
 *
 * const table = new DynamoTable({
 *   tableName: 'ExampleTable',
 *   documentClient,
 * });
 *
 * const todoEntity = new DynamoEntity({
 *   table,
 *   schema: z.object({
 *     userId: z.string(),
 *     todoId: z.string(),
 *     title: z.string(),
 *     description: z.string(),
 *     isComplete: z.boolean(),
 *   }),
 *   partitionKey: todo => key('USER', todo.userId),
 *   sortKey: todo => key('TODO', todo.todoId),
 * });
 *
 * const projectedQueryCommand = new ProjectedQuery({
 *   key: { userId: 'user123' },
 *   projection: ['title', 'isComplete'],
 *   projectionSchema: z.object({
 *     title: z.string(),
 *     isComplete: z.boolean(),
 *   }),
 *   limit: 10,
 * });
 *
 * const { items, count } = await todoEntity.send(projectedQueryCommand);
 * ```
 */
export class ProjectedQuery<Schema extends ZodObject, ProjectedSchema extends ZodObject>
  implements
    BaseCommand<ProjectedQueryResult<Schema, ProjectedSchema>, Schema>,
    BasePaginatable<ProjectedQueryResult<Schema, ProjectedSchema>, Schema>
{
  #config: ProjectedQueryConfig<Schema, ProjectedSchema>

  constructor(config: ProjectedQueryConfig<Schema, ProjectedSchema>) {
    this.#config = config
  }

  public buildCommandInput(entity: DynamoEntity<Schema>): QueryCommandInput {
    const attributeExpressionMap = new AttributeExpressionMap()

    // Generate the PK or GSIPK key
    const keyItem = entity.buildPrimaryOrIndexKey(this.#config)
    let queryKeyName: string
    if ('key' in this.#config) {
      queryKeyName = entity.table.partitionKeyName
    } else if ('index' in this.#config) {
      const indexes = Object.keys(this.#config.index)
      if (!indexes) {
        throw new DocumentBuilderError('No index specified in query configuration.')
      }
      const indexName = Object.keys(this.#config.index)[0]!
      queryKeyName = entity.table.globalSecondaryIndexKeyNames[indexName]!.partitionKey
    } else {
      throw new DocumentBuilderError("Either 'key' or 'index' must be specified for a query.")
    }
    const queryKeyValue: NativeAttributeValue = keyItem[queryKeyName!]

    const keyConditionExpression = parseCondition(
      {
        [queryKeyName!]: queryKeyValue,
        ...this.#config.sortKeyCondition,
      },
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

    return {
      TableName: entity.table.tableName,
      KeyConditionExpression: keyConditionExpression,
      FilterExpression: filterExpression,
      ProjectionExpression: projectionExpression,
      ...attributeExpressionMap.toDynamoAttributeExpression(),
      Select: this.#config.selectAttributes,
      Limit: this.#config.limit,
      ConsistentRead: this.#config.consistent ?? false,
      IndexName: 'index' in this.#config ? queryKeyName : undefined,
      ScanIndexForward: !this.#config.reverseIndexScan,
      ExclusiveStartKey: this.#config.exclusiveStartKey as
        | Record<string, NativeAttributeValue>
        | undefined,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
    } satisfies QueryCommandInput
  }

  public async validateItems(
    items: Record<string, NativeAttributeValue>[] | undefined,
  ): Promise<EntitySchema<ProjectedSchema>[]> {
    if (!items) {
      return []
    }

    if (this.#config.skipValidation) {
      return items as EntitySchema<ProjectedSchema>[]
    }

    return pMap(items, item => this.#config.projectionSchema.parseAsync(item), {
      concurrency: this.#config.validationConcurrency ?? PROJECTED_QUERY_VALIDATION_CONCURRENCY,
      signal: this.#config.abortController?.signal,
    })
  }

  public buildResult(
    items: EntitySchema<ProjectedSchema>[],
    queryResult: QueryCommandOutput,
  ): ProjectedQueryResult<Schema, ProjectedSchema> {
    return {
      items,
      count: queryResult.Count ?? 0,
      scannedCount: queryResult.ScannedCount ?? 0,
      lastEvaluatedKey: queryResult.LastEvaluatedKey as Partial<EntitySchema<Schema>> | undefined,
      responseMetadata: queryResult.$metadata,
      consumedCapacity: queryResult.ConsumedCapacity,
    }
  }

  public async execute(
    entity: DynamoEntity<Schema>,
  ): Promise<ProjectedQueryResult<Schema, ProjectedSchema>> {
    const query = new QueryCommand(this.buildCommandInput(entity))
    const queryResult = await entity.table.documentClient.send(query, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })
    const items = await this.validateItems(queryResult.Items)
    return this.buildResult(items, queryResult)
  }

  public async *executePaginated(
    entity: DynamoEntity<Schema>,
  ): AsyncGenerator<ProjectedQueryResult<Schema, ProjectedSchema>, void, unknown> {
    const paginator = paginateQuery(
      {
        client: entity.table.documentClient,
        pageSize: this.#config.pageSize,
      },
      this.buildCommandInput(entity),
      {
        abortSignal: this.#config.abortController?.signal,
        requestTimeout: this.#config.timeoutMs,
      },
    )

    for await (const page of paginator) {
      const items = await this.validateItems(page.Items)
      yield this.buildResult(items, page)
    }
  }
}
