import type { Condition } from '@/conditions/condition-types'
import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema } from '@/core/core-types'
import type { Select } from '@aws-sdk/client-dynamodb'
import type { ZodObject } from 'zod/v4'
import { AttributeExpressionMap } from '@/attributes/attribute-map'
import { QUERY_VALIDATION_CONCURRENCY } from '@/internal-constants'
import {
  QueryCommand,
  type QueryCommandInput,
  paginateQuery,
  type NativeAttributeValue,
  type QueryCommandOutput,
} from '@aws-sdk/lib-dynamodb'
import { parseCondition } from '@/conditions'
import type { BaseConfig, BaseCommand, BasePaginatable, BaseResult } from '@/commands/base-command'
import pMap from 'p-map'

export type QueryConfig<Schema extends ZodObject> = BaseConfig & {
  keyCondition: Condition
  filter?: Condition
  select?: Select
  limit?: number
  consistent?: boolean
  validationConcurrency?: number
  queryIndexName?: string
  reverseIndexScan?: boolean
  exclusiveStartKey?: Partial<EntitySchema<Schema>>
  pageSize?: number
}

export type QueryResult<Schema extends ZodObject> = BaseResult & {
  items: EntitySchema<Schema>[]
  count: number
  scannedCount: number
  lastEvaluatedKey?: Partial<EntitySchema<Schema>> | undefined
}

export class Query<Schema extends ZodObject>
  implements BaseCommand<QueryResult<Schema>, Schema>, BasePaginatable<QueryResult<Schema>, Schema>
{
  #config: QueryConfig<Schema>

  constructor(config: QueryConfig<Schema>) {
    this.#config = config
  }

  public buildCommandInput(entity: DynamoEntity<Schema>): QueryCommandInput {
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

    return {
      TableName: entity.table.tableName,
      KeyConditionExpression: keyConditionExpression,
      FilterExpression: filterExpression,
      ...attributeExpressionMap.toDynamoAttributeExpression(),
      Select: this.#config.select,
      Limit: this.#config.limit,
      ConsistentRead: this.#config.consistent ?? false,
      IndexName: this.#config.queryIndexName,
      ScanIndexForward: !this.#config.reverseIndexScan,
      ExclusiveStartKey: this.#config.exclusiveStartKey,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
    } satisfies QueryCommandInput
  }

  public async validateItems(
    entity: DynamoEntity<Schema>,
    items: Record<string, NativeAttributeValue>[] | undefined,
  ): Promise<EntitySchema<Schema>[]> {
    if (!items) {
      return []
    }
    if (this.#config.skipValidation) {
      return items as EntitySchema<Schema>[]
    }
    return pMap(items, item => entity.schema.parseAsync(item), {
      concurrency: this.#config.validationConcurrency ?? QUERY_VALIDATION_CONCURRENCY,
      signal: this.#config.abortController?.signal,
    })
  }

  public buildResult(
    items: EntitySchema<Schema>[],
    queryResult: QueryCommandOutput,
  ): QueryResult<Schema> {
    return {
      items,
      count: queryResult.Count ?? 0,
      scannedCount: queryResult.ScannedCount ?? 0,
      lastEvaluatedKey: queryResult.LastEvaluatedKey as Partial<EntitySchema<Schema>> | undefined,
      responseMetadata: queryResult.$metadata,
      consumedCapacity: queryResult.ConsumedCapacity,
    }
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<QueryResult<Schema>> {
    const query = new QueryCommand(this.buildCommandInput(entity))
    const queryResult = await entity.table.documentClient.send(query, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })
    const items = await this.validateItems(entity, queryResult.Items)
    return this.buildResult(items, queryResult)
  }

  public async *executePaginated(
    entity: DynamoEntity<Schema>,
  ): AsyncGenerator<QueryResult<Schema>, void, unknown> {
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
      const items = await this.validateItems(entity, page.Items)
      yield this.buildResult(items, page)
    }
  }
}
