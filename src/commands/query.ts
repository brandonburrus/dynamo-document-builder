import type { Condition } from '@/conditions/condition-types'
import type { DynamoEntity, EntityKeyInput } from '@/core/entity'
import type { EntitySchema } from '@/core/core-types'
import type { Select } from '@aws-sdk/client-dynamodb'
import type { ZodObject } from 'zod/v4'
import type { BaseConfig, BaseCommand, BasePaginatable, BaseResult } from '@/commands/base-command'
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
import { DocumentBuilderError } from '@/errors'
import pMap from 'p-map'

export type QueryConfig<Schema extends ZodObject> = BaseConfig &
  EntityKeyInput<EntitySchema<Schema>> & {
    sortKeyCondition?: Condition
    filter?: Condition
    limit?: number
    selectAttributes?: Select
    consistent?: boolean
    validationConcurrency?: number
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

    // Generate the PK or GSIPK key
    const keyItem = entity.buildPrimaryOrIndexKey(this.#config)
    let queryKeyName: string
    let indexName: string | undefined
    if ('key' in this.#config) {
      queryKeyName = entity.table.partitionKeyName
    } else if ('index' in this.#config) {
      const indexes = Object.keys(this.#config.index)
      if (!indexes) {
        throw new DocumentBuilderError('No index specified in query configuration.')
      }
      indexName = Object.keys(this.#config.index)[0]!
      queryKeyName = entity.table.globalSecondaryIndexKeyNames[indexName]!.partitionKey
    } else {
      throw new DocumentBuilderError("Either 'key' or 'index' must be specified for a query.")
    }
    const queryKeyValue: NativeAttributeValue = keyItem[queryKeyName!]

    const keyConditionExpression = parseCondition(
      {
        [queryKeyName!]: queryKeyValue,
        ...(this.#config.sortKeyCondition ?? {}),
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

    return {
      TableName: entity.table.tableName,
      KeyConditionExpression: keyConditionExpression,
      FilterExpression: filterExpression,
      ...attributeExpressionMap.toDynamoAttributeExpression(),
      Select: this.#config.selectAttributes,
      Limit: this.#config.limit,
      ConsistentRead: this.#config.consistent ?? false,
      IndexName: indexName,
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
