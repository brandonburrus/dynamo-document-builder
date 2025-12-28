import type { Condition } from '@/conditions/condition-types'
import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema } from '@/core/core-types'
import type { Select } from '@aws-sdk/client-dynamodb'
import type { ZodObject } from 'zod/v4'
import { AttributeExpressionMap } from '@/attributes/attribute-map'
import { QUERY_VALIDATION_CONCURRENCY } from '@/internal-constants'
import { QueryCommand } from '@aws-sdk/lib-dynamodb'
import { parseCondition } from '@/conditions'
import { type BaseConfig, EntityCommand, type BaseResult } from '@/commands/base-entity-command'
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
}

export type QueryResult<Schema extends ZodObject> = BaseResult & {
  items: EntitySchema<Schema>[]
  count: number
  scannedCount: number
  lastEvaluatedKey?: Partial<EntitySchema<Schema>> | undefined
}

export class Query<Schema extends ZodObject> extends EntityCommand<QueryResult<Schema>, Schema> {
  #config: QueryConfig<Schema>

  constructor(config: QueryConfig<Schema>) {
    super()
    this.#config = config
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<QueryResult<Schema>> {
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

    const query = new QueryCommand({
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
    })

    const queryResult = await entity.table.documentClient.send(query, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    let items: EntitySchema<Schema>[] = []
    if (queryResult.Items) {
      if (this.#config.skipValidation) {
        items = queryResult.Items as EntitySchema<Schema>[]
      } else {
        items = await pMap(queryResult.Items, item => entity.schema.parseAsync(item), {
          concurrency: this.#config.validationConcurrency ?? QUERY_VALIDATION_CONCURRENCY,
        })
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
