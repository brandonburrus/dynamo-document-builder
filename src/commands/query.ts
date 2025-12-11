import type { ResponseMetadata } from '@aws-sdk/types'
import type { ConsumedCapacity, ReturnConsumedCapacity, Select } from '@aws-sdk/client-dynamodb'
import { QueryCommand, type NativeAttributeValue } from '@aws-sdk/lib-dynamodb'
import type { DynamoEntity, EntitySchema } from '@/core/entity'
import type { Condition } from '@/conditions/condition-types'
import type { ZodObject } from 'zod/v4'
import { EntityCommand } from '@/commands/base-entity-command'
import { AttributeExpressionMap } from '@/attributes'
import { parseCondition } from '@/conditions'
import pMap from 'p-map'

const QUERY_VALIDATION_CONCURRENCY = 64

export interface QueryConfig<Schema extends ZodObject> {
  keyCondition: Condition
  filter?: Condition
  projection?: string[] // TODO: Handle projection output validation
  select?: Select
  limit?: number
  consistentRead?: boolean
  skipValidation?: boolean
  validationConcurrency?: number
  queryIndex?: keyof EntitySchema<Schema> extends string ? keyof EntitySchema<Schema> : string
  reverseIndexScan?: boolean
  exclusiveStartKey?: Partial<EntitySchema<Schema>>
  returnConsumedCapacity?: ReturnConsumedCapacity
}

export interface QueryResult<Schema extends ZodObject> {
  items: EntitySchema<Schema>[]
  count: number
  scannedCount: number
  lastEvaluatedKey?: Partial<EntitySchema<Schema>> | undefined
  responseMetadata?: ResponseMetadata
  consumedCapacity?: ConsumedCapacity | undefined
}

export class Query<Schema extends ZodObject> extends EntityCommand<QueryResult<Schema>, Schema> {
  constructor(private config: QueryConfig<Schema>) {
    super()
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<QueryResult<Schema>> {
    const attributeExpressionMap = new AttributeExpressionMap()
    const keyConditionExpression = parseCondition(
      this.config.keyCondition,
      attributeExpressionMap,
    ).conditionExpression
    let filterExpression: string | undefined
    if (this.config.filter) {
      filterExpression = parseCondition(
        this.config.filter,
        attributeExpressionMap,
      ).conditionExpression
    }
    const query = new QueryCommand({
      TableName: entity.table.tableName,
      KeyConditionExpression: keyConditionExpression,
      FilterExpression: filterExpression,
      ...attributeExpressionMap.toDynamoAttributeExpression(),
      ProjectionExpression: this.config.projection ? this.config.projection.join(', ') : undefined,
      Select: this.config.select,
      Limit: this.config.limit,
      ConsistentRead: this.config.consistentRead ?? false,
      IndexName: this.config.queryIndex,
      ScanIndexForward: !this.config.reverseIndexScan,
      ExclusiveStartKey: this.config.exclusiveStartKey as
        | Record<string, NativeAttributeValue>
        | undefined,
      ReturnConsumedCapacity: this.config.returnConsumedCapacity,
    })

    const queryResult = await entity.table.documentClient.send(query)

    let items: EntitySchema<Schema>[]
    if (queryResult.Items) {
      if (this.config.skipValidation) {
        items = queryResult.Items as EntitySchema<Schema>[]
      } else {
        // TODO: Handle validating projections here
        // Or create separate ProjectedQuery command (also change PartialGet to ProjectedGet)?
        items = await pMap(queryResult.Items, async item => await entity.validateAsync(item), {
          concurrency: this.config.validationConcurrency ?? QUERY_VALIDATION_CONCURRENCY,
        })
      }
    } else {
      items = []
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
