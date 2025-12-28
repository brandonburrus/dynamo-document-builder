import type { Condition } from '@/conditions/condition-types'
import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema } from '@/core/core-types'
import type { Select } from '@aws-sdk/client-dynamodb'
import type { ZodObject } from 'zod/v4'
import { AttributeExpressionMap } from '@/attributes/attribute-map'
import { EntityCommand, type BaseConfig, type BaseResult } from '@/commands/base-entity-command'
import { ScanCommand } from '@aws-sdk/lib-dynamodb'
import { parseCondition } from '@/conditions/condition-parser'
import pMap from 'p-map'

const SCAN_VALIDATION_CONCURRENCY = 64

export type ScanConfig<Schema extends ZodObject> = BaseConfig & {
  filter?: Condition
  select?: Select
  limit?: number
  consistent?: boolean
  validationConcurrency?: number
  scanIndexName?: string
  segment?: number
  totalSegments?: number
  exclusiveStartKey?: Partial<EntitySchema<Schema>>
}

export type ScanResult<Schema extends ZodObject> = BaseResult & {
  items: EntitySchema<Schema>[]
  count: number
  scannedCount: number
  lastEvaluatedKey?: Partial<EntitySchema<Schema>> | undefined
}

export class Scan<Schema extends ZodObject> extends EntityCommand<ScanResult<Schema>, Schema> {
  #config: ScanConfig<Schema>

  constructor(config: ScanConfig<Schema>) {
    super()
    this.#config = config
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<ScanResult<Schema>> {
    const attributeExpressionMap = new AttributeExpressionMap()

    let filterExpression: string | undefined
    if (this.#config.filter) {
      filterExpression = parseCondition(
        this.#config.filter,
        attributeExpressionMap,
      ).conditionExpression
    }

    const scan = new ScanCommand({
      TableName: entity.table.tableName,
      FilterExpression: filterExpression,
      ...attributeExpressionMap.toDynamoAttributeExpression(),
      Select: this.#config.select,
      Limit: this.#config.limit,
      ConsistentRead: this.#config.consistent ?? false,
      IndexName: this.#config.scanIndexName,
      Segment: this.#config.segment,
      TotalSegments: this.#config.totalSegments,
      ExclusiveStartKey: this.#config.exclusiveStartKey,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
    })

    const scanResult = await entity.table.documentClient.send(scan, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    let items: EntitySchema<Schema>[] = []
    if (scanResult.Items) {
      if (this.#config.skipValidation) {
        items = scanResult.Items as EntitySchema<Schema>[]
      } else {
        items = await pMap(scanResult.Items, item => entity.schema.parseAsync(item), {
          concurrency: this.#config.validationConcurrency ?? SCAN_VALIDATION_CONCURRENCY,
        })
      }
    }

    return {
      items,
      count: scanResult.Count ?? 0,
      scannedCount: scanResult.ScannedCount ?? 0,
      lastEvaluatedKey: scanResult.LastEvaluatedKey as Partial<EntitySchema<Schema>> | undefined,
      responseMetadata: scanResult.$metadata,
      consumedCapacity: scanResult.ConsumedCapacity,
    }
  }
}
