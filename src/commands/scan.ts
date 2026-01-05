import type { Condition } from '@/conditions/condition-types'
import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema } from '@/core/core-types'
import type { Select } from '@aws-sdk/client-dynamodb'
import type { ZodObject } from 'zod/v4'
import { AttributeExpressionMap } from '@/attributes/attribute-map'
import type { BaseCommand, BaseConfig, BasePaginatable, BaseResult } from '@/commands/base-command'
import { SCAN_VALIDATION_CONCURRENCY } from '@/internal-constants'
import {
  type NativeAttributeValue,
  ScanCommand,
  type ScanCommandInput,
  type ScanCommandOutput,
  paginateScan,
} from '@aws-sdk/lib-dynamodb'
import { parseCondition } from '@/conditions/condition-parser'
import pMap from 'p-map'

export type ScanConfig<Schema extends ZodObject> = BaseConfig & {
  indexName?: string
  filter?: Condition
  limit?: number
  selectAttributes?: Select
  consistent?: boolean
  validationConcurrency?: number
  segment?: number
  totalSegments?: number
  exclusiveStartKey?: Partial<EntitySchema<Schema>>
  pageSize?: number
}

export type ScanResult<Schema extends ZodObject> = BaseResult & {
  items: EntitySchema<Schema>[]
  count: number
  scannedCount: number
  lastEvaluatedKey?: Partial<EntitySchema<Schema>> | undefined
}

export class Scan<Schema extends ZodObject>
  implements BaseCommand<ScanResult<Schema>, Schema>, BasePaginatable<ScanResult<Schema>, Schema>
{
  #config: ScanConfig<Schema> | undefined

  constructor(config?: ScanConfig<Schema>) {
    this.#config = config
  }

  public buildCommandInput(entity: DynamoEntity<Schema>): ScanCommandInput {
    const attributeExpressionMap = new AttributeExpressionMap()

    let filterExpression: string | undefined
    if (this.#config?.filter) {
      filterExpression = parseCondition(
        this.#config.filter,
        attributeExpressionMap,
      ).conditionExpression
    }

    return {
      TableName: entity.table.tableName,
      FilterExpression: filterExpression,
      ...attributeExpressionMap.toDynamoAttributeExpression(),
      Select: this.#config?.selectAttributes,
      Limit: this.#config?.limit,
      ConsistentRead: this.#config?.consistent ?? false,
      IndexName: this.#config?.indexName,
      Segment: this.#config?.segment,
      TotalSegments: this.#config?.totalSegments,
      ExclusiveStartKey: this.#config?.exclusiveStartKey,
      ReturnConsumedCapacity: this.#config?.returnConsumedCapacity,
    } satisfies ScanCommandInput
  }

  public async validateItems(
    entity: DynamoEntity<Schema>,
    items: Record<string, NativeAttributeValue>[] | undefined,
  ): Promise<EntitySchema<Schema>[]> {
    if (!items) {
      return []
    }
    if (this.#config?.skipValidation) {
      return items as EntitySchema<Schema>[]
    }
    return pMap(items, item => entity.schema.parseAsync(item), {
      concurrency: this.#config?.validationConcurrency ?? SCAN_VALIDATION_CONCURRENCY,
      signal: this.#config?.abortController?.signal,
    })
  }

  public buildResult(
    items: EntitySchema<Schema>[],
    scanResult: ScanCommandOutput,
  ): ScanResult<Schema> {
    return {
      items,
      count: scanResult.Count ?? 0,
      scannedCount: scanResult.ScannedCount ?? 0,
      lastEvaluatedKey: scanResult.LastEvaluatedKey as Partial<EntitySchema<Schema>> | undefined,
      responseMetadata: scanResult.$metadata,
      consumedCapacity: scanResult.ConsumedCapacity,
    }
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<ScanResult<Schema>> {
    const scan = new ScanCommand(this.buildCommandInput(entity))
    const scanResult = await entity.table.documentClient.send(scan, {
      abortSignal: this.#config?.abortController?.signal,
      requestTimeout: this.#config?.timeoutMs,
    })
    const items = await this.validateItems(entity, scanResult.Items)
    return this.buildResult(items, scanResult)
  }

  public async *executePaginated(
    entity: DynamoEntity<Schema>,
  ): AsyncGenerator<ScanResult<Schema>, void, unknown> {
    const paginator = paginateScan(
      {
        client: entity.table.documentClient,
        pageSize: this.#config?.pageSize,
      },
      this.buildCommandInput(entity),
      {
        abortSignal: this.#config?.abortController?.signal,
        requestTimeout: this.#config?.timeoutMs,
      },
    )

    for await (const page of paginator) {
      const items = await this.validateItems(entity, page.Items)
      yield this.buildResult(items, page)
    }
  }
}
