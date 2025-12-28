import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema } from '@/core/core-types'
import type { ScanConfig } from '@/commands/scan'
import type { Projection } from '@/projections/projection-types'
import type { ZodObject } from 'zod/v4'
import { AttributeExpressionMap } from '@/attributes/attribute-map'
import type { BaseCommand, BasePaginatable, BaseResult } from '@/commands/base-command'
import { PROJECTED_SCAN_VALIDATION_CONCURRENCY } from '@/internal-constants'
import {
  type NativeAttributeValue,
  ScanCommand,
  type ScanCommandInput,
  type ScanCommandOutput,
  paginateScan,
} from '@aws-sdk/lib-dynamodb'
import { parseCondition } from '@/conditions/condition-parser'
import { parseProjection } from '@/projections/projection-parser'
import pMap from 'p-map'

export type ProjectedScanConfig<ProjectionSchema extends ZodObject> =
  ScanConfig<ProjectionSchema> & {
    projection: Projection
    projectionSchema: ProjectionSchema
  }

export type ProjectedScanResult<
  Schema extends ZodObject,
  ProjectionSchema extends ZodObject,
> = BaseResult & {
  items: EntitySchema<ProjectionSchema>[]
  count: number
  scannedCount: number
  lastEvaluatedKey?: Partial<EntitySchema<Schema>> | undefined
}

export class ProjectedScan<Schema extends ZodObject, ProjectedSchema extends ZodObject>
  implements 
    BaseCommand<ProjectedScanResult<Schema, ProjectedSchema>, Schema>,
    BasePaginatable<ProjectedScanResult<Schema, ProjectedSchema>, Schema>
{
  #config: ProjectedScanConfig<ProjectedSchema>

  constructor(config: ProjectedScanConfig<ProjectedSchema>) {
    this.#config = config
  }

  public buildCommandInput(
    entity: DynamoEntity<Schema>,
  ): ScanCommandInput {
    const attributeExpressionMap = new AttributeExpressionMap()

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
      FilterExpression: filterExpression,
      ...attributeExpressionMap.toDynamoAttributeExpression(),
      ProjectionExpression: projectionExpression,
      Select: this.#config.select,
      Limit: this.#config.limit,
      ConsistentRead: this.#config.consistent ?? false,
      IndexName: this.#config.scanIndexName,
      Segment: this.#config.segment,
      TotalSegments: this.#config.totalSegments,
      ExclusiveStartKey: this.#config.exclusiveStartKey,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
}      satisfies ScanCommandInput
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
    return pMap(
      items,
      item => this.#config.projectionSchema.parseAsync(item),
      {
        concurrency:
          this.#config.validationConcurrency ?? PROJECTED_SCAN_VALIDATION_CONCURRENCY,
        signal: this.#config.abortController?.signal,
      },
    )
  }

  public buildResult(
    items: EntitySchema<ProjectedSchema>[],
    scanResult: ScanCommandOutput,
  ): ProjectedScanResult<Schema, ProjectedSchema> {
    return {
      items,
      count: scanResult.Count ?? 0,
      scannedCount: scanResult.ScannedCount ?? 0,
      lastEvaluatedKey: scanResult.LastEvaluatedKey as
        | Partial<EntitySchema<Schema>>
        | undefined,
      responseMetadata: scanResult.$metadata,
      consumedCapacity: scanResult.ConsumedCapacity,
    }
  }

  public async execute(
    entity: DynamoEntity<Schema>,
  ): Promise<ProjectedScanResult<Schema, ProjectedSchema>> {
    const scan = new ScanCommand(this.buildCommandInput(entity))
    const scanResult = await entity.table.documentClient.send(scan, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })
    const items = await this.validateItems(scanResult.Items)
    return this.buildResult(items, scanResult)
  }

  public async *executePaginated(
    entity: DynamoEntity<Schema>,
  ): AsyncGenerator<ProjectedScanResult<Schema, ProjectedSchema>, void, unknown> {
    const paginator = paginateScan(
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
