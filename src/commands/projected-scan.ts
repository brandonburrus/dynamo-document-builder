import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema } from '@/core/core-types'
import type { ScanConfig } from '@/commands/scan'
import type { ZodObject } from 'zod/v4'
import { AttributeExpressionMap } from '@/attributes/attribute-map'
import { EntityCommand, type BaseResult } from '@/commands/base-entity-command'
import { ScanCommand } from '@aws-sdk/lib-dynamodb'
import { parseCondition } from '@/conditions/condition-parser'
import { type Projection, parseProjection } from '@/projections/projection-parser'
import pMap from 'p-map'

const PROJECTED_SCAN_VALIDATION_CONCURRENCY = 64

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

export class ProjectedScan<
  Schema extends ZodObject,
  ProjectedSchema extends ZodObject,
> extends EntityCommand<ProjectedScanResult<Schema, ProjectedSchema>, Schema> {
  #config: ProjectedScanConfig<ProjectedSchema>

  constructor(config: ProjectedScanConfig<ProjectedSchema>) {
    super()
    this.#config = config
  }

  public async execute(
    entity: DynamoEntity<Schema>,
  ): Promise<ProjectedScanResult<Schema, ProjectedSchema>> {
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

    const scan = new ScanCommand({
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
    })

    const scanResult = await entity.table.documentClient.send(scan, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    let items: EntitySchema<ProjectedSchema>[] = []
    if (scanResult.Items) {
      if (this.#config.skipValidation) {
        items = scanResult.Items as EntitySchema<ProjectedSchema>[]
      } else {
        items = await pMap(
          scanResult.Items,
          item => this.#config.projectionSchema.parseAsync(item),
          {
            concurrency:
              this.#config.validationConcurrency ?? PROJECTED_SCAN_VALIDATION_CONCURRENCY,
          },
        )
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
