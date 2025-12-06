import type { ConsumedCapacity } from '@aws-sdk/client-dynamodb'
import { GetCommand } from '@aws-sdk/lib-dynamodb'
import type { ResponseMetadata } from '@aws-sdk/types'
import type { DynamoEntity, EntitySchema } from '@/core/entity'
import { EntityCommand } from '@/commands/base-entity-command'
import type { ZodObject } from 'zod/v4'

export interface PartialGetConfig<Schema extends ZodObject> {
  key: Partial<EntitySchema<Schema>>
  projection: string[]
  consistentRead?: boolean
  skipValidation?: boolean
}

export interface PartialGetResult<Schema extends ZodObject> {
  item: Partial<EntitySchema<Schema>> | undefined
  responseMetadata?: ResponseMetadata
  consumedCapacity?: ConsumedCapacity | undefined
}

export class PartialGet<Schema extends ZodObject> extends EntityCommand<
  PartialGetResult<Schema>,
  Schema
> {
  constructor(private config: PartialGetConfig<Schema>) {
    super()
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<PartialGetResult<Schema>> {
    if (!this.config.projection || this.config.projection.length === 0) {
      throw new Error('PartialGet requires at least one field in the projection parameter')
    }

    const getItem = new GetCommand({
      TableName: entity.table.tableName,
      Key: entity.buildPrimaryKey(this.config.key),
      ConsistentRead: this.config.consistentRead ?? false,
      ProjectionExpression: this.config.projection.join(', '),
    })

    const getResult = await entity.table.documentClient.send(getItem)

    if (getResult.Item) {
      const item = this.config.skipValidation
        ? (getResult.Item as Partial<EntitySchema<Schema>>)
        : await entity.validatePartialAsync(getResult.Item)

      return {
        item,
        responseMetadata: getResult.$metadata,
        consumedCapacity: getResult.ConsumedCapacity,
      }
    }

    return {
      item: undefined,
      responseMetadata: getResult.$metadata,
      consumedCapacity: getResult.ConsumedCapacity,
    }
  }
}
