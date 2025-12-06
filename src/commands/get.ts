import type { ConsumedCapacity } from '@aws-sdk/client-dynamodb'
import { GetCommand } from '@aws-sdk/lib-dynamodb'
import type { ResponseMetadata } from '@aws-sdk/types'
import type { DynamoEntity, EntitySchema } from '@/core/entity'
import { EntityCommand } from '@/commands/base-entity-command'
import type { ZodObject } from 'zod/v4'

export interface GetConfig<Schema extends ZodObject> {
  key: Partial<EntitySchema<Schema>>
  consistentRead?: boolean
  skipValidation?: boolean
}

export interface GetResult<Schema extends ZodObject> {
  item: EntitySchema<Schema> | undefined
  responseMetadata?: ResponseMetadata
  consumedCapacity?: ConsumedCapacity | undefined
}

export class Get<Schema extends ZodObject> extends EntityCommand<GetResult<Schema>, Schema> {
  constructor(private config: GetConfig<Schema>) {
    super()
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<GetResult<Schema>> {
    const getItem = new GetCommand({
      TableName: entity.table.tableName,
      Key: entity.buildPrimaryKey(this.config.key),
      ConsistentRead: this.config.consistentRead ?? false,
    })
    const getResult = await entity.table.documentClient.send(getItem)
    if (getResult.Item) {
      const item = this.config.skipValidation
        ? (getResult.Item as EntitySchema<Schema>)
        : await entity.validateAsync(getResult.Item)
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
