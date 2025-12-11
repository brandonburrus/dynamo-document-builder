import type { ConsumedCapacity } from '@aws-sdk/client-dynamodb'
import { UpdateCommand } from '@aws-sdk/lib-dynamodb'
import type { ResponseMetadata } from '@aws-sdk/types'
import type { DynamoEntity, EntitySchema } from '@/core/entity'
import { EntityCommand } from '@/commands/base-entity-command'
import type { ZodObject } from 'zod/v4'
import { type UpdateValues, parseUpdate } from '@/updates'

export interface UpdateConfig<Schema extends ZodObject> {
  key: Partial<EntitySchema<Schema>>
  update: UpdateValues
}

export interface UpdateResult {
  responseMetadata?: ResponseMetadata
  consumedCapacity?: ConsumedCapacity | undefined
}

export class Update<EntitySchema extends ZodObject> extends EntityCommand<
  UpdateResult,
  EntitySchema
> {
  constructor(private config: UpdateConfig<EntitySchema>) {
    super()
  }

  public async execute(entity: DynamoEntity<EntitySchema>): Promise<UpdateResult> {
    const { updateExpression, attributeExpressionMap } = parseUpdate(this.config.update)
    const updateItem = new UpdateCommand({
      TableName: entity.table.tableName,
      Key: entity.buildPrimaryKey(this.config.key),
      UpdateExpression: updateExpression,
      ...attributeExpressionMap.toDynamoAttributeExpression(),
    })
    const updateResult = await entity.table.documentClient.send(updateItem)
    return {
      responseMetadata: updateResult.$metadata,
      consumedCapacity: updateResult.ConsumedCapacity,
    }
  }
}
