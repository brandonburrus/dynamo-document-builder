import type { ConsumedCapacity, ReturnValue } from '@aws-sdk/client-dynamodb'
import { PutCommand, type PutCommandInput } from '@aws-sdk/lib-dynamodb'
import type { ResponseMetadata } from '@aws-sdk/types'
import type { DynamoEntity, EntitySchema } from '@/core/entity'
import { EntityCommand } from '@/commands/base-entity-command'
import type { ZodObject } from 'zod/v4'

export interface PutConfig<Schema extends ZodObject> {
  item: EntitySchema<Schema>
  returnValues?: ReturnValue
  skipValidation?: boolean
}

export type PutResult<Schema extends ZodObject> = {
  returnItem: EntitySchema<Schema> | undefined
  responseMetadata: ResponseMetadata
  consumedCapacity?: ConsumedCapacity | undefined
}

export class Put<Schema extends ZodObject> extends EntityCommand<PutResult<Schema>, Schema> {
  constructor(private config: PutConfig<Schema>) {
    super()
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<PutResult<Schema>> {
    const encodedData = this.config.skipValidation
      ? this.config.item
      : await entity.encodeAsync(this.config.item)
    const putItemInput: PutCommandInput = {
      TableName: entity.table.tableName,
      Item: {
        ...encodedData,
        ...entity.buildPrimaryKey(this.config.item),
      },
    }
    if (this.config.returnValues) {
      putItemInput.ReturnValues = this.config.returnValues
    }
    const putResult = await entity.table.documentClient.send(new PutCommand(putItemInput))

    let oldItem: EntitySchema<Schema> | undefined
    if (putResult.Attributes) {
      oldItem = this.config.skipValidation
        ? (putResult.Attributes as EntitySchema<Schema>)
        : await entity.validateAsync(putResult.Attributes)
    }

    return {
      returnItem: oldItem,
      responseMetadata: putResult.$metadata,
      consumedCapacity: putResult.ConsumedCapacity,
    }
  }
}
