import type {
  ConsumedCapacity,
  ReturnConsumedCapacity,
  ReturnItemCollectionMetrics,
  ReturnValue,
  ReturnValuesOnConditionCheckFailure,
} from '@aws-sdk/client-dynamodb'
import { DeleteCommand, type DeleteCommandInput } from '@aws-sdk/lib-dynamodb'
import type { ResponseMetadata } from '@aws-sdk/types'
import type { DynamoEntity, EntitySchema } from '@/core/entity'
import { EntityCommand } from '@/commands/base-entity-command'
import { parseCondition } from '@/conditions/condition-parser'
import type { Condition } from '@/conditions/condition-types'
import type { ZodObject } from 'zod/v4'

export interface ConditionalDeleteConfig<Schema extends ZodObject> {
  key: Partial<EntitySchema<Schema>>
  condition: Condition
  returnValues?: ReturnValue
  returnConsumedCapacity?: ReturnConsumedCapacity
  returnItemCollectionMetrics?: ReturnItemCollectionMetrics
  returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure
  skipValidation?: boolean
}

export interface ConditionalDeleteResult<Schema extends ZodObject> {
  oldItem?: EntitySchema<Schema> | undefined
  responseMetadata?: ResponseMetadata
  consumedCapacity?: ConsumedCapacity | undefined
}

export class ConditionalDelete<Schema extends ZodObject> extends EntityCommand<
  ConditionalDeleteResult<Schema>,
  Schema
> {
  constructor(private config: ConditionalDeleteConfig<Schema>) {
    super()
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<ConditionalDeleteResult<Schema>> {
    const deleteCommandInput: DeleteCommandInput = {
      TableName: entity.table.tableName,
      Key: entity.buildPrimaryKey(this.config.key),
      ...parseCondition(this.config.condition),
    }
    if (this.config.returnValues) {
      deleteCommandInput.ReturnValues = this.config.returnValues
    }
    if (this.config.returnConsumedCapacity) {
      deleteCommandInput.ReturnConsumedCapacity = this.config.returnConsumedCapacity
    }
    if (this.config.returnItemCollectionMetrics) {
      deleteCommandInput.ReturnItemCollectionMetrics = this.config.returnItemCollectionMetrics
    }
    if (this.config.returnValuesOnConditionCheckFailure) {
      deleteCommandInput.ReturnValuesOnConditionCheckFailure =
        this.config.returnValuesOnConditionCheckFailure
    }
    const deleteItem = new DeleteCommand(deleteCommandInput)
    const deleteResult = await entity.table.documentClient.send(deleteItem)
    let oldItem: EntitySchema<Schema> | undefined
    if (deleteResult.Attributes && !this.config.skipValidation) {
      oldItem = await entity.validateAsync(deleteResult.Attributes)
    }
    return {
      oldItem,
      responseMetadata: deleteResult.$metadata,
      consumedCapacity: deleteResult.ConsumedCapacity,
    }
  }
}
