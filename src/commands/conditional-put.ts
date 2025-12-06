import type {
  ConsumedCapacity,
  ReturnConsumedCapacity,
  ReturnItemCollectionMetrics,
  ReturnValue,
  ReturnValuesOnConditionCheckFailure,
} from '@aws-sdk/client-dynamodb'
import { PutCommand, type PutCommandInput } from '@aws-sdk/lib-dynamodb'
import type { ResponseMetadata } from '@aws-sdk/types'
import type { DynamoEntity, EntitySchema } from '@/core/entity'
import { EntityCommand } from '@/commands/base-entity-command'
import { parseCondition } from '@/conditions/condition-parser'
import type { Condition } from '@/conditions/condition-types'
import type { ZodObject } from 'zod/v4'

export interface ConditionalPutConfig<Schema extends ZodObject> {
  item: EntitySchema<Schema>
  condition: Condition
  returnValues?: ReturnValue
  returnConsumedCapacity?: ReturnConsumedCapacity
  returnItemCollectionMetrics?: ReturnItemCollectionMetrics
  returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure
  skipValidation?: boolean
}

export interface ConditionalPutResult<Schema extends ZodObject> {
  oldItem?: EntitySchema<Schema> | undefined
  responseMetadata?: ResponseMetadata
  consumedCapacity?: ConsumedCapacity | undefined
}

export class ConditionalPut<Schema extends ZodObject> extends EntityCommand<
  ConditionalPutResult<Schema>,
  Schema
> {
  constructor(private config: ConditionalPutConfig<Schema>) {
    super()
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<ConditionalPutResult<Schema>> {
    const baseData: EntitySchema<Schema> = this.config.skipValidation
      ? this.config.item
      : await entity.validateAsync(this.config.item)
    const putCommandInput: PutCommandInput = {
      TableName: entity.table.tableName,
      Item: {
        ...baseData,
        ...entity.buildPrimaryKey(baseData),
      },
      ...parseCondition(this.config.condition),
    }
    if (this.config.returnValues) {
      putCommandInput.ReturnValues = this.config.returnValues
    }
    if (this.config.returnConsumedCapacity) {
      putCommandInput.ReturnConsumedCapacity = this.config.returnConsumedCapacity
    }
    if (this.config.returnItemCollectionMetrics) {
      putCommandInput.ReturnItemCollectionMetrics = this.config.returnItemCollectionMetrics
    }
    if (this.config.returnValuesOnConditionCheckFailure) {
      putCommandInput.ReturnValuesOnConditionCheckFailure =
        this.config.returnValuesOnConditionCheckFailure
    }
    const putItem = new PutCommand(putCommandInput)
    const putResult = await entity.table.documentClient.send(putItem)
    let oldItem: EntitySchema<Schema> | undefined
    if (putResult.Attributes && !this.config.skipValidation) {
      oldItem = await entity.validateAsync(putResult.Attributes)
    }
    return {
      oldItem,
      responseMetadata: putResult.$metadata,
      consumedCapacity: putResult.ConsumedCapacity,
    }
  }
}
