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

export type ConditionalPutResult<Schema extends ZodObject> = {
  returnItem: EntitySchema<Schema> | undefined
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
    const encodedData = this.config.skipValidation
      ? this.config.item
      : await entity.encodeAsync(this.config.item)
    const { conditionExpression, attributeExpressionMap } = parseCondition(this.config.condition)
    const putCommandInput: PutCommandInput = {
      TableName: entity.table.tableName,
      Item: {
        ...encodedData,
        ...entity.buildPrimaryKey(this.config.item),
      },
      ConditionExpression: conditionExpression,
      ...attributeExpressionMap.toDynamoAttributeExpression(),
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
