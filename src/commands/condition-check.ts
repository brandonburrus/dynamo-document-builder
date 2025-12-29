import type { Condition } from '@/conditions/condition-types'
import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema, TransactWriteOperation } from '@/core/core-types'
import type { ReturnValuesOnConditionCheckFailure } from '@aws-sdk/client-dynamodb'
import type { WriteTransactable } from '@/commands/base-command'
import type { ZodObject } from 'zod/v4'
import { parseCondition } from '@/conditions/condition-parser'

export type ConditionCheckConfig<Schema extends ZodObject> = {
  key: Partial<EntitySchema<Schema>>
  condition: Condition
  returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure
}

export class ConditionCheck<Schema extends ZodObject> implements WriteTransactable<Schema> {
  #config: ConditionCheckConfig<Schema>

  constructor(config: ConditionCheckConfig<Schema>) {
    this.#config = config
  }

  public async prepareWriteTransaction(
    entity: DynamoEntity<Schema>,
  ): Promise<TransactWriteOperation> {
    const { conditionExpression, attributeExpressionMap } = parseCondition(this.#config.condition)
    return {
      ConditionCheck: {
        TableName: entity.table.tableName,
        Key: entity.buildPrimaryKey(this.#config.key),
        ConditionExpression: conditionExpression,
        ...attributeExpressionMap.toDynamoAttributeExpression(),
        ReturnValuesOnConditionCheckFailure: this.#config.returnValuesOnConditionCheckFailure,
      },
    }
  }
}
