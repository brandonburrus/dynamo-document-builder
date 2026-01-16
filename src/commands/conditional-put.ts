import type { Condition } from '@/conditions'
import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema, TransactWriteOperation } from '@/core'
import type {
  ItemCollectionMetrics,
  ReturnValuesOnConditionCheckFailure,
} from '@aws-sdk/client-dynamodb'
import type { PutConfig } from '@/commands/put'
import type { ZodObject } from 'zod/v4'
import { PutCommand } from '@aws-sdk/lib-dynamodb'
import { parseCondition } from '@/conditions/condition-parser'
import type { BaseResult, BaseCommand, WriteTransactable } from '@/commands'

/**
 * Configuration for the ConditionalPut command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 */
export type ConditionalPutConfig<Schema extends ZodObject> = PutConfig<Schema> & {
  condition: Condition
  returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure
}

/**
 * Result of the ConditionalPut command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 */
export type ConditionalPutResult<Schema extends ZodObject> = BaseResult & {
  returnItem: EntitySchema<Schema> | undefined
  itemCollectionMetrics: ItemCollectionMetrics | undefined
}

/**
 * Command to create or replace an item with a condition expression.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 *
 * @example
 * ```typescript
 * import { DynamoTable, DynamoEntity, key, ConditionalPut, notExists } from 'dynamo-document-builder';
 *
 * const table = new DynamoTable({
 *   tableName: 'ExampleTable',
 *   documentClient,
 * });
 *
 * const userEntity = new DynamoEntity({
 *   table,
 *   schema: z.object({
 *     userId: z.string(),
 *     name: z.string(),
 *     status: z.string(),
 *   }),
 *   partitionKey: user => key('USER', user.userId),
 *   sortKey: () => 'METADATA',
 * });
 *
 * const conditionalPutCommand = new ConditionalPut({
 *   item: { userId: 'user123', name: 'John', status: 'active' },
 *   condition: { status: 'draft' },
 *   returnValuesOnConditionCheckFailure: 'ALL_OLD',
 * });
 *
 * await userEntity.send(conditionalPutCommand);
 * ```
 */
export class ConditionalPut<Schema extends ZodObject>
  implements BaseCommand<ConditionalPutResult<Schema>, Schema>, WriteTransactable<Schema>
{
  #config: ConditionalPutConfig<Schema>

  constructor(config: ConditionalPutConfig<Schema>) {
    this.#config = config
  }

  public async buildItem(entity: DynamoEntity<Schema>) {
    const encodedData = this.#config.skipValidation
      ? this.#config.item
      : await entity.schema.encodeAsync(this.#config.item)

    return {
      ...encodedData,
      ...entity.buildAllKeys(this.#config.item),
    }
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<ConditionalPutResult<Schema>> {
    const item = await this.buildItem(entity)
    const { conditionExpression, attributeExpressionMap } = parseCondition(this.#config.condition)

    const put = new PutCommand({
      TableName: entity.table.tableName,
      Item: item,
      ConditionExpression: conditionExpression,
      ...attributeExpressionMap.toDynamoAttributeExpression(),
      ReturnValues: this.#config.returnValues,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
      ReturnItemCollectionMetrics: this.#config.returnItemCollectionMetrics,
      ReturnValuesOnConditionCheckFailure: this.#config.returnValuesOnConditionCheckFailure,
    })

    const putResult = await entity.table.documentClient.send(put, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    if (putResult.Attributes) {
      const returnItem: EntitySchema<Schema> = this.#config.skipValidation
        ? (putResult.Attributes as EntitySchema<Schema>)
        : await entity.schema.parseAsync(putResult.Attributes)

      return {
        returnItem,
        responseMetadata: putResult.$metadata,
        consumedCapacity: putResult.ConsumedCapacity,
        itemCollectionMetrics: putResult.ItemCollectionMetrics,
      }
    }

    return {
      returnItem: undefined,
      responseMetadata: putResult.$metadata,
      consumedCapacity: putResult.ConsumedCapacity,
      itemCollectionMetrics: putResult.ItemCollectionMetrics,
    }
  }

  public async prepareWriteTransaction(
    entity: DynamoEntity<Schema>,
  ): Promise<TransactWriteOperation> {
    const item = await this.buildItem(entity)
    const { conditionExpression, attributeExpressionMap } = parseCondition(this.#config.condition)
    return {
      Put: {
        TableName: entity.table.tableName,
        Item: item,
        ConditionExpression: conditionExpression,
        ...attributeExpressionMap.toDynamoAttributeExpression(),
        ReturnValuesOnConditionCheckFailure: this.#config.returnValuesOnConditionCheckFailure,
      },
    }
  }
}
