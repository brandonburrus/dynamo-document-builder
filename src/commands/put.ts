import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema, TransactWriteOperation } from '@/core'
import type {
  ItemCollectionMetrics,
  ReturnItemCollectionMetrics,
  ReturnValue,
} from '@aws-sdk/client-dynamodb'
import type { ZodObject } from 'zod/v4'
import { PutCommand } from '@aws-sdk/lib-dynamodb'
import type { BaseConfig, BaseCommand, BaseResult, WriteTransactable } from '@/commands'

/**
 * Configuration for the Put command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 */
export type PutConfig<Schema extends ZodObject> = BaseConfig & {
  item: EntitySchema<Schema>
  returnValues?: ReturnValue
  returnItemCollectionMetrics?: ReturnItemCollectionMetrics
}

/**
 * Result of the Put command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 */
export type PutResult<Schema extends ZodObject> = BaseResult & {
  returnItem: Partial<EntitySchema<Schema>> | undefined
  itemCollectionMetrics?: ItemCollectionMetrics
}

/**
 * Command to create or replace an item in a DynamoDB table.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 *
 * @example
 * ```typescript
 * import { DynamoTable, DynamoEntity, key, Put } from 'dynamo-document-builder';
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
 *     email: z.string(),
 *   }),
 *   partitionKey: user => key('USER', user.userId),
 *   sortKey: () => 'METADATA',
 * });
 *
 * const putCommand = new Put({
 *   item: {
 *     userId: 'user123',
 *     name: 'John Doe',
 *     email: 'john@example.com',
 *   },
 *   returnValues: 'ALL_OLD',
 * });
 *
 * const { returnItem } = await userEntity.send(putCommand);
 * ```
 */
export class Put<Schema extends ZodObject>
  implements BaseCommand<PutResult<Schema>, Schema>, WriteTransactable<Schema>
{
  #config: PutConfig<Schema>

  constructor(config: PutConfig<Schema>) {
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

  public async execute(entity: DynamoEntity<Schema>): Promise<PutResult<Schema>> {
    const item = await this.buildItem(entity)
    const put = new PutCommand({
      TableName: entity.table.tableName,
      Item: item,
      ReturnValues: this.#config.returnValues,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
      ReturnItemCollectionMetrics: this.#config.returnItemCollectionMetrics,
    })
    const putResult = await entity.table.documentClient.send(put, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })
    if (putResult.Attributes) {
      const returnItem = (
        this.#config.skipValidation
          ? putResult.Attributes
          : await entity.schema.partial().parseAsync(putResult.Attributes)
      ) as Partial<EntitySchema<Schema>>

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
    return {
      Put: {
        TableName: entity.table.tableName,
        Item: item,
      },
    }
  }
}
