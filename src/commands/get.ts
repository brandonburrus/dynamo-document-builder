import { GetCommand } from '@aws-sdk/lib-dynamodb'
import type { DynamoEntity } from '@/core/entity'
import type { BaseConfig, BaseCommand, BaseResult } from '@/commands/base-command'
import type { ZodObject } from 'zod/v4'
import type { EntitySchema } from '@/core/core-types'
import type { KeyInput } from '@/core/key'

export type GetConfig<Schema extends ZodObject> = BaseConfig &
  KeyInput<EntitySchema<Schema>> & {
    consistent?: boolean
  }

export type GetResult<Schema extends ZodObject> = BaseResult & {
  item: EntitySchema<Schema> | undefined
}

export class Get<Schema extends ZodObject> implements BaseCommand<GetResult<Schema>, Schema> {
  #config: GetConfig<Schema>

  constructor(config: GetConfig<Schema>) {
    this.#config = config
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<GetResult<Schema>> {
    const getItem = new GetCommand({
      TableName: entity.table.tableName,
      Key: entity.buildPrimaryOrIndexKey(this.#config),
      ConsistentRead: this.#config.consistent ?? false,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
    })

    const getResult = await entity.table.documentClient.send(getItem, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    if (getResult.Item) {
      const item = this.#config.skipValidation
        ? (getResult.Item as EntitySchema<Schema>)
        : await entity.schema.parseAsync(getResult.Item)

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
