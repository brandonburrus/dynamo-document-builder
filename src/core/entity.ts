import type { BaseCommand, BasePaginatable } from '@/commands/base-command'
import type {
  DynamoKeyableValue,
  DynamoKeyBuilder,
  DynamoKey,
  GlobalSecondaryIndexKeyBuilders,
  LocalSecondaryIndexKeyBuilders,
} from '@/core/key'
import type { DynamoTable } from '@/core/table'
import type { ZodObject } from 'zod/v4'
import type { EntitySchema, IndexName } from '@/core/core-types'
import { DocumentBuilderError } from '@/errors'

export type DynamoEntityConfig<Schema extends ZodObject> = {
  table: DynamoTable
  schema: Schema
  partitionKey?: DynamoKeyBuilder<EntitySchema<Schema>>
  sortKey?: DynamoKeyBuilder<EntitySchema<Schema>>
  globalSecondaryIndexes?: GlobalSecondaryIndexKeyBuilders<EntitySchema<Schema>>
  localSecondaryIndexes?: LocalSecondaryIndexKeyBuilders<EntitySchema<Schema>>
}

export type EntityKeyInput<Item> =
  | {
      key: Partial<Item>
    }
  | {
      index: {
        [index: IndexName]: Partial<Item>
      }
    }

export class DynamoEntity<Schema extends ZodObject> {
  #table: DynamoTable
  #schema: Schema

  #pk: DynamoKeyBuilder<EntitySchema<Schema>> | undefined
  #sk: DynamoKeyBuilder<EntitySchema<Schema>> | undefined

  #gsi: GlobalSecondaryIndexKeyBuilders<EntitySchema<Schema>>
  #lsi: LocalSecondaryIndexKeyBuilders<EntitySchema<Schema>>

  constructor(config: DynamoEntityConfig<Schema>) {
    this.#table = config.table
    this.#schema = config.schema

    this.#pk = config.partitionKey
    this.#sk = config.sortKey

    this.#gsi = config.globalSecondaryIndexes ?? {}
    this.#lsi = config.localSecondaryIndexes ?? {}
  }

  public get table(): DynamoTable {
    return this.#table
  }

  public get schema(): Schema {
    return this.#schema
  }

  public get secondaryIndexKeyBuilders(): {
    gsi: GlobalSecondaryIndexKeyBuilders<EntitySchema<Schema>>
    lsi: LocalSecondaryIndexKeyBuilders<EntitySchema<Schema>>
  } {
    return {
      gsi: this.#gsi,
      lsi: this.#lsi,
    }
  }

  public buildPartitionKey(item: Partial<EntitySchema<Schema>>): DynamoKeyableValue | undefined {
    return this.#pk?.(item as EntitySchema<Schema>)
  }

  public buildSortKey(item: Partial<EntitySchema<Schema>>): DynamoKeyableValue | undefined {
    return this.#sk?.(item as EntitySchema<Schema>)
  }

  public buildPrimaryKey(item: Partial<EntitySchema<Schema>>): DynamoKey {
    if (!this.#pk && !this.#sk) {
      return item as DynamoKey
    }
    const key: DynamoKey = {}
    if (this.#pk) {
      const pk = this.buildPartitionKey(item)
      if (pk !== undefined) {
        key[this.table.partitionKeyName] = pk
      }
    }
    if (this.#sk && this.table.sortKeyName !== null) {
      const sk = this.buildSortKey(item)
      if (sk !== undefined) {
        key[this.table.sortKeyName] = sk
      }
    }
    return key
  }

  public buildGlobalSecondaryIndexKey(
    indexName: string,
    item: Partial<EntitySchema<Schema>>,
  ): DynamoKey {
    const gsiKeyBuilder = this.#gsi[indexName]
    const gsiKeyNames = this.table.globalSecondaryIndexKeyNames[indexName]

    if (!gsiKeyBuilder || !gsiKeyNames) {
      return item as DynamoKey
    }

    const key: DynamoKey = {
      [gsiKeyNames.partitionKey]: gsiKeyBuilder.partitionKey(item as EntitySchema<Schema>),
    }
    if (gsiKeyBuilder.sortKey && gsiKeyNames.sortKey) {
      key[gsiKeyNames.sortKey] = gsiKeyBuilder.sortKey(item as EntitySchema<Schema>)
    }
    return key
  }

  public buildLocalSecondaryIndexKey(
    indexName: string,
    item: Partial<EntitySchema<Schema>>,
  ): DynamoKey {
    const lsiKeyBuilder = this.#lsi[indexName]
    const lsiKeyNames = this.table.localSecondaryIndexKeyNames[indexName]

    if (!this.#pk || !lsiKeyBuilder || !lsiKeyNames) {
      return item as DynamoKey
    }

    return {
      [this.table.partitionKeyName]: this.buildPartitionKey(item)!,
      [lsiKeyNames.sortKey]: lsiKeyBuilder.sortKey(item as EntitySchema<Schema>),
    } satisfies DynamoKey
  }

  public buildPrimaryOrIndexKey(keyInput: EntityKeyInput<EntitySchema<Schema>>): DynamoKey {
    if ('key' in keyInput) {
      return this.buildPrimaryKey(keyInput.key)
    }
    const indexInput = Object.keys(keyInput.index)
    if (indexInput.length === 0) {
      throw new DocumentBuilderError('Index name is required to build index key')
    } else if (indexInput.length > 1) {
      throw new DocumentBuilderError('Only one index can be specified to build index key')
    }
    const indexName = indexInput[0]!
    if (this.#gsi[indexName]) {
      return this.buildGlobalSecondaryIndexKey(indexName, keyInput.index[indexName]!)
    }
    if (this.#lsi[indexName]) {
      return this.buildLocalSecondaryIndexKey(indexName, keyInput.index[indexName]!)
    }
    throw new DocumentBuilderError(`Index "${indexName}" is not defined on entity`)
  }

  public buildAllKeys(item: Partial<EntitySchema<Schema>>): DynamoKey {
    const allKeys: DynamoKey = this.buildPrimaryKey(item)
    for (const indexName of Object.keys(this.#gsi)) {
      Object.assign(allKeys, this.buildGlobalSecondaryIndexKey(indexName, item))
    }
    for (const indexName of Object.keys(this.#lsi)) {
      Object.assign(allKeys, this.buildLocalSecondaryIndexKey(indexName, item))
    }
    return allKeys
  }

  public async send<CommandOutput>(
    command: BaseCommand<CommandOutput, Schema>,
  ): Promise<CommandOutput> {
    return await command.execute(this)
  }

  public async *paginate<CommandOutput>(
    paginatable: BasePaginatable<CommandOutput, Schema>,
  ): AsyncGenerator<CommandOutput, void, unknown> {
    for await (const page of paginatable.executePaginated(this)) {
      yield page
    }
  }
}
