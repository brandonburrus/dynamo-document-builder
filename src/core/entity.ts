import type { BaseCommand, BasePaginatable } from '@/commands'
import type {
  DynamoKeyableValue,
  DynamoKeyBuilder,
  DynamoKey,
  GlobalSecondaryIndexKeyBuilders,
  LocalSecondaryIndexKeyBuilders,
  DynamoIndexKey,
} from '@/core/key'
import type { DynamoTable } from '@/core/table'
import type { ZodObject } from 'zod/v4'
import type { EntitySchema, IndexName } from '@/core'
import { DocumentBuilderError } from '@/errors'

/**
 * Configuration type for creating a DynamoEntity.
 *
 * @template Schema - The Zod schema representing the entity's structure.
 *
 * @property table - The DynamoTable instance associated with the entity.
 * @property schema - The Zod schema defining the entity's structure.
 * @property partitionKey - Key builder function to build the partition key from an entity item.
 * @property sortKey - Key builder function to build the sort key from an entity item.
 * @property globalSecondaryIndexes - Mapping of global secondary index names to their key builders.
 * @property localSecondaryIndexes - Mapping of local secondary index names to their key builders.
 */
export type DynamoEntityConfig<Schema extends ZodObject> = {
  table: DynamoTable
  schema: Schema
  partitionKey?: DynamoKeyBuilder<EntitySchema<Schema>>
  sortKey?: DynamoKeyBuilder<EntitySchema<Schema>>
  globalSecondaryIndexes?: GlobalSecondaryIndexKeyBuilders<EntitySchema<Schema>>
  localSecondaryIndexes?: LocalSecondaryIndexKeyBuilders<EntitySchema<Schema>>
}

/**
 * Input type for building *either* a DynamoDB primary key or secondary index key.
 *
 * This is used in cases where the user may want to specify either a full primary key
 * or a secondary index key to identify an item.
 */
export type EntityKeyInput<Item> =
  | {
      key: Partial<Item>
    }
  | {
      index: {
        [index: IndexName]: Partial<Item>
      }
    }

/**
 * Core class that represents a DynamoDB entity.
 *
 * @template Schema - The Zod schema representing the entity's structure.
 */
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

  /**
   * Gets the DynamoDB table associated with this entity.
   */
  public get table(): DynamoTable {
    return this.#table
  }

  /**
   * Gets the Zod schema defining the structure of this entity.
   */
  public get schema(): Schema {
    return this.#schema
  }

  /**
   * Gets the key builders for the secondary indexes defined on this entity.
   */
  public get secondaryIndexKeyBuilders(): {
    gsi: GlobalSecondaryIndexKeyBuilders<EntitySchema<Schema>>
    lsi: LocalSecondaryIndexKeyBuilders<EntitySchema<Schema>>
  } {
    return {
      gsi: this.#gsi,
      lsi: this.#lsi,
    }
  }

  /**
   * Builds the partition key for the given item using the entity's partition key builder.
   */
  public buildPartitionKey(item: Partial<EntitySchema<Schema>>): DynamoKeyableValue | undefined {
    return this.#pk?.(item as EntitySchema<Schema>)
  }

  /**
   * Builds the sort key for the given item using the entity's sort key builder.
   */
  public buildSortKey(item: Partial<EntitySchema<Schema>>): DynamoKeyableValue | undefined {
    return this.#sk?.(item as EntitySchema<Schema>)
  }

  /**
   * Builds the primary key for the given item, including both partition and sort keys if defined.
   *
   * If the entity does not have partition or sort key builders defined, the item is returned as-is.
   */
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

  /**
   * Builds the key for a global secondary index for the given item.
   *
   * If the specified index does not exist or its key builders are not defined,
   * the item is returned as-is.
   */
  public buildGlobalSecondaryIndexKey(
    indexName: IndexName,
    item: Partial<EntitySchema<Schema>>,
  ): DynamoIndexKey {
    const gsiKeyBuilder = this.#gsi[indexName]
    const gsiKeyNames = this.table.globalSecondaryIndexKeyNames[indexName]

    if (!gsiKeyBuilder || !gsiKeyNames) {
      return item as DynamoIndexKey
    }

    const key: DynamoIndexKey = {}
    if (gsiKeyNames.partitionKey) {
      const gsiPK = gsiKeyBuilder.partitionKey(item as EntitySchema<Schema>)
      if (gsiPK !== undefined) {
        key[gsiKeyNames.partitionKey] = gsiPK
      } else if (gsiPK === undefined) {
        return {}
      }
    }
    if (gsiKeyBuilder.sortKey && gsiKeyNames.sortKey) {
      const gsiSK = gsiKeyBuilder.sortKey(item as EntitySchema<Schema>)
      if (gsiSK !== undefined) {
        key[gsiKeyNames.sortKey] = gsiSK
      }
    }
    return key
  }

  /**
   * Builds the key for a local secondary index for the given item.
   *
   * If the specified index does not exist or its key builders are not defined,
   * the item is returned as-is.
   */
  public buildLocalSecondaryIndexKey(
    indexName: IndexName,
    item: Partial<EntitySchema<Schema>>,
  ): DynamoIndexKey {
    const lsiKeyBuilder = this.#lsi[indexName]
    const lsiKeyNames = this.table.localSecondaryIndexKeyNames[indexName]

    if (!this.#pk || !lsiKeyBuilder || !lsiKeyNames) {
      return item as DynamoIndexKey
    }

    const key: DynamoIndexKey = {
      [this.table.partitionKeyName]: this.buildPartitionKey(item)!,
    }
    const lsiSK = lsiKeyBuilder.sortKey(item as EntitySchema<Schema>)
    if (lsiSK !== undefined) {
      key[lsiKeyNames.sortKey] = lsiSK
    }
    return key
  }

  /**
   * Builds either a primary key or a secondary index key based on the provided input.
   *
   * If the input contains a `key`, the primary key is built.
   * If the input contains an `index`, the corresponding secondary index key is built.
   *
   * Works similarly to the other key building methods and will pass-through the key or index
   * input if the entity does not have the necessary key builders defined.
   *
   * @throws DocumentBuilderError if the index name is missing or not defined on the entity.
   */
  public buildPrimaryOrIndexKey(
    keyInput: EntityKeyInput<EntitySchema<Schema>>,
  ): DynamoKey | DynamoIndexKey {
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

  /**
   * Builds all keys (primary and secondary index keys) for the given item.
   */
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

  /**
   * Sends a command to be executed against this entity's table.
   */
  public async send<CommandOutput>(
    command: BaseCommand<CommandOutput, Schema>,
  ): Promise<CommandOutput> {
    return await command.execute(this)
  }

  /**
   * Paginates through results of a paginatable command for this entity's table.
   */
  public async *paginate<CommandOutput>(
    paginatable: BasePaginatable<CommandOutput, Schema>,
  ): AsyncGenerator<CommandOutput, void, unknown> {
    for await (const page of paginatable.executePaginated(this)) {
      yield page
    }
  }
}
