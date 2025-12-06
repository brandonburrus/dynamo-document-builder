import {
  type output as ZodOutput,
  type input as ZodInput,
  encode,
  encodeAsync,
  parse,
  parseAsync,
} from 'zod/v4/core'
import type { EntityCommand } from '@/commands/base-entity-command'
import type { DynamoKeyableValue, DynamoKeyBuilder, DynamoPrimaryKey } from '@/core/key'
import type { DynamoTable } from '@/core/table'
import type { ZodObject } from 'zod/v4'

// This is the actual type of the entity items dervied from the zod schema
export type EntitySchema<SchemaDef extends ZodObject> = ZodOutput<SchemaDef>
export type EncodedEntitySchema<SchemaDef extends ZodObject> = ZodInput<SchemaDef>

// External type exposed to module users for getting the schema type from a defined entity
// biome-ignore lint/suspicious/noExplicitAny: TODO
export type Entity<E extends DynamoEntity<any>> = ZodOutput<E['schema']>

export interface DynamoEntityConfig<Schema extends ZodObject> {
  table: DynamoTable
  schema: Schema
  partitionKey: DynamoKeyBuilder<EntitySchema<Schema>>
  sortKey?: DynamoKeyBuilder<EntitySchema<Schema>>
}

/**
 * TODO: Add documentation
 */
export class DynamoEntity<SchemaDef extends ZodObject> {
  #table: DynamoTable
  #schema: SchemaDef

  #pk: DynamoKeyBuilder<EntitySchema<SchemaDef>>
  #sk: DynamoKeyBuilder<EntitySchema<SchemaDef>> | undefined

  constructor(config: DynamoEntityConfig<SchemaDef>) {
    this.#table = config.table
    this.#schema = config.schema

    this.#pk = config.partitionKey
    this.#sk = config.sortKey
  }

  public get table(): DynamoTable {
    return this.#table
  }

  public get schema(): SchemaDef {
    return this.#schema
  }

  public buildPartitionKey(item: Partial<EntitySchema<SchemaDef>>): DynamoKeyableValue {
    return this.#pk(item as EntitySchema<SchemaDef>)
  }

  public buildSortKey(item: Partial<EntitySchema<SchemaDef>>): DynamoKeyableValue | undefined {
    return this.#sk ? this.#sk(item as EntitySchema<SchemaDef>) : undefined
  }

  public buildPrimaryKey(item: Partial<EntitySchema<SchemaDef>>): DynamoPrimaryKey {
    const primaryKey = {
      [this.table.partitionKeyName]: this.buildPartitionKey(item),
      [this.table.sortKeyName]: this.buildSortKey(item),
    } as DynamoPrimaryKey
    return primaryKey
  }

  public async send<CommandOutput>(
    entityCommand: EntityCommand<CommandOutput, SchemaDef>,
  ): Promise<CommandOutput> {
    return await entityCommand.execute(this)
  }

  public validate(item: unknown): EntitySchema<SchemaDef> {
    return parse(this.#schema, item)
  }

  public validateAsync(item: unknown): Promise<EntitySchema<SchemaDef>> {
    return parseAsync(this.#schema, item)
  }

  public validatePartial(item: unknown): Partial<EntitySchema<SchemaDef>> {
    return parse(this.#schema.partial(), item) as Partial<EntitySchema<SchemaDef>>
  }

  public validatePartialAsync(item: unknown): Promise<Partial<EntitySchema<SchemaDef>>> {
    return parseAsync(this.#schema.partial(), item) as Promise<Partial<EntitySchema<SchemaDef>>>
  }

  public encode(item: EntitySchema<SchemaDef>): EncodedEntitySchema<SchemaDef> {
    return encode(this.#schema, item)
  }

  public encodeAsync(item: EntitySchema<SchemaDef>): Promise<EncodedEntitySchema<SchemaDef>> {
    return encodeAsync(this.#schema, item)
  }
}
