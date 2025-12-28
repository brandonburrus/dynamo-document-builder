import type { output as ZodOutput, input as ZodInput, ZodObject } from 'zod/v4'
import type { DynamoEntity } from '@/core/entity'

// This is the actual type of the entity items dervied from the zod schema
export type EntitySchema<SchemaDef extends ZodObject> = ZodOutput<SchemaDef>
export type EncodedEntitySchema<SchemaDef extends ZodObject> = ZodInput<SchemaDef>

// External type exposed to module users for getting the schema type from a defined entity
// biome-ignore lint/suspicious/noExplicitAny: Schema is the output type, not the input; any is ok
export type Entity<E extends DynamoEntity<any>> = ZodOutput<E['schema']>
// biome-ignore lint/suspicious/noExplicitAny: Schema is the output type, not the input; any is ok
export type EncodedEntity<E extends DynamoEntity<any>> = ZodInput<E['schema']>

export type IndexName = string

export type GlobalSecondaryIndexKeyName = {
  partitionKey: string
  sortKey?: string
}

export type LocalSecondaryIndexKeyName = {
  sortKey: string
}

export type NamedGlobalSecondaryIndexKeyNames = Record<IndexName, GlobalSecondaryIndexKeyName>

export type NamedLocalSecondaryIndexKeyNames = Record<IndexName, LocalSecondaryIndexKeyName>
