import type { DynamoEntity } from '@/core/entity'
import type { ZodObject } from 'zod/v4'

export abstract class EntityCommand<Output, SchemaDef extends ZodObject> {
  abstract execute(entity: DynamoEntity<SchemaDef>): Promise<Output>
}
