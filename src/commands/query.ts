// import type { $ZodObject, infer as Infer } from 'zod/v4/core'
// import type { DynamoEntity } from '@/core/entity'
// import { EntityCommand } from '@/commands/base-entity-command'
//
// export interface QueryConfig<EntitySchema extends $ZodObject> {
//   keyCondition: unknown
// }
//
// export interface QueryResult<EntitySchema extends $ZodObject> {
//   items: Infer<EntitySchema>[]
// }
//
// export class Query<EntitySchema extends $ZodObject> extends EntityCommand<
//   EntitySchema,
//   QueryResult<EntitySchema>
// > {
//   constructor(private config: QueryConfig<EntitySchema>) {
//     super()
//   }
//
//   public async execute(entity: DynamoEntity<EntitySchema>): Promise<QueryResult<EntitySchema>> {
//     throw new Error('Unimplemented')
//   }
// }
