import type { DynamoEntity } from '@/core/entity'
import type { ConsumedCapacity, ReturnConsumedCapacity } from '@aws-sdk/client-dynamodb'
import type { ResponseMetadata } from '@aws-sdk/types'
import type { ZodObject } from 'zod/v4'

export type BaseCommand<Output, Schema extends ZodObject> = {
  execute(entity: DynamoEntity<Schema>): Promise<Output>
}

export type BasePaginatable<Output, Schema extends ZodObject> = {
  executePaginated(entity: DynamoEntity<Schema>): AsyncGenerator<Output, void, unknown>
}

export type BaseConfig = {
  skipValidation?: boolean
  returnConsumedCapacity?: ReturnConsumedCapacity
  abortController?: AbortController
  timeoutMs?: number
}

export type BaseResult = {
  responseMetadata?: ResponseMetadata
  consumedCapacity?: ConsumedCapacity | undefined
}
