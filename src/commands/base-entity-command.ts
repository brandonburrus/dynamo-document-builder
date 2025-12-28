import type { DynamoEntity } from '@/core/entity'
import type { ConsumedCapacity, ReturnConsumedCapacity } from '@aws-sdk/client-dynamodb'
import type { ResponseMetadata } from '@aws-sdk/types'
import type { ZodObject } from 'zod/v4'

export abstract class EntityCommand<Output, Schema extends ZodObject> {
  abstract execute(entity: DynamoEntity<Schema>): Promise<Output>
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
