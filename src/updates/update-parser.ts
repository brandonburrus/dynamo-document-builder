import type { DynamoAttributeExpression } from '@/attributes'
import type { UpdateValues, UpdateExpression, ValueReference } from '@/updates/update-types'
import { buildAttributeExpression } from '@/attributes/attribute-builder'
import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'
import {
  $set,
  $remove,
  $delete,
  $add,
  $subtract,
  $append,
  $addToSet,
  $prepend,
  $ref,
} from '@/updates/update-symbols'

export interface DynamoUpdateExpression extends DynamoAttributeExpression {
  UpdateExpression: string
}

export class InvalidDynamoDBUpdateExpressionError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'InvalidDynamoDBUpdateExpressionError'
  }
}

function sanitizeAttributeName(name: string): string {
  return name.replace(/[^a-zA-Z0-9_]/g, '_')
}

function parseAttributePath(path: string): {
  expression: string
  names: Record<string, string>
} {
  const names: Record<string, string> = {}
  const parts = path.split('.')

  const expression = parts
    .map(part => {
      const match = part.match(/^(.+?)(\[\d+\])$/)
      if (match) {
        const attrName = match[1]!
        const index = match[2]!
        const placeholder = `#${sanitizeAttributeName(attrName)}`
        names[placeholder] = attrName
        return `${placeholder}${index}`
      }
      const placeholder = `#${sanitizeAttributeName(part)}`
      names[placeholder] = part
      return placeholder
    })
    .join('.')

  return { expression, names }
}

function createValuePlaceholder(path: string): string {
  const sanitized = path.replace(/\[(\d+)\]/g, '_$1')
  return `:${sanitizeAttributeName(sanitized)}`
}

function isValueReference(value: unknown): value is ValueReference {
  return typeof value === 'object' && value !== null && 'type' in value && value.type === $ref
}

function isUpdateExpression(value: unknown): value is UpdateExpression {
  return (
    typeof value === 'object' &&
    value !== null &&
    'type' in value &&
    (value.type === $set || value.type === $remove || value.type === $delete || value.type === $add)
  )
}

export function parseUpdate(update: UpdateValues): DynamoUpdateExpression {
  if (Object.keys(update).length === 0) {
    throw new InvalidDynamoDBUpdateExpressionError('Update expression cannot be empty')
  }

  const names: Record<string, string> = {}
  const values: Record<string, NativeAttributeValue> = {}
  const setExpressions: string[] = []
  const removeExpressions: string[] = []
  const addExpressions: string[] = []
  const deleteExpressions: string[] = []

  for (const [path, value] of Object.entries(update)) {
    const { expression: pathExpr, names: pathNames } = parseAttributePath(path)
    Object.assign(names, pathNames)

    if (isValueReference(value)) {
      const { expression: refExpr, names: refNames } = parseAttributePath(value.to)
      Object.assign(names, refNames)

      if (value.default !== undefined) {
        const defaultPlaceholder = `${createValuePlaceholder(path)}_default`
        values[defaultPlaceholder] = value.default
        setExpressions.push(`${pathExpr} = if_not_exists(${refExpr}, ${defaultPlaceholder})`)
      } else {
        setExpressions.push(`${pathExpr} = ${refExpr}`)
      }
    } else if (isUpdateExpression(value)) {
      if (value.type === $remove) {
        removeExpressions.push(pathExpr)
      } else if (value.type === $delete) {
        if (isValueReference(value.values)) {
          const { expression: refExpr, names: refNames } = parseAttributePath(value.values.to)
          Object.assign(names, refNames)
          deleteExpressions.push(`${pathExpr} ${refExpr}`)
        } else {
          const valuePlaceholder = createValuePlaceholder(path)
          values[valuePlaceholder] = value.values
          deleteExpressions.push(`${pathExpr} ${valuePlaceholder}`)
        }
      } else if (value.type === $add) {
        if ('op' in value && value.op === $addToSet) {
          if (isValueReference(value.values)) {
            const { expression: refExpr, names: refNames } = parseAttributePath(value.values.to)
            Object.assign(names, refNames)
            addExpressions.push(`${pathExpr} ${refExpr}`)
          } else {
            const valuePlaceholder = createValuePlaceholder(path)
            values[valuePlaceholder] = value.values
            addExpressions.push(`${pathExpr} ${valuePlaceholder}`)
          }
        }
      } else if (value.type === $set) {
        if ('op' in value) {
          if (value.op === $add) {
            if (isValueReference(value.value)) {
              const { expression: refExpr, names: refNames } = parseAttributePath(value.value.to)
              Object.assign(names, refNames)
              setExpressions.push(`${pathExpr} = ${pathExpr} + ${refExpr}`)
            } else {
              const valuePlaceholder = createValuePlaceholder(path)
              values[valuePlaceholder] = value.value
              setExpressions.push(`${pathExpr} = ${pathExpr} + ${valuePlaceholder}`)
            }
          } else if (value.op === $subtract) {
            if (isValueReference(value.value)) {
              const { expression: refExpr, names: refNames } = parseAttributePath(value.value.to)
              Object.assign(names, refNames)
              setExpressions.push(`${pathExpr} = ${pathExpr} - ${refExpr}`)
            } else {
              const valuePlaceholder = createValuePlaceholder(path)
              values[valuePlaceholder] = value.value
              setExpressions.push(`${pathExpr} = ${pathExpr} - ${valuePlaceholder}`)
            }
          } else if (value.op === $append) {
            if (isValueReference(value.values)) {
              const { expression: refExpr, names: refNames } = parseAttributePath(value.values.to)
              Object.assign(names, refNames)
              setExpressions.push(`${pathExpr} = list_append(${pathExpr}, ${refExpr})`)
            } else {
              const valuePlaceholder = createValuePlaceholder(path)
              values[valuePlaceholder] = value.values
              setExpressions.push(`${pathExpr} = list_append(${pathExpr}, ${valuePlaceholder})`)
            }
          } else if (value.op === $prepend) {
            if (isValueReference(value.values)) {
              const { expression: refExpr, names: refNames } = parseAttributePath(value.values.to)
              Object.assign(names, refNames)
              setExpressions.push(`${pathExpr} = list_append(${refExpr}, ${pathExpr})`)
            } else {
              const valuePlaceholder = createValuePlaceholder(path)
              values[valuePlaceholder] = value.values
              setExpressions.push(`${pathExpr} = list_append(${valuePlaceholder}, ${pathExpr})`)
            }
          }
        }
      }
    } else {
      const valuePlaceholder = createValuePlaceholder(path)
      values[valuePlaceholder] = value
      setExpressions.push(`${pathExpr} = ${valuePlaceholder}`)
    }
  }

  const parts: string[] = []
  if (setExpressions.length > 0) {
    parts.push(`SET ${setExpressions.join(', ')}`)
  }
  if (removeExpressions.length > 0) {
    parts.push(`REMOVE ${removeExpressions.join(', ')}`)
  }
  if (addExpressions.length > 0) {
    parts.push(`ADD ${addExpressions.join(', ')}`)
  }
  if (deleteExpressions.length > 0) {
    parts.push(`DELETE ${deleteExpressions.join(', ')}`)
  }

  const UpdateExpression = parts.join(' ')
  const result = buildAttributeExpression({ names, values })

  return {
    UpdateExpression,
    ...result,
  }
}
