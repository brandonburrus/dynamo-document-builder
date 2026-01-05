import { AttributeExpressionMap } from '@/attributes/attribute-map'
import type { UpdateValues, UpdateExpression, ValueReference } from '@/updates/update-types'
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
import { DocumentBuilderError } from '@/errors'

function parseAttributePath(map: AttributeExpressionMap, path: string): string {
  const parts = path.split('.')

  return parts
    .map(part => {
      const match = part.match(/^(.+?)(\[\d+\])$/)
      if (match) {
        const attrName = match[1]!
        const index = match[2]!
        map.addName(attrName)
        return `#${attrName}${index}`
      }
      map.addName(part)
      return `#${part}`
    })
    .join('.')
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

export type UpdateParserResult = {
  updateExpression: string
  attributeExpressionMap: AttributeExpressionMap
}

export function parseUpdate(
  update: UpdateValues,
  attributeExpressionMap: AttributeExpressionMap = new AttributeExpressionMap(),
): UpdateParserResult {
  if (Object.keys(update).length === 0) {
    throw new DocumentBuilderError('Update expression cannot be empty')
  }

  const setExpressions: string[] = []
  const removeExpressions: string[] = []
  const addExpressions: string[] = []
  const deleteExpressions: string[] = []

  for (const [path, value] of Object.entries(update)) {
    const pathExpr = parseAttributePath(attributeExpressionMap, path)

    if (isValueReference(value)) {
      const refExpr = parseAttributePath(attributeExpressionMap, value.to)

      if (value.default !== undefined) {
        attributeExpressionMap.addValue(value.default)
        const defaultPlaceholder = attributeExpressionMap.getPlaceholderFromValue(value.default)!
        setExpressions.push(`${pathExpr} = if_not_exists(${refExpr}, ${defaultPlaceholder})`)
      } else {
        setExpressions.push(`${pathExpr} = ${refExpr}`)
      }
    } else if (isUpdateExpression(value)) {
      if (value.type === $remove) {
        removeExpressions.push(pathExpr)
      } else if (value.type === $delete) {
        if (isValueReference(value.values)) {
          const refExpr = parseAttributePath(attributeExpressionMap, value.values.to)
          deleteExpressions.push(`${pathExpr} ${refExpr}`)
        } else {
          const setValues = Array.isArray(value.values) ? new Set(value.values) : value.values
          attributeExpressionMap.addValue(setValues)
          const valuePlaceholder = attributeExpressionMap.getPlaceholderFromValue(setValues)!
          deleteExpressions.push(`${pathExpr} ${valuePlaceholder}`)
        }
      } else if (value.type === $add) {
        if ('op' in value && value.op === $addToSet) {
          if (isValueReference(value.values)) {
            const refExpr = parseAttributePath(attributeExpressionMap, value.values.to)
            addExpressions.push(`${pathExpr} ${refExpr}`)
          } else {
            const setValues = Array.isArray(value.values) ? new Set(value.values) : value.values
            attributeExpressionMap.addValue(setValues)
            const valuePlaceholder = attributeExpressionMap.getPlaceholderFromValue(setValues)!
            addExpressions.push(`${pathExpr} ${valuePlaceholder}`)
          }
        }
      } else if (value.type === $set) {
        if ('op' in value) {
          if (value.op === $add) {
            if (isValueReference(value.value)) {
              const refExpr = parseAttributePath(attributeExpressionMap, value.value.to)
              setExpressions.push(`${pathExpr} = ${pathExpr} + ${refExpr}`)
            } else {
              attributeExpressionMap.addValue(value.value)
              const valuePlaceholder = attributeExpressionMap.getPlaceholderFromValue(value.value)!
              setExpressions.push(`${pathExpr} = ${pathExpr} + ${valuePlaceholder}`)
            }
          } else if (value.op === $subtract) {
            if (isValueReference(value.value)) {
              const refExpr = parseAttributePath(attributeExpressionMap, value.value.to)
              setExpressions.push(`${pathExpr} = ${pathExpr} - ${refExpr}`)
            } else {
              attributeExpressionMap.addValue(value.value)
              const valuePlaceholder = attributeExpressionMap.getPlaceholderFromValue(value.value)!
              setExpressions.push(`${pathExpr} = ${pathExpr} - ${valuePlaceholder}`)
            }
          } else if (value.op === $append) {
            if (isValueReference(value.values)) {
              const refExpr = parseAttributePath(attributeExpressionMap, value.values.to)
              setExpressions.push(`${pathExpr} = list_append(${pathExpr}, ${refExpr})`)
            } else {
              attributeExpressionMap.addValue(value.values)
              const valuePlaceholder = attributeExpressionMap.getPlaceholderFromValue(value.values)!
              setExpressions.push(`${pathExpr} = list_append(${pathExpr}, ${valuePlaceholder})`)
            }
          } else if (value.op === $prepend) {
            if (isValueReference(value.values)) {
              const refExpr = parseAttributePath(attributeExpressionMap, value.values.to)
              setExpressions.push(`${pathExpr} = list_append(${refExpr}, ${pathExpr})`)
            } else {
              attributeExpressionMap.addValue(value.values)
              const valuePlaceholder = attributeExpressionMap.getPlaceholderFromValue(value.values)!
              setExpressions.push(`${pathExpr} = list_append(${valuePlaceholder}, ${pathExpr})`)
            }
          }
        }
      }
    } else {
      attributeExpressionMap.addValue(value)
      const valuePlaceholder = attributeExpressionMap.getPlaceholderFromValue(value)!
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

  const updateExpression = parts.join(' ')

  return {
    updateExpression,
    attributeExpressionMap,
  }
}
