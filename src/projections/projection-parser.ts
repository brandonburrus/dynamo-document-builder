import { AttributeExpressionMap } from '@/attributes/attribute-map'
import type { Projection } from '@/projections/projection-types'

export type ProjectionResult = {
  projectionExpression: string
  attributeExpressionMap: AttributeExpressionMap
}

export function parseProjection(
  projection: Projection,
  attributeExpressionMap: AttributeExpressionMap = new AttributeExpressionMap(),
): ProjectionResult {
  const seen = new Set<string>()
  const projectionExpression = projection
    .filter(attribute => {
      if (seen.has(attribute)) return false
      seen.add(attribute)
      return true
    })
    .map(attribute => {
      const parts = attribute.split('.')
      return parts
        .map(part => {
          const match = part.match(/^([^[]+)(\[.+\])$/)
          if (match) {
            const [, name, index] = match
            return attributeExpressionMap.addName(name) + index
          }
          return attributeExpressionMap.addName(part)
        })
        .join('.')
    })
    .join(', ')

  return {
    projectionExpression,
    attributeExpressionMap,
  }
}
