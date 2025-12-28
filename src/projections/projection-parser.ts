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
  const projectionExpression = projection
    .map(attribute => {
      const parts = attribute.split('.')
      return parts.map(part => attributeExpressionMap.addName(part)).join('.')
    })
    .join(', ')

  return {
    projectionExpression,
    attributeExpressionMap,
  }
}
