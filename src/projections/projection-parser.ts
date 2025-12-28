import { AttributeExpressionMap } from '@/attributes/attribute-map'

export type Projection = string[]

export interface ProjectionResult {
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
