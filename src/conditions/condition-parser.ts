import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'
import type {
  BeginsWithExpression,
  BetweenExpression,
  ComparisonExpression,
  Condition,
  ConditionExpression,
  ConditionTemplate,
  ContainsExpression,
  ExistsExpression,
  InExpression,
  LogicalExpression,
  NotExpression,
  SizeExpression,
  TypeCheckExpression,
  ValueExpression,
} from '@/conditions/condition-types'
import {
  $beginsWith,
  $between,
  $comparison,
  $contains,
  $exists,
  $in,
  $logical,
  $not,
  $size,
  $type,
  isConditionSymbol,
} from '@/conditions/condition-symbols'
import { AttributeExpressionMap } from '@/attributes/attribute-map'
import { DocumentBuilderError } from '@/errors'

function isSizeExpression(value: unknown): value is SizeExpression {
  return typeof value === 'object' && value !== null && 'type' in value && value.type === $size
}

function parseSizeExpression(map: AttributeExpressionMap, expr: SizeExpression): string {
  const namePath = parseAttributePath(map, expr.attribute)
  return `size(${namePath})`
}

function parseAttributePath(map: AttributeExpressionMap, path: string): string {
  const parts = path.split('.')
  return parts.map(part => map.addName(part)).join('.')
}

function parseValueExpression(map: AttributeExpressionMap, value: ValueExpression): string {
  if (isSizeExpression(value)) {
    return parseSizeExpression(map, value)
  }
  return map.addValue(value)
}

function parseComparisonExpression(
  map: AttributeExpressionMap,
  expr: ComparisonExpression,
): string {
  const operand = parseAttributePath(map, expr.operand)
  const value: string = parseValueExpression(map, expr.value)
  return `${operand} ${expr.operator} ${value}`
}

function parseLogicalExpression(map: AttributeExpressionMap, expr: LogicalExpression): string {
  if (expr.subConditions.length < 1) {
    throw new DocumentBuilderError(
      'Invalid Condition: Logical expression must have at least one sub-condition',
    )
  }
  const conditions = expr.subConditions.map(c => {
    let parsed: string
    if (isConditionTemplate(c)) {
      parsed = parseConditionTemplate(map, c as ConditionTemplate)
      // Only wrap templates with multiple conditions in parentheses
      const templateKeys = Object.keys(c as ConditionTemplate)
      if (templateKeys.length > 1) {
        return `(${parsed})`
      }
      return parsed
    } else {
      parsed = parseConditionExpression(map, c as ConditionExpression)
      // Wrap logical expressions in parentheses to preserve precedence
      if ((c as ConditionExpression).type === $logical) {
        return `(${parsed})`
      }
      return parsed
    }
  })
  return conditions.join(` ${expr.operator} `)
}

function parseBetweenExpression(map: AttributeExpressionMap, expr: BetweenExpression): string {
  const operand = parseAttributePath(map, expr.operand)

  const lower = isSizeExpression(expr.lowerValue)
    ? parseSizeExpression(map, expr.lowerValue)
    : parseValueExpression(map, expr.lowerValue)

  const upper = isSizeExpression(expr.upperValue)
    ? parseSizeExpression(map, expr.upperValue)
    : parseValueExpression(map, expr.upperValue)

  return `${operand} BETWEEN ${lower} AND ${upper}`
}

function parseInExpression(map: AttributeExpressionMap, expr: InExpression): string {
  if (expr.values.length < 1) {
    throw new DocumentBuilderError('InvalidCondition: IN expression must have at least one value')
  }
  const operand = parseAttributePath(map, expr.operand)

  const values = expr.values.map(v => parseValueExpression(map, v)).join(', ')

  return `${operand} IN (${values})`
}

function parseNotExpression(map: AttributeExpressionMap, expr: NotExpression): string {
  let condition: string
  if (isConditionTemplate(expr.condition)) {
    condition = parseConditionTemplate(map, expr.condition as ConditionTemplate)
  } else {
    condition = parseConditionExpression(map, expr.condition as ConditionExpression)
  }
  return `NOT (${condition})`
}

function parseExistsExpression(map: AttributeExpressionMap, expr: ExistsExpression): string {
  const operand = parseAttributePath(map, expr.operand)
  if (expr.not) {
    return `attribute_not_exists(${operand})`
  }
  return `attribute_exists(${operand})`
}

function parseTypeCheckExpression(map: AttributeExpressionMap, expr: TypeCheckExpression): string {
  const operand = parseAttributePath(map, expr.operand)
  const typeValue = parseValueExpression(map, expr.attributeType)
  return `attribute_type(${operand}, ${typeValue})`
}

function parseBeginsWithExpression(
  map: AttributeExpressionMap,
  expr: BeginsWithExpression,
): string {
  const operand = parseAttributePath(map, expr.operand)
  const prefix = parseValueExpression(map, expr.prefix)
  return `begins_with(${operand}, ${prefix})`
}

function parseContainsExpression(map: AttributeExpressionMap, expr: ContainsExpression): string {
  const operand = parseAttributePath(map, expr.operand)
  const value = parseValueExpression(map, expr.substringOrElement)
  return `contains(${operand}, ${value})`
}

function parseConditionExpression(map: AttributeExpressionMap, expr: ConditionExpression): string {
  if (!('type' in expr)) {
    throw new DocumentBuilderError('Unknown condition expression')
  }

  switch (expr.type) {
    case $comparison:
      return parseComparisonExpression(map, expr as ComparisonExpression)
    case $logical:
      return parseLogicalExpression(map, expr as LogicalExpression)
    case $between:
      return parseBetweenExpression(map, expr as BetweenExpression)
    case $in:
      return parseInExpression(map, expr as InExpression)
    case $not:
      return parseNotExpression(map, expr as NotExpression)
    case $exists:
      return parseExistsExpression(map, expr as ExistsExpression)
    case $type:
      return parseTypeCheckExpression(map, expr as TypeCheckExpression)
    case $beginsWith:
      return parseBeginsWithExpression(map, expr as BeginsWithExpression)
    case $contains:
      return parseContainsExpression(map, expr as ContainsExpression)
    default:
      throw new DocumentBuilderError('Unknown expression type')
  }
}

function isConditionTemplate(value: unknown): value is ConditionTemplate {
  if (typeof value !== 'object' || value === null) {
    return false
  }
  if ('type' in value && typeof value.type === 'symbol') {
    return !isConditionSymbol(value.type)
  }
  return true
}

function isConditionExpression(value: unknown): value is ConditionExpression {
  if (typeof value !== 'object' || value === null) {
    return false
  }
  return 'type' in value && typeof value.type === 'symbol' && isConditionSymbol(value.type)
}

function parseConditionTemplate(map: AttributeExpressionMap, template: ConditionTemplate): string {
  const conditions: string[] = []

  for (const [operand, value] of Object.entries(template)) {
    if (typeof value === 'object' && value !== null && 'type' in value) {
      const expr = { ...value, operand } as ConditionExpression
      conditions.push(parseConditionExpression(map, expr))
    } else {
      const namePlaceholder = parseAttributePath(map, operand)
      const valuePlaceholder = isSizeExpression(value)
        ? parseValueExpression(map, value as ValueExpression)
        : parseValueExpression(map, value as NativeAttributeValue)
      conditions.push(`${namePlaceholder} = ${valuePlaceholder}`)
    }
  }

  return conditions.join(' AND ')
}

export type ConditionParserResult = {
  conditionExpression: string
  attributeExpressionMap: AttributeExpressionMap
}

export function parseCondition(
  condition: Condition,
  attributeExpressionMap: AttributeExpressionMap = new AttributeExpressionMap(),
): ConditionParserResult {
  let conditionExpression: string

  if (Array.isArray(condition)) {
    const expressions = condition.map(c => {
      if (isConditionTemplate(c)) {
        return parseConditionTemplate(attributeExpressionMap, c)
      } else if (isConditionExpression(c)) {
        return parseConditionExpression(attributeExpressionMap, c)
      }
      throw new DocumentBuilderError('Invalid condition')
    })
    conditionExpression = expressions.join(' AND ')
  } else if (isConditionTemplate(condition)) {
    conditionExpression = parseConditionTemplate(attributeExpressionMap, condition)
  } else if (isConditionExpression(condition)) {
    conditionExpression = parseConditionExpression(attributeExpressionMap, condition)
  } else {
    throw new DocumentBuilderError('Invalid condition')
  }

  return {
    conditionExpression,
    attributeExpressionMap,
  }
}
