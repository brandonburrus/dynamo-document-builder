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
} from '@/conditions/condition-symbols'
import { buildAttributeExpression, type DynamoAttributeExpression } from '@/attributes'

export interface DynamoConditionExpression extends DynamoAttributeExpression {
  ConditionExpression: string
}

export class InvalidDynamoDBConditionExpressionError extends Error {
  constructor(message: string) {
    super(`Invalid DynamoDB Condition Expression: ${message}`)
    this.name = 'InvalidDynamoDBConditionExpressionError'
  }
}

interface BuildContext {
  names: Set<string>
  values: Set<NativeAttributeValue>
  valuePlaceholders: Array<[NativeAttributeValue, string]>
  valueCounters: Map<string, number>
}

function createContext(): BuildContext {
  return {
    names: new Set(),
    values: new Set(),
    valuePlaceholders: [],
    valueCounters: new Map(),
  }
}

function getNamePlaceholder(ctx: BuildContext, name: string): string {
  ctx.names.add(name)
  return `#${name}`
}

function getValuePlaceholder(
  ctx: BuildContext,
  value: NativeAttributeValue,
  name?: string,
): string {
  const baseName = name || 'value'

  // Check if this exact value with this base name already exists
  const existingWithSameBase = ctx.valuePlaceholders.find(
    ([val, placeholder]) => val === value && placeholder.startsWith(`:${baseName}`),
  )

  if (existingWithSameBase) {
    return existingWithSameBase[1]
  }

  // Get the current counter for this base name
  const currentCount = ctx.valueCounters.get(baseName) || 0

  // Create the placeholder
  const placeholder = currentCount === 0 ? `:${baseName}` : `:${baseName}${currentCount}`

  // Update counters and maps
  ctx.valueCounters.set(baseName, currentCount + 1)
  ctx.valuePlaceholders.push([value, placeholder])
  ctx.values.add(value)

  return placeholder
}

function isSizeExpression(value: unknown): value is SizeExpression {
  return typeof value === 'object' && value !== null && 'type' in value && value.type === $size
}

function parseSizeExpression(ctx: BuildContext, expr: SizeExpression): string {
  const namePath = parseAttributePath(ctx, expr.attribute)
  return `size(${namePath})`
}

function parseAttributePath(ctx: BuildContext, path: string): string {
  const parts = path.split('.')
  return parts.map(part => getNamePlaceholder(ctx, part)).join('.')
}

function parseValueExpression(ctx: BuildContext, value: ValueExpression): string {
  if (isSizeExpression(value)) {
    return parseSizeExpression(ctx, value)
  }
  return getValuePlaceholder(ctx, value)
}

function parseComparisonExpression(ctx: BuildContext, expr: ComparisonExpression): string {
  const operand = parseAttributePath(ctx, expr.operand)
  const operandParts = expr.operand.split('.')
  const valueName = operandParts[operandParts.length - 1]

  let valuePlaceholder: string
  if (isSizeExpression(expr.value)) {
    valuePlaceholder = parseSizeExpression(ctx, expr.value)
  } else {
    valuePlaceholder = getValuePlaceholder(ctx, expr.value, valueName)
  }

  return `${operand} ${expr.operator} ${valuePlaceholder}`
}

function parseLogicalExpression(ctx: BuildContext, expr: LogicalExpression): string {
  if (expr.subConditions.length === 0) {
    throw new InvalidDynamoDBConditionExpressionError(
      'Logical expression must have at least one sub-condition',
    )
  }
  const conditions = expr.subConditions.map(c => {
    let parsed: string
    if (isConditionTemplate(c)) {
      parsed = parseConditionTemplate(ctx, c as ConditionTemplate)
      // Only wrap templates with multiple conditions in parentheses
      const templateKeys = Object.keys(c as ConditionTemplate)
      if (templateKeys.length > 1) {
        return `(${parsed})`
      }
      return parsed
    } else {
      parsed = parseConditionExpression(ctx, c as ConditionExpression)
      // Wrap logical expressions in parentheses to preserve precedence
      if ((c as ConditionExpression).type === $logical) {
        return `(${parsed})`
      }
      return parsed
    }
  })
  return conditions.join(` ${expr.operator} `)
}

function parseBetweenExpression(ctx: BuildContext, expr: BetweenExpression): string {
  const operand = parseAttributePath(ctx, expr.operand)
  const operandParts = expr.operand.split('.')
  const valueName = operandParts[operandParts.length - 1]

  const lower = isSizeExpression(expr.lowerValue)
    ? parseSizeExpression(ctx, expr.lowerValue)
    : getValuePlaceholder(ctx, expr.lowerValue, valueName)

  const upper = isSizeExpression(expr.upperValue)
    ? parseSizeExpression(ctx, expr.upperValue)
    : getValuePlaceholder(ctx, expr.upperValue, valueName)

  return `${operand} BETWEEN ${lower} AND ${upper}`
}

function parseInExpression(ctx: BuildContext, expr: InExpression): string {
  if (expr.values.length === 0) {
    throw new InvalidDynamoDBConditionExpressionError('IN expression must have at least one value')
  }
  const operand = parseAttributePath(ctx, expr.operand)
  const operandParts = expr.operand.split('.')
  const valueName = operandParts[operandParts.length - 1]

  const values = expr.values
    .map(v =>
      isSizeExpression(v) ? parseSizeExpression(ctx, v) : getValuePlaceholder(ctx, v, valueName),
    )
    .join(', ')

  return `${operand} IN (${values})`
}

function parseNotExpression(ctx: BuildContext, expr: NotExpression): string {
  let condition: string
  if (isConditionTemplate(expr.condition)) {
    condition = parseConditionTemplate(ctx, expr.condition as ConditionTemplate)
  } else {
    condition = parseConditionExpression(ctx, expr.condition as ConditionExpression)
  }
  return `NOT (${condition})`
}

function parseExistsExpression(ctx: BuildContext, expr: ExistsExpression): string {
  const operand = parseAttributePath(ctx, expr.operand)
  if (expr.not) {
    return `attribute_not_exists(${operand})`
  }
  return `attribute_exists(${operand})`
}

function parseTypeCheckExpression(ctx: BuildContext, expr: TypeCheckExpression): string {
  const operand = parseAttributePath(ctx, expr.operand)
  const operandParts = expr.operand.split('.')
  const valueName = operandParts[operandParts.length - 1]
  const typeValue = getValuePlaceholder(ctx, expr.attributeType, valueName)
  return `attribute_type(${operand}, ${typeValue})`
}

function parseBeginsWithExpression(ctx: BuildContext, expr: BeginsWithExpression): string {
  const operand = parseAttributePath(ctx, expr.operand)
  const operandParts = expr.operand.split('.')
  const valueName = operandParts[operandParts.length - 1]
  const prefix = getValuePlaceholder(ctx, expr.prefix, valueName)
  return `begins_with(${operand}, ${prefix})`
}

function parseContainsExpression(ctx: BuildContext, expr: ContainsExpression): string {
  const operand = parseAttributePath(ctx, expr.operand)
  const operandParts = expr.operand.split('.')
  const valueName = operandParts[operandParts.length - 1]
  const value = getValuePlaceholder(ctx, expr.substringOrElement, valueName)
  return `contains(${operand}, ${value})`
}

function parseConditionExpression(ctx: BuildContext, expr: ConditionExpression): string {
  if (!('type' in expr)) {
    throw new InvalidDynamoDBConditionExpressionError('Unknown condition expression')
  }

  switch (expr.type) {
    case $comparison:
      return parseComparisonExpression(ctx, expr as ComparisonExpression)
    case $logical:
      return parseLogicalExpression(ctx, expr as LogicalExpression)
    case $between:
      return parseBetweenExpression(ctx, expr as BetweenExpression)
    case $in:
      return parseInExpression(ctx, expr as InExpression)
    case $not:
      return parseNotExpression(ctx, expr as NotExpression)
    case $exists:
      return parseExistsExpression(ctx, expr as ExistsExpression)
    case $type:
      return parseTypeCheckExpression(ctx, expr as TypeCheckExpression)
    case $beginsWith:
      return parseBeginsWithExpression(ctx, expr as BeginsWithExpression)
    case $contains:
      return parseContainsExpression(ctx, expr as ContainsExpression)
    default:
      throw new InvalidDynamoDBConditionExpressionError('Unknown expression type')
  }
}

function isConditionTemplate(value: unknown): value is ConditionTemplate {
  if (typeof value !== 'object' || value === null) {
    return false
  }
  if ('type' in value && typeof value.type === 'symbol') {
    return false
  }
  return true
}

function parseConditionTemplate(ctx: BuildContext, template: ConditionTemplate): string {
  const conditions: string[] = []

  for (const [operand, value] of Object.entries(template)) {
    if (typeof value === 'object' && value !== null && 'type' in value) {
      const expr = { ...value, operand } as ConditionExpression
      conditions.push(parseConditionExpression(ctx, expr))
    } else {
      const namePlaceholder = parseAttributePath(ctx, operand)
      // For simple values in templates, use the last part of the operand path as the value key
      const operandParts = operand.split('.')
      const valueKey = operandParts[operandParts.length - 1]
      const valuePlaceholder = isSizeExpression(value)
        ? parseValueExpression(ctx, value as ValueExpression)
        : getValuePlaceholder(ctx, value as NativeAttributeValue, valueKey)
      conditions.push(`${namePlaceholder} = ${valuePlaceholder}`)
    }
  }

  return conditions.join(' AND ')
}

export function parseCondition(condition: Condition): DynamoConditionExpression {
  const ctx = createContext()
  let conditionExpression: string

  if (Array.isArray(condition)) {
    const expressions = condition.map(c => {
      if (isConditionTemplate(c)) {
        return parseConditionTemplate(ctx, c)
      }
      return parseConditionExpression(ctx, c as ConditionExpression)
    })
    conditionExpression = expressions.join(' AND ')
  } else if (isConditionTemplate(condition)) {
    conditionExpression = parseConditionTemplate(ctx, condition)
  } else {
    conditionExpression = parseConditionExpression(ctx, condition as ConditionExpression)
  }

  // Convert Sets to Records for buildAttributeExpression
  const names: Record<string, string> = {}
  for (const name of ctx.names) {
    names[`#${name}`] = name
  }

  const values: Record<string, NativeAttributeValue> = {}
  for (const [value, placeholder] of ctx.valuePlaceholders) {
    values[placeholder] = value
  }

  return {
    ConditionExpression: conditionExpression,
    ...buildAttributeExpression({
      names,
      values,
    }),
  }
}
