# Conditions API

> An expressive API for building DynamoDB condition expressions. Used in conditional writes (`condition:`), query/scan filters (`filter:`), and sort key conditions (`sortKeyCondition:`). All operators are importable from `'dynamo-document-builder'`.

Conditions are plain objects mapping attribute paths to operator values. A direct value implies `equals`.

```typescript
{ status: 'active' }          // same as { status: equals('active') }
{ status: equals('active') }
```

## Comparison Operators

```typescript
import {
  equals, notEquals,
  greaterThan, greaterThanOrEqual,
  lessThan, lessThanOrEqual,
  between, isIn,
} from 'dynamo-document-builder';

{ status: equals('active') }
{ status: notEquals('cancelled') }
{ age: greaterThan(18) }
{ age: greaterThanOrEqual(21) }
{ age: lessThan(65) }
{ age: lessThanOrEqual(100) }
{ price: between(10, 100) }          // inclusive on both ends
{ status: isIn('pending', 'processing', 'shipped') }
```

## Logical Operators

```typescript
import { and, or, not } from 'dynamo-document-builder';

// AND — explicit
and(
  { total: greaterThan(50) },
  { total: lessThan(200) },
)

// AND — implicit via array
[
  { total: greaterThan(50) },
  { total: lessThan(200) },
]

// OR
or(
  { status: 'pending' },
  { status: 'processing' },
)

// NOT — negates the entire condition
not({ status: 'cancelled' })

// Nested combinations
and(
  { status: 'active' },
  not({ role: 'admin' }),
  or({ tier: 'pro' }, { tier: 'enterprise' }),
)
```

## String Operators

```typescript
import { beginsWith, contains } from 'dynamo-document-builder';

{ SK: beginsWith('USER#') }
{ title: contains('DynamoDB') }
{ tags: contains('urgent') }   // works on lists and sets too
```

`beginsWith` is the only operator valid in `sortKeyCondition`.

## Attribute Checks

```typescript
import { exists, notExists, size, typeIs } from 'dynamo-document-builder';

{ lastLogin: exists() }            // attribute must be present
{ middleName: notExists() }        // attribute must be absent

{ username: size(5) }              // string/binary length or set/list size equals 5
{ tags: size(greaterThan(2)) }     // size with a nested comparison operator

{ age: typeIs('N') }               // type check using DynamoDB type descriptors
{ isActive: typeIs('BOOL') }
```

### DynamoDB Type Descriptors

| Descriptor | Type |
|---|---|
| `S` | String |
| `N` | Number |
| `B` | Binary |
| `BOOL` | Boolean |
| `NULL` | Null |
| `M` | Map |
| `L` | List |
| `SS` | String Set |
| `NS` | Number Set |
| `BS` | Binary Set |
