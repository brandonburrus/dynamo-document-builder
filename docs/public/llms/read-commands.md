# Read Commands

> All read commands are dispatched via `entity.send(command)`. Results are validated against the entity's Zod schema by default.

## Get

Retrieve a single item by primary key. Returns `{ item: T | undefined }`.

```typescript
import { Get, ProjectedGet } from 'dynamo-document-builder';

const { item } = await todoEntity.send(new Get({
  key: { userId: '123', todoId: '456' },
  consistent: true, // optional: strongly consistent read
}));

// Projected get — returns a subset of attributes with a narrowed type
const { item } = await todoEntity.send(new ProjectedGet({
  key: { userId: '123', todoId: '456' },
  projection: ['title', 'isComplete'],
  projectionSchema: z.object({
    title: z.string(),
    isComplete: z.boolean(),
  }),
}));
```

**Options**: `key` (required), `consistent`, `skipValidation`, `returnConsumedCapacity`

**`ProjectedGet` additional options**: `projection` (required), `projectionSchema` (required)

## Query

Retrieve multiple items matching a partition key, with optional sort key conditions and filters. Returns `{ items: T[], count: number, lastEvaluatedKey }`.

```typescript
import { Query, ProjectedQuery } from 'dynamo-document-builder';
import { beginsWith, greaterThan, and } from 'dynamo-document-builder';

// Query by entity partition key
const { items, count, lastEvaluatedKey } = await todoEntity.send(new Query({
  key: { userId: '123' },
  sortKeyCondition: { SK: beginsWith('TODO#') },
  filter: and(
    { isComplete: false },
    { createdAt: greaterThan('2024-01-01') },
  ),
  limit: 10,
  reverseIndexScan: false,
  consistent: false,
}));

// Query a GSI
const { items } = await todoEntity.send(new Query({
  index: { StatusIndex: { status: 'in-progress' } },
  sortKeyCondition: { GSI1SK: beginsWith('2024') },
}));
```

**Options**: `key` or `index` (one required), `sortKeyCondition`, `filter`, `limit`, `pageSize`, `consistent`, `reverseIndexScan`, `exclusiveStartKey`, `validationConcurrency`, `skipValidation`, `returnConsumedCapacity`

### Pagination

Use `entity.paginate(query)` to iterate pages without manually tracking `lastEvaluatedKey`.

```typescript
for await (const page of todoEntity.paginate(new Query({
  key: { userId: '123' },
  pageSize: 50,
}))) {
  console.log(`Page: ${page.count} items`);
  processItems(page.items);
}
```

`pageSize` controls the DynamoDB `Limit` per request. `limit` caps total items returned across all pages.

## Scan

Read all items in a table or index. **Expensive** — prefer `Query` when possible. Filters do not reduce read capacity consumption.

```typescript
import { Scan, ProjectedScan } from 'dynamo-document-builder';

const { items, scannedCount } = await todoEntity.send(new Scan({
  filter: { isComplete: false },
  limit: 100,
  indexName: 'StatusIndex', // optional: scan a GSI or LSI
}));

// Parallel scan — split the table into segments processed concurrently
async function parallelScan(totalSegments: number) {
  const segments = Array.from({ length: totalSegments }, (_, i) =>
    todoEntity.paginate(new Scan({ segment: i, totalSegments }))
  );
  // Process each segment's async iterator concurrently
}
```

**Options**: `indexName`, `filter`, `limit`, `segment`, `totalSegments`, `consistent`, `pageSize`, `exclusiveStartKey`, `skipValidation`, `returnConsumedCapacity`

## BatchGet

Retrieve up to 100 items by primary key in one DynamoDB request. Unprocessed keys should be retried with exponential backoff.

```typescript
import { BatchGet, BatchProjectedGet } from 'dynamo-document-builder';

const { items, unprocessedKeys } = await todoEntity.send(new BatchGet({
  keys: [
    { userId: '123', todoId: '456' },
    { userId: '789', todoId: '101' },
  ],
  consistent: true,
}));

// Projected batch get
const { items } = await todoEntity.send(new BatchProjectedGet({
  keys: [{ userId: '123', todoId: '456' }],
  projection: ['title', 'isComplete'],
  projectionSchema: z.object({ title: z.string(), isComplete: z.boolean() }),
}));

if (unprocessedKeys?.length) {
  // Retry unprocessedKeys with exponential backoff
}
```

**`BatchGet` options**: `keys` (required), `consistent`, `skipValidation`, `returnConsumedCapacity`

**`BatchProjectedGet` options**: `keys` (required), `projection` (required), `projectionSchema` (required), `consistent`, `skipValidation`, `returnConsumedCapacity`

## TransactGet

Strongly consistent, all-or-nothing read of multiple items. Returns `{ items: (T | undefined)[] }` in the same order as the input keys.

```typescript
import { TransactGet } from 'dynamo-document-builder';

const { items } = await todoEntity.send(new TransactGet({
  keys: [
    { userId: '123', todoId: '456' },
    { userId: '789', todoId: '101' },
  ],
}));
// items[0] is undefined if the first key was not found
```

**Options**: `keys` (required), `skipValidation`, `returnConsumedCapacity`
