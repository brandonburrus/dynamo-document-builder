# Patterns, Best Practices & Troubleshooting

## Performance Best Practices

1. **Query over Scan** — Queries target a specific partition; scans read the entire table. Filters on scans do not reduce capacity consumption.
2. **Limit consistent reads** — Strongly consistent reads cost 2× read capacity. Use `consistent: true` only when stale data is unacceptable.
3. **Use projection expressions** — `ProjectedGet`, `ProjectedQuery`, `ProjectedScan`, and `BatchProjectedGet` reduce data transfer by returning only the attributes you need.
4. **Paginate large result sets** — Use `entity.paginate(query)` with a `pageSize` instead of fetching all items at once.
5. **Parallel scans** — For large full-table scans, split into `totalSegments` and process each segment concurrently.
6. **Batch operations** — Use `BatchGet` / `BatchWrite` instead of looping `Get` / `Put` to reduce round trips.
7. **Retry unprocessed items** — `BatchGet` and `BatchWrite` may return unprocessed items under heavy load. Retry with exponential backoff and jitter.

## Design Patterns

### Single Table Design

Keep all entity types in one table. Use composite keys (`PK` + `SK`) with prefixes to namespace entities and enable efficient access patterns.

```
PK: USER#<userId>    SK: PROFILE
PK: USER#<userId>    SK: TODO#<todoId>
PK: ORDER#<orderId>  SK: METADATA
```

### User with Email Lookup Index

```typescript
const userEntity = new DynamoEntity({
  table: myTable,
  schema: z.object({
    userId: z.string(),
    email: z.string().email(),
    name: z.string(),
    role: z.enum(['user', 'admin']),
    createdAt: z.iso.datetime(),
  }),
  partitionKey: user => key('USER', user.userId),
  sortKey: () => 'PROFILE',
  globalSecondaryIndexes: {
    EmailIndex: {
      partitionKey: user => key('EMAIL', user.email),
      sortKey: user => key('USER', user.userId),
    },
  },
});
```

### Hierarchical Data (Parent → Children)

Store children under the parent's partition key with a sort key that supports range queries.

```typescript
const commentEntity = new DynamoEntity({
  table: myTable,
  schema: z.object({
    postId: z.string(),
    commentId: z.string(),
    userId: z.string(),
    content: z.string(),
    createdAt: z.iso.datetime(),
  }),
  partitionKey: comment => key('POST', comment.postId),
  sortKey: comment => key('COMMENT', comment.createdAt, comment.commentId),
});

// Fetch all comments for a post, newest first
const { items } = await commentEntity.send(new Query({
  key: { postId: '123' },
  sortKeyCondition: { SK: beginsWith('COMMENT#') },
  reverseIndexScan: true,
}));
```

### Audit Trail

```typescript
const auditEntity = new DynamoEntity({
  table: myTable,
  schema: z.object({
    entityId: z.string(),
    action: z.enum(['create', 'update', 'delete']),
    userId: z.string(),
    timestamp: z.iso.datetime(),
    changes: z.record(z.any()),
  }),
  partitionKey: audit => key('ENTITY', audit.entityId),
  sortKey: audit => key('AUDIT', audit.timestamp),
});
```

### Lambda Best Practices

- Construct `DynamoTable` and `DynamoEntity` instances **outside** the handler function so they are reused across warm invocations.
- Read `tableName` from an environment variable to support multiple deployment environments.
- Import commands from subpath entries (e.g. `dynamo-document-builder/commands/get`) to reduce bundle size via tree-shaking.

```typescript
// Outside handler — constructed once per Lambda instance
const table = new DynamoTable({
  tableName: process.env.TABLE_NAME!,
  documentClient: DynamoDBDocumentClient.from(new DynamoDBClient()),
});

const todoEntity = new DynamoEntity({ table, schema: todoSchema, /* ... */ });

// Handler
export const handler = async (event) => {
  const { item } = await todoEntity.send(new Get({ key: { /* ... */ } }));
  // ...
};
```

## Troubleshooting

### Keys not being written

- Verify `partitionKey` and `sortKey` are defined on the entity.
- Key functions must be pure and return non-empty strings.
- Use `key()` helper to ensure consistent `#`-separated formatting.

### Validation errors on read

- Confirm the Zod schema matches the actual stored attribute names and types.
- Use `skipValidation: true` temporarily to inspect raw results (not for production).
- If using codecs, verify the `decode` function handles the stored representation correctly.

### TypeScript type errors

- Use `Entity<typeof myEntity>` to infer the domain type from an entity instance.
- Use `EncodedEntity<typeof myEntity>` when working with the raw stored representation (e.g. when `skipValidation: true`).
- Ensure the Zod schema's inferred type aligns with how you construct input items.

### Performance issues

- Replace `Scan` with `Query` by adding a GSI for the access pattern.
- Add projection expressions to high-traffic reads.
- Enable pagination; avoid fetching more data than needed.
- Check that `consistent: true` is only used where required.

## Quick Reference: All Imports

```typescript
// Core
import { DynamoTable, DynamoEntity, key, indexKey, type Entity, type EncodedEntity } from 'dynamo-document-builder';

// Read Commands
import { Get, ProjectedGet } from 'dynamo-document-builder';
import { Query, ProjectedQuery } from 'dynamo-document-builder';
import { Scan, ProjectedScan } from 'dynamo-document-builder';
import { BatchGet, BatchProjectedGet } from 'dynamo-document-builder';
import { TransactGet } from 'dynamo-document-builder';

// Write Commands
import { Put, ConditionalPut } from 'dynamo-document-builder';
import { Update, ConditionalUpdate } from 'dynamo-document-builder';
import { Delete, ConditionalDelete } from 'dynamo-document-builder';
import { BatchWrite } from 'dynamo-document-builder';
import { TransactWrite, ConditionCheck } from 'dynamo-document-builder';

// Multi-Entity Commands (use with table.send())
import { TableBatchGet, TableBatchWrite, TableTransactGet, TableTransactWrite } from 'dynamo-document-builder';

// Conditions
import {
  equals, notEquals,
  greaterThan, greaterThanOrEqual,
  lessThan, lessThanOrEqual,
  between, isIn,
  and, or, not,
  beginsWith, contains,
  exists, notExists, size, typeIs,
} from 'dynamo-document-builder';

// Update Operations
import { ref, remove, add, subtract, append, prepend, addToSet, removeFromSet } from 'dynamo-document-builder';
```
