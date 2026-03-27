# Multi-Entity Commands

> Table-level commands operate across multiple entity types in a single DynamoDB request. They are dispatched via `table.send()`. Use `entity.prepare(command)` to bind a command to an entity before passing it to a table command.

All entities involved in a multi-entity command must belong to the same table. A `DocumentBuilderError` is thrown at runtime if they do not.

## entity.prepare()

`entity.prepare(command)` binds a single command or array of commands to an entity without executing them, producing a typed group accepted by table-level commands.

```typescript
// Single command
todoEntity.prepare(new BatchGet({ keys: [...] }))

// Array of commands (used with TableTransactWrite)
todoEntity.prepare([
  new Put({ item: { /* ... */ } }),
  new Delete({ key: { /* ... */ } }),
])
```

## TableBatchGet

Batch get across multiple entity types. Results are typed tuples in the same order as the input groups.

```typescript
import { TableBatchGet, BatchGet } from 'dynamo-document-builder';

const { items, unprocessedKeys } = await myTable.send(
  new TableBatchGet({
    consistent: true, // command-level: overrides all group-level consistent settings
    gets: [
      userEntity.prepare(new BatchGet({ keys: [{ userId: '1' }] })),
      orderEntity.prepare(new BatchGet({ keys: [{ orderId: 'o1' }, { orderId: 'o2' }] })),
    ],
  }),
);

const [users, orders] = items;
// users: User[]
// orders: Order[]

const [userUnprocessed, orderUnprocessed] = unprocessedKeys ?? [[], []];
```

**Consistency semantics**:
- `consistent: true` on the command → forces `ConsistentRead: true` for all groups
- `consistent: false` on the command → forces `ConsistentRead: false` even if a group sets `consistent: true`
- Not set on the command → per-group logic; if any group has `consistent: true`, the entire request uses it

**Note**: DynamoDB does not guarantee order in batch get responses. Document Builder matches items back to their group by primary key. Unprocessed items are returned as original domain objects, not raw DynamoDB items.

**Options**: `gets` (required), `consistent`, `skipValidation`, `returnConsumedCapacity`

## TableBatchWrite

Batch write (puts and deletes) across multiple entity types.

```typescript
import { TableBatchWrite, BatchWrite } from 'dynamo-document-builder';

const { unprocessedPuts, unprocessedDeletes } = await myTable.send(
  new TableBatchWrite({
    writes: [
      userEntity.prepare(new BatchWrite({
        items: [{ userId: '1', name: 'Alice', /* ... */ }],
        deletes: [{ userId: '2' }],
      })),
      orderEntity.prepare(new BatchWrite({
        items: [{ orderId: 'o1', status: 'pending', total: 99 }],
      })),
    ],
  }),
);

const [userUnprocessedPuts, orderUnprocessedPuts] = unprocessedPuts;
// userUnprocessedPuts: User[] | undefined
// orderUnprocessedPuts: Order[] | undefined

const [userUnprocessedDeletes, orderUnprocessedDeletes] = unprocessedDeletes;
```

**Options**: `writes` (required), `returnItemCollectionMetrics`, `skipValidation`, `returnConsumedCapacity`

## TableTransactGet

Strongly consistent, all-or-nothing read across multiple entity types. Items within each group are in the same order as the input keys. `undefined` for keys where no item was found.

```typescript
import { TableTransactGet, TransactGet } from 'dynamo-document-builder';

const { items } = await myTable.send(
  new TableTransactGet({
    gets: [
      userEntity.prepare(new TransactGet({ keys: [{ userId: '1' }, { userId: '2' }] })),
      orderEntity.prepare(new TransactGet({ keys: [{ orderId: 'o1' }] })),
    ],
  }),
);

const [users, orders] = items;
// users: (User | undefined)[]
// orders: (Order | undefined)[]
```

**Options**: `gets` (required), `skipValidation`, `returnConsumedCapacity`

## TableTransactWrite

Atomic, all-or-nothing write across multiple entity types. Each entity group accepts an array of write commands.

```typescript
import { TableTransactWrite, Put, Update, Delete } from 'dynamo-document-builder';

await myTable.send(
  new TableTransactWrite({
    transactions: [
      userEntity.prepare([
        new Put({ item: { userId: '1', name: 'Alice' } }),
        new Delete({ key: { userId: '2' } }),
      ]),
      orderEntity.prepare([
        new Update({
          key: { orderId: 'o1' },
          updates: { status: 'shipped' },
        }),
      ]),
    ],
    idempotencyToken: 'unique-token', // optional
  }),
);
```

**Options**: `transactions` (required), `idempotencyToken`, `returnItemCollectionMetrics`, `skipValidation`, `returnConsumedCapacity`

## Tree-Shakable Imports

```typescript
import { TableBatchGet }     from 'dynamo-document-builder/commands/table-batch-get';
import { TableBatchWrite }   from 'dynamo-document-builder/commands/table-batch-write';
import { TableTransactGet }  from 'dynamo-document-builder/commands/table-transact-get';
import { TableTransactWrite } from 'dynamo-document-builder/commands/table-transact-write';
```
