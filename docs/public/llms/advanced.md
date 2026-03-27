# Advanced Features

## Zod Codecs

Document Builder supports Zod 4.1+ codecs for custom serialization/deserialization. Codecs let you work with richer types in application code (e.g. `Date`) while storing a simpler representation in DynamoDB (e.g. ISO string).

```typescript
import { z } from 'zod';
import { DynamoEntity, Put, Get } from 'dynamo-document-builder';

const stringToDate = z.codec(
  z.iso.datetime(),  // stored type (what DynamoDB holds)
  z.date(),          // domain type (what your code works with)
  {
    encode: (date: Date) => date.toISOString(),
    decode: (iso: string) => new Date(iso),
  },
);

const eventEntity = new DynamoEntity({
  table: myTable,
  schema: z.object({
    id: z.string(),
    name: z.string(),
    eventDate: stringToDate,
  }),
  partitionKey: event => key('EVENT', event.id),
  sortKey: () => 'METADATA',
});

// Write: pass a Date, codec encodes it to ISO string before writing
await eventEntity.send(new Put({
  item: { id: '1', name: 'Conference', eventDate: new Date(2025, 0, 15) },
}));

// Read: DynamoDB string is decoded back to a Date object
const { item } = await eventEntity.send(new Get({ key: { id: '1' } }));
// item.eventDate is a Date instance
```

**Warning**: `skipValidation: true` bypasses both schema validation and codec encoding/decoding. Use `EncodedEntity<T>` when you need the raw stored types.

## Low-Level Parsers

For advanced use cases where you need to build DynamoDB expressions directly, Document Builder exposes its internal parsers.

```typescript
import { parseCondition } from 'dynamo-document-builder/conditions/condition-parser';
import { parseUpdate }    from 'dynamo-document-builder/updates/update-parser';
import { parseProjection } from 'dynamo-document-builder/projections/projection-parser';
import { AttributeExpressionMap } from 'dynamo-document-builder/attributes/attribute-map';

// Parse a condition to a DynamoDB ConditionExpression string
const { conditionExpression, attributeExpressionMap } = parseCondition({
  age: greaterThan(21),
  status: 'active',
});

// Parse update operations to a DynamoDB UpdateExpression string
const { updateExpression, attributeExpressionMap } = parseUpdate({
  value: 42,
  'nested.attribute': 'newValue',
  counter: add(1),
});

// Parse a projection list to a DynamoDB ProjectionExpression string
const { projectionExpression, attributeExpressionMap } = parseProjection([
  'name', 'age', 'address.city',
]);

// Share an AttributeExpressionMap across multiple parsers to avoid token collisions
const map = new AttributeExpressionMap();
const { conditionExpression } = parseCondition(condition, map);
const { updateExpression }    = parseUpdate(updates, map);

const { ExpressionAttributeNames, ExpressionAttributeValues } =
  map.toDynamoAttributeExpression();
```

## AttributeExpressionMap

Manages the `ExpressionAttributeNames` and `ExpressionAttributeValues` maps required by all DynamoDB expression APIs, ensuring placeholder tokens are unique and collision-free.

```typescript
import { AttributeExpressionMap } from 'dynamo-document-builder/attributes/attribute-map';

const map = new AttributeExpressionMap();

// Add a name/value pair — returns [namePlaceholder, valuePlaceholder]
const [nameToken, valueToken] = map.add('status', 'active');
// nameToken:  '#status'
// valueToken: ':v1'

// Add name or value independently
const nameToken  = map.addName('age');   // '#age'
const valueToken = map.addValue(30);     // ':v2'

// Existence checks
map.hasName('status');   // true
map.hasValue(30);        // true

// Forward lookups (attribute → placeholder)
map.getPlaceholderFromName('status');   // '#status'
map.getPlaceholderFromValue('active');  // ':v1'

// Reverse lookups (placeholder → attribute)
map.getNameFromPlaceholder('#status');  // 'status'
map.getValueFromPlaceholder(':v1');     // 'active'

// Counts
map.getNameCount();   // number of unique attribute names
map.getValueCount();  // number of unique values

// Export to DynamoDB SDK format
map.toDynamoAttributeNames();      // { '#status': 'status', ... }
map.toDynamoAttributeValues();     // { ':v1': 'active', ... }
map.toDynamoAttributeExpression(); // both combined
```

## Tree-Shakable Imports

Every export has a dedicated subpath entry point so bundlers can eliminate unused code.

```typescript
// Core
import { DynamoTable }  from 'dynamo-document-builder/core/table';
import { DynamoEntity } from 'dynamo-document-builder/core/entity';

// Read commands
import { Get }               from 'dynamo-document-builder/commands/get';
import { ProjectedGet }      from 'dynamo-document-builder/commands/projected-get';
import { Query }             from 'dynamo-document-builder/commands/query';
import { ProjectedQuery }    from 'dynamo-document-builder/commands/projected-query';
import { Scan }              from 'dynamo-document-builder/commands/scan';
import { ProjectedScan }     from 'dynamo-document-builder/commands/projected-scan';
import { BatchGet }          from 'dynamo-document-builder/commands/batch-get';
import { BatchProjectedGet } from 'dynamo-document-builder/commands/batch-projected-get';
import { TransactGet }       from 'dynamo-document-builder/commands/transact-get';

// Write commands
import { Put }              from 'dynamo-document-builder/commands/put';
import { ConditionalPut }   from 'dynamo-document-builder/commands/conditional-put';
import { Update }           from 'dynamo-document-builder/commands/update';
import { ConditionalUpdate } from 'dynamo-document-builder/commands/conditional-update';
import { Delete }           from 'dynamo-document-builder/commands/delete';
import { ConditionalDelete } from 'dynamo-document-builder/commands/conditional-delete';
import { BatchWrite }       from 'dynamo-document-builder/commands/batch-write';
import { TransactWrite }    from 'dynamo-document-builder/commands/transact-write';

// Multi-entity commands
import { TableBatchGet }      from 'dynamo-document-builder/commands/table-batch-get';
import { TableBatchWrite }    from 'dynamo-document-builder/commands/table-batch-write';
import { TableTransactGet }   from 'dynamo-document-builder/commands/table-transact-get';
import { TableTransactWrite } from 'dynamo-document-builder/commands/table-transact-write';

// Conditions
import { equals }            from 'dynamo-document-builder/conditions/equals';
import { notEquals }         from 'dynamo-document-builder/conditions/not-equals';
import { greaterThan }       from 'dynamo-document-builder/conditions/greater-than';
import { greaterThanOrEqual } from 'dynamo-document-builder/conditions/greater-than-or-equal';
import { lessThan }          from 'dynamo-document-builder/conditions/less-than';
import { lessThanOrEqual }   from 'dynamo-document-builder/conditions/less-than-or-equal';
import { between }           from 'dynamo-document-builder/conditions/between';
import { isIn }              from 'dynamo-document-builder/conditions/is-in';
import { and }               from 'dynamo-document-builder/conditions/and';
import { or }                from 'dynamo-document-builder/conditions/or';
import { not }               from 'dynamo-document-builder/conditions/not';
import { beginsWith }        from 'dynamo-document-builder/conditions/begins-with';
import { contains }          from 'dynamo-document-builder/conditions/contains';
import { exists }            from 'dynamo-document-builder/conditions/exists';
import { notExists }         from 'dynamo-document-builder/conditions/not-exists';
import { size }              from 'dynamo-document-builder/conditions/size';
import { typeIs }            from 'dynamo-document-builder/conditions/type-is';

// Update operations
import { ref }           from 'dynamo-document-builder/updates/ref';
import { remove }        from 'dynamo-document-builder/updates/remove';
import { add }           from 'dynamo-document-builder/updates/add';
import { subtract }      from 'dynamo-document-builder/updates/subtract';
import { append }        from 'dynamo-document-builder/updates/append';
import { prepend }       from 'dynamo-document-builder/updates/prepend';
import { addToSet }      from 'dynamo-document-builder/updates/add-to-set';
import { removeFromSet } from 'dynamo-document-builder/updates/remove-from-set';
```

The main `'dynamo-document-builder'` barrel export re-exports everything above for convenience.
