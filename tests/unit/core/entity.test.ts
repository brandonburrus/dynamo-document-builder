import { describe, it, expect } from 'vitest'
import { DynamoEntity, DynamoTable } from '@/core'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import { z } from 'zod'
import type { BaseCommand, BasePaginatable } from '@/commands'

describe('DynamoEntity', () => {
  const dynamoClient = new DynamoDBClient()
  const documentClient = DynamoDBDocumentClient.from(dynamoClient)

  const testTable = new DynamoTable({
    tableName: 'TestTable',
    documentClient,
    keyNames: {
      partitionKey: 'PK',
      sortKey: 'SK',
      globalSecondaryIndexes: {
        GSI1: {
          partitionKey: 'GSI1PK',
          sortKey: 'GSI1SK',
        },
      },
      localSecondaryIndexes: {
        LSI1: {
          sortKey: 'LSI1SK',
        },
      },
    },
  })

  it('should instantiate', () => {
    const entity = new DynamoEntity({
      table: testTable,
      schema: z.object({
        id: z.string(),
        type: z.string(),
      }),
    })

    expect(entity).toBeDefined()
    expect(entity.table).toBe(testTable)
    expect(entity.schema).toBeDefined()
  })

  it('should provide access to its parent table', () => {
    const entity = new DynamoEntity({
      table: testTable,
      schema: z.object({}),
    })

    expect(entity.table).toBe(testTable)
  })

  it('should provide access to its schema', () => {
    const schema = z.object({
      id: z.string(),
      name: z.string(),
    })

    const entity = new DynamoEntity({
      table: testTable,
      schema,
    })

    expect(entity.schema).toBe(schema)
  })

  it('should accept primary key builder functions', () => {
    const entity = new DynamoEntity({
      table: testTable,
      schema: z.object({
        id: z.string(),
        sort: z.string(),
      }),
      partitionKey: item => `USER#${item.id}`,
      sortKey: item => `SORT#${item.sort}`,
    })

    expect(entity).toBeDefined()
  })

  it('should be able to construct its partition and sort keys from key builders', () => {
    const entity = new DynamoEntity({
      table: testTable,
      schema: z.object({
        id: z.string(),
        sort: z.string(),
      }),
      partitionKey: item => `USER#${item.id}`,
      sortKey: item => `SORT#${item.sort}`,
    })

    const pk = entity.buildPartitionKey({ id: '123' })
    const sk = entity.buildSortKey({ sort: '456' })

    expect(pk).toBe('USER#123')
    expect(sk).toBe('SORT#456')
  })

  it('should be able to construct its primary key from key builders', () => {
    const entity = new DynamoEntity({
      table: testTable,
      schema: z.object({
        id: z.string(),
        sort: z.string(),
      }),
      partitionKey: item => `USER#${item.id}`,
      sortKey: item => `SORT#${item.sort}`,
    })

    const primaryKey = entity.buildPrimaryKey({ id: '123', sort: '456' })

    expect(primaryKey).toEqual({
      PK: 'USER#123',
      SK: 'SORT#456',
    })
  })

  it('should handle entities without sort keys', () => {
    const entity = new DynamoEntity({
      table: testTable,
      schema: z.object({
        id: z.string(),
      }),
      partitionKey: item => `USER#${item.id}`,
    })

    const primaryKey = entity.buildPrimaryKey({ id: '123' })

    expect(primaryKey).toEqual({
      PK: 'USER#123',
    })

    const imaginarySortKey = entity.buildSortKey({ id: '123' })
    expect(imaginarySortKey).toBeUndefined()
  })

  it('should handle entities without any key builders', () => {
    const entity = new DynamoEntity({
      table: testTable,
      schema: z.object({
        id: z.string(),
        sort: z.string(),
      }),
    })

    const primaryKey = entity.buildPrimaryKey({ id: '123', sort: '456' })

    expect(primaryKey).toEqual({
      id: '123',
      sort: '456',
    })
  })

  it('should accept global secondary index key builders', () => {
    const entity = new DynamoEntity({
      table: testTable,
      schema: z.object({
        id: z.string(),
        sort: z.string(),
        gsi1pk: z.string(),
        gsi1sk: z.string(),
      }),
      partitionKey: item => `USER#${item.id}`,
      sortKey: item => `SORT#${item.sort}`,
      globalSecondaryIndexes: {
        GSI1: {
          partitionKey: item => `GSI1PK#${item.gsi1pk}`,
          sortKey: item => `GSI1SK#${item.gsi1sk}`,
        },
      },
    })

    expect(entity.secondaryIndexKeyBuilders.gsi.GSI1).toBeDefined()
  })

  it('should accept local secondary index key builders', () => {
    const entity = new DynamoEntity({
      table: testTable,
      schema: z.object({
        id: z.string(),
        sort: z.string(),
        lsi1sk: z.string(),
      }),
      partitionKey: item => `USER#${item.id}`,
      sortKey: item => `SORT#${item.sort}`,
      localSecondaryIndexes: {
        LSI1: {
          sortKey: item => `LSI1SK#${item.lsi1sk}`,
        },
      },
    })

    expect(entity.secondaryIndexKeyBuilders.lsi.LSI1).toBeDefined()
  })

  it('should be able to construct GSI keys from key builders', () => {
    const entity = new DynamoEntity({
      table: testTable,
      schema: z.object({
        id: z.string(),
        sort: z.string(),
        gsi1pk: z.string(),
        gsi1sk: z.string(),
      }),
      partitionKey: item => `USER#${item.id}`,
      sortKey: item => `SORT#${item.sort}`,
      globalSecondaryIndexes: {
        GSI1: {
          partitionKey: item => `GSI1PK#${item.gsi1pk}`,
          sortKey: item => `GSI1SK#${item.gsi1sk}`,
        },
      },
    })

    const gsiKeys = entity.buildGlobalSecondaryIndexKey('GSI1', {
      gsi1pk: 'PKValue',
      gsi1sk: 'SKValue',
    })

    expect(gsiKeys).toEqual({
      GSI1PK: 'GSI1PK#PKValue',
      GSI1SK: 'GSI1SK#SKValue',
    })
  })

  it('should be able to construct LSI keys from key builders', () => {
    const entity = new DynamoEntity({
      table: testTable,
      schema: z.object({
        id: z.string(),
        sort: z.string(),
        lsi1sk: z.string(),
      }),
      partitionKey: item => `USER#${item.id}`,
      sortKey: item => `SORT#${item.sort}`,
      localSecondaryIndexes: {
        LSI1: {
          sortKey: item => `LSI1SK#${item.lsi1sk}`,
        },
      },
    })

    const lsiKeys = entity.buildLocalSecondaryIndexKey('LSI1', {
      id: '123',
      lsi1sk: 'LSIValue',
    })

    expect(lsiKeys).toEqual({
      PK: 'USER#123',
      LSI1SK: 'LSI1SK#LSIValue',
    })
  })

  describe('buildPrimaryOrIndexKey()', () => {
    it('should build primary key when key is given', () => {
      const entity = new DynamoEntity({
        table: testTable,
        schema: z.object({
          id: z.string(),
          sort: z.string(),
        }),
        partitionKey: item => `USER#${item.id}`,
        sortKey: item => `SORT#${item.sort}`,
      })

      const key = entity.buildPrimaryOrIndexKey({ key: { id: '123', sort: '456' } })

      expect(key).toEqual({
        PK: 'USER#123',
        SK: 'SORT#456',
      })
    })

    it('should build GSI key when GSI index is given', () => {
      const entity = new DynamoEntity({
        table: testTable,
        schema: z.object({
          id: z.string(),
          sort: z.string(),
          gsi1pk: z.string(),
          gsi1sk: z.string(),
        }),
        partitionKey: item => `USER#${item.id}`,
        sortKey: item => `SORT#${item.sort}`,
        globalSecondaryIndexes: {
          GSI1: {
            partitionKey: item => `GSI1PK#${item.gsi1pk}`,
            sortKey: item => `GSI1SK#${item.gsi1sk}`,
          },
        },
      })

      const key = entity.buildPrimaryOrIndexKey({
        index: {
          GSI1: {
            gsi1pk: 'PKValue',
            gsi1sk: 'SKValue',
          },
        },
      })

      expect(key).toEqual({
        GSI1PK: 'GSI1PK#PKValue',
        GSI1SK: 'GSI1SK#SKValue',
      })
    })

    it('should build LSI key when LSI index is given', () => {
      const entity = new DynamoEntity({
        table: testTable,
        schema: z.object({
          id: z.string(),
          sort: z.string(),
          lsi1sk: z.string(),
        }),
        partitionKey: item => `USER#${item.id}`,
        sortKey: item => `SORT#${item.sort}`,
        localSecondaryIndexes: {
          LSI1: {
            sortKey: item => `LSI1SK#${item.lsi1sk}`,
          },
        },
      })

      const key = entity.buildPrimaryOrIndexKey({
        index: {
          LSI1: {
            id: '123',
            lsi1sk: 'LSIValue',
          },
        },
      })

      expect(key).toEqual({
        PK: 'USER#123',
        LSI1SK: 'LSI1SK#LSIValue',
      })
    })
  })

  it('should build all keys correctly when all key builders are provided', () => {
    const entity = new DynamoEntity({
      table: testTable,
      schema: z.object({
        id: z.string(),
        sort: z.string(),
        gsi1pk: z.string(),
        gsi1sk: z.string(),
        lsi1sk: z.string(),
      }),
      partitionKey: item => `USER#${item.id}`,
      sortKey: item => `SORT#${item.sort}`,
      globalSecondaryIndexes: {
        GSI1: {
          partitionKey: item => `GSI1PK#${item.gsi1pk}`,
          sortKey: item => `GSI1SK#${item.gsi1sk}`,
        },
      },
      localSecondaryIndexes: {
        LSI1: {
          sortKey: item => `LSI1SK#${item.lsi1sk}`,
        },
      },
    })

    const primaryKey = entity.buildAllKeys({
      id: '123',
      sort: '456',
      gsi1pk: 'GSI1PKValue',
      gsi1sk: 'GSI1SKValue',
      lsi1sk: 'LSI1SKValue',
    })

    expect(primaryKey).toEqual({
      PK: 'USER#123',
      SK: 'SORT#456',
      GSI1PK: 'GSI1PK#GSI1PKValue',
      GSI1SK: 'GSI1SK#GSI1SKValue',
      LSI1SK: 'LSI1SK#LSI1SKValue',
    })
  })

  it('should be able to be sent commands', async () => {
    const entity = new DynamoEntity({
      table: testTable,
      schema: z.object({
        id: z.string(),
        sort: z.string(),
      }),
    })

    class TestCommand implements BaseCommand<string, typeof entity.schema> {
      execute(cmdEntity: typeof entity): Promise<string> {
        expect(cmdEntity).toBe(entity)
        return Promise.resolve('command executed')
      }
    }

    const result = await entity.send(new TestCommand())
    expect(result).toBe('command executed')
  })

  it('should be able to paginate() through paginatables', async () => {
    const entity = new DynamoEntity({
      table: testTable,
      schema: z.object({
        id: z.string(),
        sort: z.string(),
      }),
    })

    class TestPaginatable implements BasePaginatable<number, typeof entity.schema> {
      async *executePaginated(testEntity: typeof entity): AsyncGenerator<number, void, unknown> {
        expect(testEntity).toBe(entity)
        for (let i = 0; i < 3; i++) {
          yield i * 7
        }
      }
    }

    const results: number[] = []
    for await (const page of entity.paginate(new TestPaginatable())) {
      results.push(page)
    }

    expect(results).toEqual([0, 7, 14])
  })

  describe('sparse index support', () => {
    describe('buildGlobalSecondaryIndexKey()', () => {
      it('should omit GSI partition key when builder returns undefined', () => {
        const entity = new DynamoEntity({
          table: testTable,
          schema: z.object({
            id: z.string(),
            gsi1pk: z.string().optional(),
            gsi1sk: z.string(),
          }),
          partitionKey: item => `USER#${item.id}`,
          globalSecondaryIndexes: {
            GSI1: {
              partitionKey: item => (item.gsi1pk ? `GSI1PK#${item.gsi1pk}` : undefined),
              sortKey: item => `GSI1SK#${item.gsi1sk}`,
            },
          },
        })

        const gsiKeys = entity.buildGlobalSecondaryIndexKey('GSI1', {
          id: '123',
          gsi1sk: 'SKValue',
        })

        expect(gsiKeys).toEqual({
          GSI1SK: 'GSI1SK#SKValue',
        })
        expect(gsiKeys).not.toHaveProperty('GSI1PK')
      })

      it('should omit GSI sort key when builder returns undefined', () => {
        const entity = new DynamoEntity({
          table: testTable,
          schema: z.object({
            id: z.string(),
            gsi1pk: z.string(),
            gsi1sk: z.string().optional(),
          }),
          partitionKey: item => `USER#${item.id}`,
          globalSecondaryIndexes: {
            GSI1: {
              partitionKey: item => `GSI1PK#${item.gsi1pk}`,
              sortKey: item => (item.gsi1sk ? `GSI1SK#${item.gsi1sk}` : undefined),
            },
          },
        })

        const gsiKeys = entity.buildGlobalSecondaryIndexKey('GSI1', {
          id: '123',
          gsi1pk: 'PKValue',
        })

        expect(gsiKeys).toEqual({
          GSI1PK: 'GSI1PK#PKValue',
        })
        expect(gsiKeys).not.toHaveProperty('GSI1SK')
      })

      it('should omit both keys when both builders return undefined', () => {
        const entity = new DynamoEntity({
          table: testTable,
          schema: z.object({
            id: z.string(),
            gsi1pk: z.string().optional(),
            gsi1sk: z.string().optional(),
          }),
          partitionKey: item => `USER#${item.id}`,
          globalSecondaryIndexes: {
            GSI1: {
              partitionKey: item => (item.gsi1pk ? `GSI1PK#${item.gsi1pk}` : undefined),
              sortKey: item => (item.gsi1sk ? `GSI1SK#${item.gsi1sk}` : undefined),
            },
          },
        })

        const gsiKeys = entity.buildGlobalSecondaryIndexKey('GSI1', {
          id: '123',
        })

        expect(gsiKeys).toEqual({})
      })

      it('should include keys when builders return defined values', () => {
        const entity = new DynamoEntity({
          table: testTable,
          schema: z.object({
            id: z.string(),
            gsi1pk: z.string().optional(),
            gsi1sk: z.string().optional(),
          }),
          partitionKey: item => `USER#${item.id}`,
          globalSecondaryIndexes: {
            GSI1: {
              partitionKey: item => (item.gsi1pk ? `GSI1PK#${item.gsi1pk}` : undefined),
              sortKey: item => (item.gsi1sk ? `GSI1SK#${item.gsi1sk}` : undefined),
            },
          },
        })

        const gsiKeys = entity.buildGlobalSecondaryIndexKey('GSI1', {
          id: '123',
          gsi1pk: 'PKValue',
          gsi1sk: 'SKValue',
        })

        expect(gsiKeys).toEqual({
          GSI1PK: 'GSI1PK#PKValue',
          GSI1SK: 'GSI1SK#SKValue',
        })
      })

      it('should handle partial key definitions for sparse indexes', () => {
        const entity = new DynamoEntity({
          table: testTable,
          schema: z.object({
            id: z.string(),
            status: z.string().optional(),
            timestamp: z.number().optional(),
          }),
          partitionKey: item => `USER#${item.id}`,
          globalSecondaryIndexes: {
            GSI1: {
              partitionKey: item =>
                item.status === 'active' ? `STATUS#${item.status}` : undefined,
              sortKey: item => (item.timestamp ? item.timestamp : undefined),
            },
          },
        })

        const keysWithoutStatus = entity.buildGlobalSecondaryIndexKey('GSI1', {
          id: '123',
          status: 'inactive',
          timestamp: 1234567890,
        })

        expect(keysWithoutStatus).toEqual({
          GSI1SK: 1234567890,
        })

        const keysWithStatus = entity.buildGlobalSecondaryIndexKey('GSI1', {
          id: '123',
          status: 'active',
          timestamp: 1234567890,
        })

        expect(keysWithStatus).toEqual({
          GSI1PK: 'STATUS#active',
          GSI1SK: 1234567890,
        })
      })
    })

    describe('buildLocalSecondaryIndexKey()', () => {
      it('should omit LSI sort key when builder returns undefined', () => {
        const entity = new DynamoEntity({
          table: testTable,
          schema: z.object({
            id: z.string(),
            sort: z.string(),
            lsi1sk: z.string().optional(),
          }),
          partitionKey: item => `USER#${item.id}`,
          sortKey: item => `SORT#${item.sort}`,
          localSecondaryIndexes: {
            LSI1: {
              sortKey: item => (item.lsi1sk ? `LSI1SK#${item.lsi1sk}` : undefined),
            },
          },
        })

        const lsiKeys = entity.buildLocalSecondaryIndexKey('LSI1', {
          id: '123',
          sort: '456',
        })

        expect(lsiKeys).toEqual({
          PK: 'USER#123',
        })
        expect(lsiKeys).not.toHaveProperty('LSI1SK')
      })

      it('should include partition key and sort key when builder returns defined value', () => {
        const entity = new DynamoEntity({
          table: testTable,
          schema: z.object({
            id: z.string(),
            sort: z.string(),
            lsi1sk: z.string().optional(),
          }),
          partitionKey: item => `USER#${item.id}`,
          sortKey: item => `SORT#${item.sort}`,
          localSecondaryIndexes: {
            LSI1: {
              sortKey: item => (item.lsi1sk ? `LSI1SK#${item.lsi1sk}` : undefined),
            },
          },
        })

        const lsiKeys = entity.buildLocalSecondaryIndexKey('LSI1', {
          id: '123',
          sort: '456',
          lsi1sk: 'LSIValue',
        })

        expect(lsiKeys).toEqual({
          PK: 'USER#123',
          LSI1SK: 'LSI1SK#LSIValue',
        })
      })

      it('should handle sparse LSI correctly', () => {
        const entity = new DynamoEntity({
          table: testTable,
          schema: z.object({
            id: z.string(),
            sort: z.string(),
            priority: z.number().optional(),
          }),
          partitionKey: item => `USER#${item.id}`,
          sortKey: item => `SORT#${item.sort}`,
          localSecondaryIndexes: {
            LSI1: {
              sortKey: item =>
                item.priority !== undefined && item.priority > 0 ? item.priority : undefined,
            },
          },
        })

        const keysWithoutPriority = entity.buildLocalSecondaryIndexKey('LSI1', {
          id: '123',
          sort: '456',
        })

        expect(keysWithoutPriority).toEqual({
          PK: 'USER#123',
        })

        const keysWithPriority = entity.buildLocalSecondaryIndexKey('LSI1', {
          id: '123',
          sort: '456',
          priority: 5,
        })

        expect(keysWithPriority).toEqual({
          PK: 'USER#123',
          LSI1SK: 5,
        })
      })
    })

    describe('buildPrimaryOrIndexKey()', () => {
      it('should return DynamoIndexKey for GSI with undefined values', () => {
        const entity = new DynamoEntity({
          table: testTable,
          schema: z.object({
            id: z.string(),
            gsi1pk: z.string().optional(),
            gsi1sk: z.string().optional(),
          }),
          partitionKey: item => `USER#${item.id}`,
          globalSecondaryIndexes: {
            GSI1: {
              partitionKey: item => (item.gsi1pk ? `GSI1PK#${item.gsi1pk}` : undefined),
              sortKey: item => (item.gsi1sk ? `GSI1SK#${item.gsi1sk}` : undefined),
            },
          },
        })

        const keyWithPartialValues = entity.buildPrimaryOrIndexKey({
          index: {
            GSI1: {
              id: '123',
              gsi1pk: 'PKValue',
            },
          },
        })

        expect(keyWithPartialValues).toEqual({
          GSI1PK: 'GSI1PK#PKValue',
        })
      })

      it('should return DynamoIndexKey for LSI with undefined values', () => {
        const entity = new DynamoEntity({
          table: testTable,
          schema: z.object({
            id: z.string(),
            sort: z.string(),
            lsi1sk: z.string().optional(),
          }),
          partitionKey: item => `USER#${item.id}`,
          sortKey: item => `SORT#${item.sort}`,
          localSecondaryIndexes: {
            LSI1: {
              sortKey: item => (item.lsi1sk ? `LSI1SK#${item.lsi1sk}` : undefined),
            },
          },
        })

        const keyWithoutSortKey = entity.buildPrimaryOrIndexKey({
          index: {
            LSI1: {
              id: '123',
              sort: '456',
            },
          },
        })

        expect(keyWithoutSortKey).toEqual({
          PK: 'USER#123',
        })
      })

      it('should return DynamoKey for primary key', () => {
        const entity = new DynamoEntity({
          table: testTable,
          schema: z.object({
            id: z.string(),
            sort: z.string(),
          }),
          partitionKey: item => `USER#${item.id}`,
          sortKey: item => `SORT#${item.sort}`,
        })

        const primaryKey = entity.buildPrimaryOrIndexKey({
          key: {
            id: '123',
            sort: '456',
          },
        })

        expect(primaryKey).toEqual({
          PK: 'USER#123',
          SK: 'SORT#456',
        })
      })
    })
  })
})
