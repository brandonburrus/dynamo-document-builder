import { DynamoTable } from '@/index'
import { documentClient } from './test-utils'

export const simpleTable = new DynamoTable({
  tableName: 'simple-key-table',
  documentClient,
  keyNames: {
    partitionKey: 'Key',
    sortKey: null,
  },
})

export const standardTable = new DynamoTable({
  tableName: 'standard-table',
  documentClient,
})

export const gsiTable = new DynamoTable({
  tableName: 'gsi-table',
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
  },
})

export const lsiTable = new DynamoTable({
  tableName: 'lsi-table',
  documentClient,
  keyNames: {
    partitionKey: 'PK',
    sortKey: 'SK',
    localSecondaryIndexes: {
      LSI1: {
        sortKey: 'LSI1SK',
      },
    },
  },
})

export const simpleGsiTable = new DynamoTable({
  tableName: 'simple-gsi-table',
  documentClient,
  keyNames: {
    partitionKey: 'PK',
    sortKey: 'SK',
    globalSecondaryIndexes: {
      GSI1: {
        partitionKey: 'GSI1PK',
      },
    },
  },
})

export const multiIndexTable = new DynamoTable({
  tableName: 'multi-index-table',
  documentClient,
  keyNames: {
    partitionKey: 'PK',
    sortKey: 'SK',
    globalSecondaryIndexes: {
      GSI1: {
        partitionKey: 'GSI1PK',
        sortKey: 'GSI1SK',
      },
      GSI2: {
        partitionKey: 'GSI2PK',
        sortKey: 'GSI2SK',
      },
    },
    localSecondaryIndexes: {
      LSI1: {
        sortKey: 'LSI1SK',
      },
      LSI2: {
        sortKey: 'LSI2SK',
      },
    },
  },
})

export const scanTable = new DynamoTable({
  tableName: 'scan-table',
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
  },
})
