# serializable-bptree

[![](https://data.jsdelivr.com/v1/package/npm/serializable-bptree/badge)](https://www.jsdelivr.com/package/npm/serializable-bptree)
![Node.js workflow](https://github.com/izure1/serializable-bptree/actions/workflows/node.js.yml/badge.svg)
[![Benchmark](https://github.com/izure1/serializable-bptree/actions/workflows/benchmark.yml/badge.svg)](https://izure1.github.io/serializable-bptree/dev/bench/)

This is a B+tree that's totally okay with duplicate values. If you need to keep track of the B+ tree's state, don't just leave it in memory - make sure you write it down.

```typescript
import { readFileSync, writeFileSync, unlinkSync, existsSync } from 'fs'
import {
  BPTreeSync,
  SerializeStrategySync,
  NumericComparator
} from 'serializable-bptree'

class FileStoreStrategySync extends SerializeStrategySync<K, V> {
  id(): string {
    return crypto.randomUUID()
  }

  read(id: string): BPTreeNode<K, V> {
    const raw = readFileSync(id, 'utf8')
    return JSON.parse(raw)
  }

  write(id: string, node: BPTreeNode<K, V>): void {
    const stringify = JSON.stringify(node)
    writeFileSync(id, stringify, 'utf8')
  }

  delete(id: string): void {
    unlinkSync(id)
  }

  readHead(): SerializeStrategyHead|null {
    if (!existsSync('head')) {
      return null
    }
    const raw = readFileSync('head', 'utf8')
    return JSON.parse(raw)
  }

  writeHead(head: SerializeStrategyHead): void {
    const stringify = JSON.stringify(head)
    writeFileSync('head', stringify, 'utf8')
  }
}

const order = 5
const tree = new BPTreeSync(
  new FileStoreStrategySync(order),
  new NumericComparator()
)

tree.init()
tree.insert('a', 1)
tree.insert('b', 2)
tree.insert('c', 3)

tree.delete('b', 2)

tree.where({ equal: 1 }) // Map([{ key: 'a', value: 1 }])
tree.where({ gt: 1 }) // Map([{ key: 'c', value: 3 }])
tree.where({ lt: 2 }) // Map([{ key: 'a', value: 1 }])
tree.where({ gt: 0, lt: 4 }) // Map([{ key: 'a', value: 1 }, { key: 'c', value: 3 }])
tree.where({ or: [3, 1] }) // Map([{ key: 'a', value: 1 }, { key: 'c', value: 3 }])
tree.where({ like: 'user_%' }) // Matches values matching the pattern

tree.clear()
```

## Why use a `serializable-bptree`?

Firstly, in most cases, there is no need to use a B+tree in JavaScript. This is because there is a great alternative, the Map object. Nonetheless, if you need to retrieve values in a sorted order, a B+tree can be a good solution. These cases are often related to databases, and you may want to store this state not just in memory, but on a remote server or in a file. In this case, **serializable-bptree** can help you.

Additionally, this library supports asynchronous operations and rule-based query optimization for multi-index scenarios. Please refer to the sections below for more details.

## Key Features

- **Transactions**: Supports ACID transactions with Snapshot Isolation (MVCC).
- **Serializable**: Save and load the B+Tree state to/from any storage (File, DB, Memory, etc.).
- **Duplicate Values**: Naturally handles duplicate values.
- **Async/Sync Support**: Provides both synchronous and asynchronous APIs.
- **Query Optimization**: Rule-based optimizer to choose the best index for complex queries.
- **TypeScript**: Fully typed for a better developer experience.

## How to use

### Node.js (cjs)

```bash
npm i serializable-bptree
```

```typescript
import {
  BPTreeSync,
  BPTreeAsync,
  SerializeStrategySync,
  SerializeStrategyAsync,
  NumericComparator,
  StringComparator
} from 'serializable-bptree'
```

### Browser (esm)

```html
<script type="module">
  import {
    BPTreeSync,
    BPTreeAsync,
    InMemoryStoreStrategySync,
    InMemoryStoreStrategyAsync,
    ValueComparator,
    NumericComparator,
    StringComparator
  } from 'https://cdn.jsdelivr.net/npm/serializable-bptree@8/+esm'
</script>
```

## Documentation

Explore the detailed guides and concepts of `serializable-bptree`:

- **Core Concepts**
  - [Value Comparators](./docs/COMPARATORS.md): How sorting and matching works.
  - [Serialize Strategies](./docs/STRATEGIES.md): How to persist nodes to storage.
- **API & Usage**
  - [Query Conditions](./docs/QUERY.md): Detailed explanation of the `where()` operators.
  - [Asynchronous Usage](./docs/ASYNC.md): How to use the tree in an async environment.
- **Advanced Topics**
  - [Transaction System (MVCC)](./docs/TRANSACTION.md): ACID transactions, Snapshot Isolation, and Optimistic Locking.
  - [Best Practices](./docs/BEST_PRACTICES.md): Tips for bulk insertion and performance optimization.
  - [Duplicate Value Handling](./docs/DUPLICATE_VALUES.md): Strategies for managing large amounts of duplicate data.
  - [Concurrency & Synchronization](./docs/CONCURRENCY.md): Multi-instance usage and locking mechanisms.
  - [Query Optimization Guide](./docs/QUERY.md#performance--optimization): How to use `ChooseDriver` and `keys()` for complex queries.
  - [Performance Benchmark](https://izure1.github.io/serializable-bptree/dev/bench/): Real-time performance metrics and history.

## Quick Example: Query Optimization

When you have multiple indexes (e.g., an index for `id` and another for `age`), you can use `ChooseDriver` to select the most efficient index for your query.

```typescript
const query = { id: { equal: 100 }, age: { gt: 20 } }

// 1. Select the best index based on condition priority
const candidates = [
  { tree: idxId, condition: query.id },
  { tree: idxAge, condition: query.age }
]
const driver = BPTreeSync.ChooseDriver(candidates)
const others = candidates.filter((c) => driver.tree !== c.tree)

// 2. Execute query using the selected driver
let keys = driver.tree.keys(driver.condition)
for (const { tree, condition } of others) {
  keys = tree.keys(condition, keys)
}

console.log('Found: ', keys)
```

## Migration

Instructions for migrating between major versions (e.g., v8.0.0, v6.0.0) can be found in the [Migration Guide](./docs/MIGRATION.md).

## LICENSE

MIT LICENSE
