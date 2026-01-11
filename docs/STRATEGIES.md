# Serialize Strategy

A B+tree consists of numerous nodes. To persist these nodes (e.g., to a file or database), you must implement a logic for input/output by inheriting from the `SerializeStrategy` class.

## Interface Definition

Depending on your environment, you can inherit from `SerializeStrategySync` or `SerializeStrategyAsync`.

```typescript
import { SerializeStrategySync } from 'serializable-bptree'

class MyFileIOStrategySync extends SerializeStrategySync<K, V> {
  id(isLeaf: boolean): string { /* ... */ }
  read(id: string): BPTreeNode<K, V> { /* ... */ }
  write(id: string, node: BPTreeNode<K, V>): void { /* ... */ }
  delete(id: string): void { /* ... */ }
  readHead(): SerializeStrategyHead | null { /* ... */ }
  writeHead(head: SerializeStrategyHead): void { /* ... */ }
}
```

## Methods Explanation

### `id(isLeaf: boolean): string`
Generates a unique identifier for a new node. Usually a UUID or a file path. This is called before a node is created.

### `read(id: string): BPTreeNode<K, V>`
Loads a saved node from storage. This is called only once when a node is first accessed; subsequent accesses use the in-memory cache.

### `write(id: string, node: BPTreeNode<K, V>): void`
Called whenever a node's content changes (insert/delete). This synchronizes in-memory changes to persistent storage.

> [!TIP]
> Since this is called frequently, consider using **Write-Back Caching** to batch I/O operations for better performance.

### `delete(id: string): void`
Called when a node is no longer needed (e.g., node merging or tree clearing). Use this to free up storage space.

### `readHead(): SerializeStrategyHead | null`
Restores the tree's metadata (like the root node ID). Returns `null` for a new tree.

### `writeHead(head: SerializeStrategyHead): void`
Persists the tree's metadata whenever the root node or other global state changes.

## Built-in Strategies

`serializable-bptree` provides memory-based strategies out of the box:

- **InMemoryStoreStrategySync**
- **InMemoryStoreStrategyAsync**

These are ideal for testing or when persistence is not required.
