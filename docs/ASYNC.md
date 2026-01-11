# Asynchronous Usage

`serializable-bptree` provides full support for asynchronous operations, which is essential for I/O-bound tasks like file system access or remote database communication.

## Implementation Example

To use the tree asynchronously, use `BPTreeAsync` and `SerializeStrategyAsync`.

```typescript
import { BPTreeAsync, SerializeStrategyAsync, NumericComparator } from 'serializable-bptree'
import { readFile, writeFile, unlink } from 'fs/promises'

class FileStoreStrategyAsync extends SerializeStrategyAsync<K, V> {
  async id(isLeaf: boolean): Promise<string> {
    return crypto.randomUUID()
  }

  async read(id: string): Promise<BPTreeNode<K, V>> {
    const raw = await readFile(id, 'utf8')
    return JSON.parse(raw)
  }

  async write(id: string, node: BPTreeNode<K, V>): Promise<void> {
    await writeFile(id, JSON.stringify(node), 'utf8')
  }

  async delete(id: string): Promise<void> {
    await unlink(id)
  }

  async readHead(): Promise<SerializeStrategyHead | null> {
    // ... restore head info ...
  }

  async writeHead(head: SerializeStrategyHead): Promise<void> {
    // ... save head info ...
  }
}

const tree = new BPTreeAsync(new FileStoreStrategyAsync(5), new NumericComparator())
await tree.init()
await tree.insert('key', 100)
const results = await tree.where({ equal: 100 })
```

## Key Differences from Sync

1. **Class Names**: Suffix everything with `Async` (e.g., `BPTreeAsync`, `InMemoryStoreStrategyAsync`).
2. **Method Returns**: Most methods return `Promise` and must be `await`ed.
3. **Concurrency**: `BPTreeAsync` includes built-in read/write locks to ensure data integrity during concurrent operations.
