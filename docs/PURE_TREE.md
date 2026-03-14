# Pure Tree (No MVCC/Cache)

This document explains the purpose, use cases, and limitations of the **Pure Tree** classes (`BPTreePureSync` and `BPTreePureAsync`) introduced in version 9.0.0.

---

## Introduction

`BPTreePureSync` and `BPTreePureAsync` are the lightest implementations of a B+Tree available in this library. 
Unlike `BPTreeSync` or `BPTreeAsync`, the Pure classes **do not include any internal Transaction management, MVCC (Multi-Version Concurrency Control), or Node Caching mechanisms.** 

They are designed solely to execute pure B+Tree algorithms (searching, splitting, merging, etc.) and delegate all I/O operations directly to the provided `SerializeStrategy`.

## Why were they created?

The standard `BPTreeSync` and `BPTreeAsync` classes are powerful because they provide ACID transactions, Snapshot Isolation, and internal Read/Write caching out of the box. However, they introduce overhead that might be unnecessary or redundant in certain situations.

Specifically, the Pure B+Tree classes were created for the following reasons:

1. **Avoid Double Caching:** When you build a system where the node state is already managed by an external library, a global cache manager, or custom logic (e.g., maintaining `dirtyPages` manually), using the standard tree classes would result in redundant memory allocation and double caching.
2. **Custom Transaction Control:** If you already have your own Concurrency Control or Transaction mechanism at the Storage or Application layer, you do not need the library's built-in `mvcc-api`.
3. **Minimize Memory Footprint:** The Pure classes operate with extreme memory efficiency since they do not need to keep track of rollback buffers, snapshot histories, or internal node locks.

## Limitations & Unavailable Features

Because the `BPTreePureSync` and `BPTreePureAsync` are strictly focused on B+Tree structural algorithms, they **do not support** the following features:

- **No `commit()`, `rollback()`, or `createTransaction()`**: The Pure classes do not understand the concept of a transaction. Once an insert/delete operation is called, it directly modifies the structure via the `SerializeStrategy`.
- **No `reload()` or `clear()`**: Since there is no internal cache or state kept in the tree instance, there is nothing to clear or reload in memory.
- **No Concurrency Safety (Out of the Box)**: Without MVCC, conflicting concurrent writes will result in structural corruption. If you use the Pure Tree in a highly concurrent environment, **you must implement your own locking or consistency mechanism** inside your custom `SerializeStrategy`.
- **No Built-in Memory Cache**: Every time a node needs to be accessed during a traversal, the `strategy.read()` method is called. If your Strategy reads directly from the disk without caching, performance will degrade. Your Strategy is fully responsible for caching loaded nodes if needed.

## When to use?

You should consider using `BPTreePureSync` or `BPTreePureAsync` if:
- You are integrating this B+Tree with another state-management library that handles its own "Dirty Pages" and caching.
- You have extremely constrained memory limits.
- You need a simple data structure and you guarantee that all operations will happen sequentially (or you handle the locking yourself).
- You want to execute B+Tree operations directly on top of a highly optimized custom Storage layer.

## Usage Example

The usage is identical to the standard tree, minus any transaction management.

```typescript
import {
  BPTreePureSync,
  InMemoryStoreStrategySync,
  NumericComparator
} from 'serializable-bptree'

// Strategy is responsible for storing and retrieving nodes.
const strategy = new InMemoryStoreStrategySync<string, number>(5)
const tree = new BPTreePureSync<string, number>(strategy, new NumericComparator())

// Initialize the tree structure (Creates a root)
tree.init()

// Directly alters the strategy store
tree.insert('a', 1)
tree.insert('b', 2)
tree.insert('c', 3)

// Reads directly from the strategy
const result = tree.where({ gt: 1 })
console.log(result.size) // 2
console.log(result.has('b')) // true

// No commit() is needed. The state is already updated in the strategy.
```
