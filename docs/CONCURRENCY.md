# Concurrency and Synchronization

## Synchronization Issues

`serializable-bptree` optimizes performance by caching loaded nodes in-memory. While this works perfectly for a single tree instance, issues arise when **multiple instances** (across different processes or servers) share the same storage.

### The Problem
Each instance has its own cache. If Instance A modifies a node in storage, Instance B might still be using its outdated cached version.

### The Solution: `forceUpdate`
You must implement a signaling mechanism (e.g., Redis Pub/Sub, WebSockets) to notify other instances when a change occurs. Upon receiving a signal, call `forceUpdate()` on the affected instances to refresh their cache.

---

## Concurrency in v8.0.0+ (MVCC)

From version 8.0.0, the transaction system has been fully migrated to an **MVCC (Multi-Version Concurrency Control)** model via `mvcc-api`.

### Snapshot Isolation
Each transaction operates on a consistent snapshot taken at the moment `createTransaction()` is called. No external locks are required during the data operation phase, allowing for excellent read/write concurrency.

### Conflict Detection (Optimistic Locking)
Concurrency conflicts are detected at the time of `commit()`. If another transaction has updated the root of the tree since your snapshot was taken, the commit will fail. This is a form of optimistic locking that scales better than traditional Read/Write locks in distributed or high-latency environments.

### Consistency Guarantee
Even if a transaction fails due to a conflict, the tree's internal structure remains perfectly consistent because of the **Copy-on-Write** nature of node modifications. Failed transactions never partially overwrite existing data.

---

## Migration from Older Versions

Versions prior to 6.0.0 had different internal sorting logic. If you are upgrading, it is highly recommended to **rebuild your tree** to ensure compatibility with the new strict sorting requirements.
