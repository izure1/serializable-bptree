# Concurrency and Synchronization

## Synchronization Issues

`serializable-bptree` optimizes performance by caching loaded nodes in-memory. While this works perfectly for a single tree instance, issues arise when **multiple instances** (across different processes or servers) share the same storage.

### The Problem
Each instance has its own cache. If Instance A modifies a node in storage, Instance B might still be using its outdated cached version.

### The Solution: `forceUpdate`
You must implement a signaling mechanism (e.g., Redis Pub/Sub, WebSockets) to notify other instances when a change occurs. Upon receiving a signal, call `forceUpdate()` on the affected instances to refresh their cache.

---

## Concurrency in `BPTreeAsync`

As of version 5.x.x, `BPTreeAsync` features a built-in **Read/Write Lock** mechanism.

### Internal Locking
Public methods like `insert()`, `delete()`, `where()`, and `exists()` automatically acquire the necessary locks.

### Development Precautions
- **Protected Methods**: Internal methods (prefixed with `_`) do **not** automatically acquire locks.
- **Inheritance**: If you extend `BPTreeAsync` and call protected methods directly, you must manually manage locks using `readLock` and `writeLock` to prevent race conditions.

---

## Migration from Older Versions

Versions prior to 6.0.0 had different internal sorting logic. If you are upgrading, it is highly recommended to **rebuild your tree** to ensure compatibility with the new strict sorting requirements.
