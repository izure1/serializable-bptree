# Migration Guide

This document provides instructions for migrating between major versions of `serializable-bptree`.

## Table of Contents
- [Migration to v8.0.0](#migration-to-v800)
- [Migration to v6.0.0](#migration-to-v600)

---

## Migration to v8.0.0

Version 8.0.0 introduces a major architectural change by integrating `mvcc-api` for transaction management.

### 1. `mvcc-api` Integration
The internal custom transaction logic has been replaced with the professional MVCC management library [`mvcc-api`](https://www.npmjs.com/package/mvcc-api). This provides more robust Snapshot Isolation and conflict detection.

### 2. Changes in `commit()` and `rollback()` Return Values
Transaction results now return a `TransactionResult` object containing detailed information, rather than just success/failure.

- **Before**: No return value or simple void.
- **v8.0.0**: 
  ```typescript
  const result = tx.commit()
  if (result.success) {
    // result.created, updated, deleted contain node IDs
    console.log('Created node IDs:', result.created)
    console.log('Updated node IDs:', result.updated)
    console.log('Deleted node IDs:', result.deleted)
  } else {
    console.error('Commit failed:', result.error)
  }
  ```

### 3. Internal Structure Changes (For Power Users)
If you are directly extending `BPTreeSyncTransaction` or `BPTreeAsyncTransaction`, notice that internal method signatures and strategy patterns have been significantly changed to align with `mvcc-api` standards.

- Refer to `SyncMVCCStrategy` or `AsyncMVCCStrategy` when implementing custom storage.
- Copy-on-Write (CoW) is now managed at the MVCC level.

### Backward Compatibility (General Users)
The basic B+Tree APIs (`insert`, `delete`, `get`, `where`, etc.) remain the same, so most users' code will not require changes. However, if you are manually controlling transactions, please update your code to handle the new return values mentioned above.

---

## Migration to v6.0.0

Version 6.0.0 included a critical fix for how internal nodes are sorted.

> [!IMPORTANT]
> **Breaking Changes & Incompatibility**
> v6.0.0 enforces strict value sorting. **Data structures created with v5.x.x or earlier are incompatible** with v6.0.0. It is highly recommended to rebuild your tree from scratch.

For more details on legacy synchronization, see the [Concurrency & Synchronization](./CONCURRENCY.md) guide.

---

## References
For more specialized technical design details, please refer to the [`mvcc-api` documentation](https://github.com/izure1/mvcc-api).
