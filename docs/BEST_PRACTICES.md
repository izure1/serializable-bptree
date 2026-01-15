# Best Practices

Here are some tips for using `serializable-bptree` more efficiently.

## Bulk Data Insertion

When inserting a large amount of data, it is much more performant to create a transaction manually and commit it once, rather than calling `tree.insert()` repeatedly.

### Why use a transaction manually?
For user convenience, `tree.insert()` internally creates and commits an individual transaction for every call. Therefore, if you insert 10,000 pieces of data using `insert()`, it results in 10,000 transaction overheads and storage (I/O) writes.

In contrast, if you open a transaction manually, all changes are processed in memory, and then recorded to storage all at once at the final commit.

### Recommended Pattern

```typescript
const tx = await tree.createTransaction();

for (const [key, value] of data) {
  // Perform insert operations within the transaction
  await tx.insert(key, value);
}

// Commit only once at the end
const result = await tx.commit();

if (!result.success) {
  // Handle failure (e.g., retry)
}
```

## Async Operation Optimization

When using the asynchronous strategy (`BPTreeAsync`), you can improve performance by processing multiple read operations in parallel. However, since write operations (commits) use optimistic locking, conflicts are likely if multiple transactions attempt to commit at the same time.

- **Read-heavy**: It is safe and fast to perform multiple lookups simultaneously using `Promise.all`, etc.
- **Write-heavy**: It is effective to reduce the number of commits by grouping operations into as few transactions as possible to prevent conflicts.
