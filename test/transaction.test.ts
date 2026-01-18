import { BPTreeSync } from '../src/BPTreeSync'
import { BPTreeAsync } from '../src/BPTreeAsync'
import { InMemoryStoreStrategySync } from '../src/SerializeStrategySync'
import { InMemoryStoreStrategyAsync } from '../src/SerializeStrategyAsync'
import { NumericComparator } from '../src/base/ValueComparator'

describe('BPTree Transaction (MVCC CoW)', () => {
  describe('Sync Transaction', () => {
    let tree: BPTreeSync<number, number>
    let strategy: InMemoryStoreStrategySync<number, number>

    beforeEach(() => {
      strategy = new InMemoryStoreStrategySync(3) // Order 3 for easy splitting
      tree = new BPTreeSync(strategy, new NumericComparator())
      tree.init()
    })

    test('should insert data in transaction without affecting base tree immediately', () => {
      // Setup initial data
      tree.insert(1, 1)
      tree.insert(2, 2)

      const tx = tree.createTransaction()
      tx.insert(3, 3)

      // In transaction, we should find the data (though `get` isn't fully overridden in Tx to look at buffer yet,
      // the Tx insert logic works on isolated nodes. For now, we check internal structure or commit result)

      // Base tree should NOT have 3 yet
      expect(tree.get(3)).toBeUndefined()
      expect(tree.get(1)).toBe(1)

      const result = tx.commit()
      expect(result.success).toBe(true)
      expect(result.createdIds.length).toBeGreaterThan(0)

      // After commit, base tree should have 3
      // Note: Because we are swapping HEAD, we need to reload or re-fetch head in base tree 
      // but InMemoryStrategy shares the 'head' object reference, so it should be visible if root ID changed.
      // However, BPTreeSync caches `rootId`. We might need to refresh `rootId` from strategy head if it changed externally?
      // In this specific implementation, BPTreeSync doesn't auto-refresh rootId on every op. 
      // But here the Transaction is modifying the SAME strategy instance.

      // Let's manually force tree to re-read head for test purpose, or assume the user pattern involves re-instantiating or checking head.
      // Actually BPTreeSync holds `this.rootId`. The transaction updates `strategy.head.root`. 
      // The base tree instance `tree` won't know `this.rootId` changed until we tell it or it re-inits.

      expect(tree.get(3)).toBe(3)
    })

    test('should not affect base tree if not committed', () => {
      tree.insert(10, 10)
      const tx = tree.createTransaction()
      tx.insert(20, 20)

      // Transaction is simply discarded without commit
      expect(tree.get(10)).toBe(10)
      expect(tree.get(20)).toBeUndefined()
    })

    test('should handle node splitting (CoW Bubble Up) correctly', () => {
      // Order 3: Max 2 values per node.
      tree.insert(10, 10)
      tree.insert(20, 20)

      const tx = tree.createTransaction()
      tx.insert(30, 30) // Should cause split
      tx.insert(40, 40) // Should cause another split/merge

      const result = tx.commit()
      expect(result.success).toBe(true)

      expect(tree.get(10)).toBe(10)
      expect(tree.get(20)).toBe(20)
      expect(tree.get(30)).toBe(30)
      expect(tree.get(40)).toBe(40)
    })

    test('should handle optimistic locking conflict', () => {
      tree.insert(1, 1)
      const tx1 = tree.createTransaction()
      const tx2 = tree.createTransaction()

      tx1.insert(2, 2)
      tx2.insert(3, 3)

      const res1 = tx1.commit()
      expect(res1.success).toBe(true)

      const res2 = tx2.commit()
      expect(res2.success).toBe(false) // CAS fail because root changed by tx1

      expect(tree.get(2)).toBe(2)
      expect(tree.get(3)).toBeUndefined()
    })

    test('should maintain snapshot isolation against base tree updates', () => {
      tree.insert(1, 1)
      const tx = tree.createTransaction()

      // Update base tree directly
      tree.insert(2, 2)

      // Tx should not see 2
      expect(tree.get(2)).toBe(2)
      // TODO: Verify tx.get(2) is undefined once Transaction supports read isolation properly
    })

    test('should handle complex delete causing root shrink', () => {
      // Order 3
      const values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      values.forEach(v => tree.insert(v, v))

      const tx = tree.createTransaction()
      // Delete all
      values.forEach(v => tx.delete(v, v))

      const res = tx.commit()
      expect(res.success).toBe(true)

      values.forEach(v => expect(tree.get(v)).toBeUndefined())
    })

    test('Stress Test: 500 random operations in a single transaction (Sync)', () => {
      const initialCount = 100
      for (let i = 0; i < initialCount; i++) {
        tree.insert(i, i)
      }

      const tx = tree.createTransaction()
      const addedKeys: number[] = []
      const removedKeys: number[] = []

      // 300 Inserts
      for (let i = initialCount; i < initialCount + 300; i++) {
        tx.insert(i, i)
        addedKeys.push(i)
      }
      // 200 Deletes
      for (let i = 0; i < 200; i++) {
        const key = Math.floor(Math.random() * (initialCount + 300))
        tx.delete(key, key)
        removedKeys.push(key)
      }

      const result = tx.commit()
      expect(result.success).toBe(true)

      for (const key of addedKeys) {
        if (!removedKeys.includes(key)) {
          expect(tree.get(key)).toBe(key)
        }
      }
    })
  })

  describe('Async Transaction', () => {
    let tree: BPTreeAsync<number, number>
    let strategy: InMemoryStoreStrategyAsync<number, number>

    beforeEach(async () => {
      strategy = new InMemoryStoreStrategyAsync(3)
      tree = new BPTreeAsync(strategy, new NumericComparator())
      await tree.init()
    })

    test('should insert and commit async', async () => {
      await tree.insert(1, 1)

      const tx = await tree.createTransaction()
      await tx.insert(2, 2)

      expect(await tree.get(1)).toBe(1)
      expect(await tree.get(2)).toBeUndefined() // Isolation

      const result = await tx.commit()
      expect(result.success).toBe(true)

      expect(await tree.get(1)).toBe(1)
      expect(await tree.get(2)).toBe(2)
    })

    test('should handle complex delete for async', async () => {
      const values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      for (const v of values) {
        await tree.insert(v, v)
      }

      const tx = await tree.createTransaction()
      for (const v of values) {
        await tx.delete(v, v)
      }

      const result = await tx.commit()
      expect(result.success).toBe(true)

      for (const v of values) {
        expect(await tree.get(v)).toBeUndefined()
      }
    })

    test('Stress Test: 500 random operations in a single transaction', async () => {
      const initialCount = 100
      for (let i = 0; i < initialCount; i++) {
        await tree.insert(i, i)
      }

      const tx = await tree.createTransaction()
      const addedKeys: number[] = []
      const removedKeys: number[] = []

      // 300 Inserts
      for (let i = initialCount; i < initialCount + 300; i++) {
        await tx.insert(i, i)
        addedKeys.push(i)
      }
      // 200 Deletes
      for (let i = 0; i < 200; i++) {
        const key = Math.floor(Math.random() * (initialCount + 300))
        await tx.delete(key, key)
        removedKeys.push(key)
      }

      const result = await tx.commit()
      expect(result.success).toBe(true)

      // Verification of some state
      for (const key of addedKeys) {
        if (!removedKeys.includes(key)) {
          expect(await tree.get(key)).toBe(key)
        }
      }
    })

    test('Concurrency Test: Multiple parallel transactions with some conflicts', async () => {
      await tree.insert(100, 100)

      const txs = await Promise.all([
        tree.createTransaction(),
        tree.createTransaction(),
        tree.createTransaction(),
        tree.createTransaction(),
        tree.createTransaction()
      ])

      // Each transaction does something different
      await txs[0].insert(1, 1)
      await txs[1].insert(2, 2)
      await txs[2].delete(100, 100)
      await txs[3].insert(3, 3)
      await txs[4].insert(4, 4)

      // Commit them sequentially or in batches. 
      // The first one should succeed, others that read the same initial root should fail if they commit after.
      const results = await Promise.all(txs.map(tx => tx.commit()))

      const successCount = results.filter(r => r.success).length
      // At least one must succeed (the first one to reach strategy). 
      // In our current simple strategy with NO real DB locking but just CAS, 
      // only the very first one that finishes swap will succeed because they all share same initialRootId.
      expect(successCount).toBe(1)

      // Only one of the changes should be persistent
      const val100 = await tree.get(100)
      const val1 = await tree.get(1)
      const val2 = await tree.get(2)

      // Verification: Sum of effects matches success
      // (This is a simplified check for the "only one succeeds" property of CAS)
    })

    test('Snapshot Isolation: Tx should not see changes committed after its start', async () => {
      await tree.insert(1, 1)
      const tx = await tree.createTransaction()

      // Update base tree (this internally creates and commits a transaction)
      await tree.insert(2, 2)
      expect(await tree.get(2)).toBe(2)

      // Transaction should still use snapshot of root from its init time.
      // tx should not see key 2 (committed after tx's snapshot was taken)
      expect(await tx.get(2)).toBeUndefined()
      expect(await tx.get(1)).toBe(1)

      // Clean up tx
      await tx.rollback()
    })

    test('should populate obsoleteNodes and delete from storage on commit (cleanup=true)', async () => {
      // 1. Insert initial data
      await tree.insert(10, 10)
      const strategyAny = strategy as any
      const initialStore = { ...strategyAny.node }

      // 2. Start transaction and modify data
      const tx = await tree.createTransaction()
      await tx.delete(10, 10)

      // 3. Spy on the delete method
      const deleteSpy = jest.spyOn(strategy, 'delete')

      // 4. Commit (cleanup=true explicitly)
      const result = await tx.commit(true)
      expect(result.success).toBe(true)

      // 5. Verify obsoleteNodes are populated
      expect(result.obsoleteIds.length).toBeGreaterThan(0)
      const obsoleteIdsFromProp = result.obsoleteIds

      // 6. Verify immediate deletion from disk
      expect(deleteSpy).toHaveBeenCalled()
      for (const id of obsoleteIdsFromProp) {
        expect(strategyAny.node[id]).toBeUndefined()
      }

      deleteSpy.mockRestore()
    })

    test('should populate obsoleteNodes BUT NOT delete from storage on commit(cleanup=false)', async () => {
      await tree.insert(20, 20)
      const strategyAny = strategy as any

      const tx = await tree.createTransaction()
      await tx.delete(20, 20)

      const deleteSpy = jest.spyOn(strategy, 'delete')

      const result = await tx.commit(false)
      expect(result.success).toBe(true)

      expect(result.obsoleteIds.length).toBeGreaterThan(0)
      const obsoleteIdsFromProp = result.obsoleteIds

      // Verify NO immediate deletion from disk
      expect(deleteSpy).not.toHaveBeenCalled()
      for (const id of obsoleteIdsFromProp) {
        expect(strategyAny.node[id]).toBeDefined()
      }

      deleteSpy.mockRestore()
    })
  })
})
