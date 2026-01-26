import { BPTreeSync } from '../src/BPTreeSync'
import { BPTreeAsync } from '../src/BPTreeAsync'
import { InMemoryStoreStrategySync } from '../src/SerializeStrategySync'
import { InMemoryStoreStrategyAsync } from '../src/SerializeStrategyAsync'
import { NumericComparator } from '../src/base/ValueComparator'

describe('BPTreeTransaction.getResultEntries Test', () => {
  describe('Sync BPTree', () => {
    let tree: BPTreeSync<number, number>
    let strategy: InMemoryStoreStrategySync<number, number>

    beforeEach(() => {
      strategy = new InMemoryStoreStrategySync(3)
      tree = new BPTreeSync(strategy, new NumericComparator())
      tree.init()
    })

    test('should track created, updated, and deleted nodes in transaction', () => {
      // 1. Initial Insert (at root)
      tree.insert(1, 1) // created (root)

      const tx = tree.createTransaction()
      tx.insert(2, 2) // updated (root)

      const entries1 = tx.getResultEntries()
      // updated keys: [rootId, '__HEAD__']
      expect(entries1.created.length).toBe(0)
      expect(entries1.updated.length).toBe(2) // root updated + __HEAD__ updated
      expect(entries1.deleted.length).toBe(0)

      const updatedKeys = entries1.updated.map(e => e.key)
      expect(updatedKeys).toContain('__HEAD__')

      // 2. Insert causing split
      tx.insert(3, 3) // split occurs
      const entries2 = tx.getResultEntries()
      expect(entries2.created.length).toBeGreaterThan(0)
      expect(entries2.updated.length).toBeGreaterThan(1)
    })

    test('should report visibility of created nodes after rollback', () => {
      const tx = tree.createTransaction()
      tx.insert(10, 10)
      tx.insert(11, 11)
      tx.insert(12, 12)

      const createdIds = tx.getResultEntries().created.map(e => e.key)

      // Rollback
      tx.rollback()

      // Verify in strategy
      const strategyAny = strategy as any
      const remainingIds = createdIds.filter(id => strategyAny.node[id] !== undefined)

      console.log(`[Sync] Created IDs: ${createdIds.length}, Remaining after rollback: ${remainingIds.length}`)
      // If remainingIds.length is 0, it means mvcc-api buffers everything.
    })
  })

  describe('Async BPTree', () => {
    let tree: BPTreeAsync<number, number>
    let strategy: InMemoryStoreStrategyAsync<number, number>

    beforeEach(async () => {
      strategy = new InMemoryStoreStrategyAsync(3)
      tree = new BPTreeAsync(strategy, new NumericComparator())
      await tree.init()
    })

    test('should track created, updated, and deleted nodes in async transaction', async () => {
      await tree.insert(1, 1)
      const tx = await tree.createTransaction()
      await tx.insert(2, 2)

      const entries = tx.getResultEntries()
      expect(entries.updated.length).toBe(2) // root + __HEAD__

      await tx.insert(3, 3) // split
      const entriesAfterSplit = tx.getResultEntries()
      expect(entriesAfterSplit.created.length).toBeGreaterThan(0)
    })

    test('should report visibility of created nodes after async rollback', async () => {
      const tx = await tree.createTransaction()
      await tx.insert(10, 10)
      await tx.insert(11, 11)
      await tx.insert(12, 12)

      const createdIds = tx.getResultEntries().created.map(e => e.key)

      tx.rollback()

      const strategyAny = strategy as any
      const remainingIds = createdIds.filter(id => strategyAny.node[id] !== undefined)
      console.log(`[Async] Created IDs: ${createdIds.length}, Remaining after rollback: ${remainingIds.length}`)
    })
  })
})
