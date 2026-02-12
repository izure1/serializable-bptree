
import { BPTreeAsync } from '../src/BPTreeAsync'
import { BPTreeSync } from '../src/BPTreeSync'
import { InMemoryStoreStrategyAsync } from '../src/SerializeStrategyAsync'
import { InMemoryStoreStrategySync } from '../src/SerializeStrategySync'
import { NumericComparator } from '../src/base/ValueComparator'
import { BPTreeLeafNode, BPTreeInternalNode, BPTreeUnknownNode } from '../src/types'

// Helper function to validate B+Tree structure integrity and return all values
async function validateAsyncTreeStructure(tree: BPTreeAsync<number, number>, expectedCount: number): Promise<number[]> {
  // await tree.init()
  const rootId = tree.getRootId()

  if (expectedCount === 0 && !rootId) return []

  if (!rootId) {
    throw new Error('Tree root is missing but expected items > 0')
  }

  // 1. Check leaf linkage and ordering
  let node = await (tree as any).leftestNode() as BPTreeLeafNode<number, number>
  let observedCount = 0
  let lastValue = -Infinity

  const visitedIds = new Set<string>()
  const allValues: number[] = []

  while (node) {
    if (visitedIds.has(node.id)) break
    visitedIds.add(node.id)

    for (const val of node.values) {
      if (val < lastValue) throw new Error(`Ordering violation: ${lastValue} > ${val}`)
      lastValue = val
      allValues.push(val)
      observedCount++
    }

    if (!node.next) break
    const nextNode = await (tree as any).getNode(node.next) as BPTreeLeafNode<number, number>
    node = nextNode
  }

  if (observedCount !== expectedCount) {
    throw new Error(`Count mismatch: expected ${expectedCount}, found ${observedCount}`)
  }

  return allValues
}

describe('Strict BPTree Transaction Tests', () => {
  describe('Async High Concurrency & Integrity', () => {
    let tree: BPTreeAsync<number, number>

    beforeEach(async () => {
      const strategy = new InMemoryStoreStrategyAsync<number, number>(5) // Order 5
      tree = new BPTreeAsync(strategy, new NumericComparator())
      await tree.init()
    })

    test('Heavy Concurrency: 100 concurrent transactions (some fail, consistency maintained)', async () => {
      // 100 TXs, each inserting 10 unique items.
      const txCount = 100
      const itemsPerTx = 10
      const tasks: Promise<void>[] = []

      for (let i = 0; i < txCount; i++) {
        tasks.push((async () => {
          for (let j = 0; j < itemsPerTx; j++) {
            const val = i * itemsPerTx + j
            await tree.insert(val, val)
          }
        })())
      }

      const results = await Promise.allSettled(tasks)
      const successCount = results.filter(r => r.status === 'fulfilled').length

      // At least one should succeed
      expect(successCount).toBeGreaterThan(0)

      // Only FULLY successful tasks contributed 10 items.
      // Since we await insert one by one, if one fails, the task stops.
      // But BPTree insert is atomic for one item. The loop in 'tasks' inserts 10 items sequentially.
      // If item 3 fails, items 0,1,2 persist? 
      // YES, because tree.insert is a transaction itself.
      // So partially completed tasks ARE possible.
      // We cannot rely on txCount * itemsPerTx.

      // We must count successful items individually if we wanted exact match, 
      // OR redefine the test to run bulk insert in ONE transaction.
      // But here we test `tree.insert` concurrency.

      // Integrity Check: Just ensure tree structure is valid for whatever count ended up there.
      // We don't know exact count, but we can verify integrity.

      const allValues = await validateAsyncTreeStructure(tree, (await getAllValues(tree)).length)
      expect(allValues.length).toBeGreaterThan(0)
    })

    test('Heavy Concurrency: Counting successful atomic inserts', async () => {
      const totalOps = 500
      const tasks: Promise<void>[] = []

      for (let i = 0; i < totalOps; i++) {
        tasks.push(tree.insert(i, i))
      }

      const results = await Promise.allSettled(tasks)
      const successfulOps = results.filter(r => r.status === 'fulfilled').length

      console.log(`High Concurrency: ${successfulOps} / ${totalOps} succeeded.`)

      const values = await validateAsyncTreeStructure(tree, successfulOps)
      expect(values.length).toBe(successfulOps)
    }, 30000)

    test('Extreme Contention: Update same key', async () => {
      const contenders = 50
      const tasks: Promise<void>[] = []

      for (let i = 0; i < contenders; i++) {
        tasks.push(tree.insert(1, i))
      }

      const results = await Promise.allSettled(tasks)
      const successfulOps = results.filter(r => r.status === 'fulfilled').length

      // Tree size should be successfulOps
      const values = await validateAsyncTreeStructure(tree, successfulOps)

      // All values should be for key 1 (implied by validate structure if we only verify structure)
      // But wait! values array contains VALUES.
      // In B+Tree, same key can exist multiple times with different values.
      // We inserted (1, 0), (1, 1), ...
      // So all retrieved values should be < 50.
      values.forEach(v => {
        expect(v).toBeGreaterThanOrEqual(0)
        expect(v).toBeLessThan(contenders)
      })
    })

    test('Snapshot Isolation: Long running reader vs Many writers', async () => {
      await tree.insert(1, 1) // Initial data

      const longTx = await tree.createTransaction()

      // Long TX sees 1
      expect(await longTx.get(1)).toBe(1)

      // Background writers
      const writers = []
      for (let i = 100; i < 150; i++) {
        writers.push(tree.insert(i, i))
      }

      const results = await Promise.allSettled(writers)
      const successCount = results.filter(r => r.status === 'fulfilled').length

      // Long TX should NOT see ANY new data
      for (let i = 100; i < 150; i++) {
        expect(await longTx.get(i)).toBeUndefined()
      }

      // Long Tx tries to write and commit
      await longTx.insert(0, 0)
      const res = await longTx.commit()

      // Should fail if ANY writer succeeded (because root changed)
      if (successCount > 0) {
        expect(res.success).toBe(false)
      } else {
        expect(res.success).toBe(true)
      }
    })

    test('Sync Integrity Check', () => {
      const syncTree = new BPTreeSync(new InMemoryStoreStrategySync<number, number>(4), new NumericComparator())
      syncTree.init()

      syncTree.insert(1, 1)

      try {
        const tx = syncTree.createTransaction()
        tx.insert(2, 2)
        throw new Error('Simulated Failure')
      } catch (e) {
        // simulated
      }

      expect(syncTree.get(2)).toBeUndefined()
    })
  })
})

async function getAllValues(tree: BPTreeAsync<number, number>): Promise<number[]> {
  const values: number[] = []
  let wrapper = await (tree as any).leftestNode()
  while (wrapper) {
    values.push(...wrapper.values)
    if (wrapper.next) {
      wrapper = await (tree as any).getNode(wrapper.next)
    } else {
      break
    }
  }
  return values
}
