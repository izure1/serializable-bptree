import { BPTreeSync } from '../src/BPTreeSync'
import { InMemoryStoreStrategySync } from '../src/SerializeStrategySync'
import { NumericComparator } from '../src/base/ValueComparator'
import { BPTreeSyncTransaction } from '../src/transaction/BPTreeSyncTransaction'

describe('BPTreeTransaction.CheckConflicts Test', () => {
  let tree: BPTreeSync<number, number>
  let strategy: InMemoryStoreStrategySync<number, number>

  beforeEach(() => {
    strategy = new InMemoryStoreStrategySync(3)
    tree = new BPTreeSync(strategy, new NumericComparator())
    tree.init()
  })

  test('Empty transaction list should return empty array', () => {
    const result = BPTreeSyncTransaction.CheckConflicts([])
    expect(Array.isArray(result)).toBe(true)
    expect(result).toHaveLength(0)
  })

  test('Single transaction (read or write) should return empty array', () => {
    const tx = tree.createTransaction()
    tx.insert(1, 100)

    // Even if it has pending writes, a single transaction cannot conflict with itself in CheckConflicts
    const result = BPTreeSyncTransaction.CheckConflicts([tx])
    expect(result).toHaveLength(0)
  })

  test('Multiple read-only transactions should NOT conflict', () => {
    // Shared data
    tree.insert(1, 100)
    tree.insert(2, 200)

    const tx1 = tree.createTransaction()
    const tx2 = tree.createTransaction()

    tx1.where({ equal: 100 })
    tx2.where({ equal: 200 })

    const result = BPTreeSyncTransaction.CheckConflicts([tx1, tx2])
    expect(result).toHaveLength(0)
  })

  test('Multiple conflicting transactions (Write-Write on same node) should return conflicting node IDs', () => {
    const tx1 = tree.createTransaction()
    const tx2 = tree.createTransaction()

    tx1.insert(1, 100)
    tx2.insert(2, 200) // Both will modify node "1" in a small tree

    const result = BPTreeSyncTransaction.CheckConflicts([tx1, tx2])

    // The conflict should contain node ID "1" (the leaf node)
    // Note: Since we fixed _initInternal, __HEAD__ should NOT be here if no split occurred
    expect(result).toContain('1')
  })

  test('Mixed transactions: conflict should be detected correctly', () => {
    const tx1 = tree.createTransaction() // Write node 1
    const tx2 = tree.createTransaction() // Read node 1
    const tx3 = tree.createTransaction() // Write node 1 (Conflicts with tx1)

    tx1.insert(1, 100)
    tx2.where({ equal: 1 }) // Read (MVCC Read-Write conflict might depend on isolation level, but CheckConflicts typically checks Write-Write)
    tx3.insert(1, 200)

    const result = BPTreeSyncTransaction.CheckConflicts([tx1, tx2, tx3])
    // tx1 and tx3 conflict on node ID "1"
    expect(result).toContain('1')
  })
})
