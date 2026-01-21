import { BPTreeSync } from '../src/BPTreeSync'
import { InMemoryStoreStrategySync } from '../src/SerializeStrategySync'
import { NumericComparator } from '../src/base/ValueComparator'

describe('Simple MVCC Test', () => {
  let tree: BPTreeSync<number, number>
  let strategy: InMemoryStoreStrategySync<number, number>

  beforeEach(() => {
    strategy = new InMemoryStoreStrategySync(3)
    tree = new BPTreeSync(strategy, new NumericComparator())
    tree.init()
  })

  test('Isolated Insert and Commit', () => {
    // 1. Initial State
    tree.insert(1, 1)
    expect(tree.get(1)).toBe(1)

    // 2. Start Transaction
    const tx = tree.createTransaction()
    tx.insert(2, 2)

    // 3. Verify Isolation
    expect(tree.get(2)).toBeUndefined()
    expect(tx.get(2)).toBe(2)

    // 4. Commit
    const result = tx.commit()
    expect(result.success).toBe(true)

    // 5. Verify Persistence
    // We may need to re-init or refresh tree if HEAD changed but tree instance didn't auto-refresh
    // But let's check if it sees it immediately
    expect(tree.get(2)).toBe(2)
  })

  test('Conflict Detection', () => {
    tree.insert(10, 10)

    const tx1 = tree.createTransaction()
    const tx2 = tree.createTransaction()

    tx1.insert(20, 20)
    tx2.insert(20, 30) // Conflict on key 20? 
    // Actually, inserts might not conflict dependent on locking, but writing same key definitely should conflict.

    const res1 = tx1.commit()
    expect(res1.success).toBe(true)

    const res2 = tx2.commit()
    expect(res2.success).toBe(false)

    expect(tree.get(20)).toBe(20)
  })
})
