import { BPTreeSync } from '../src/BPTreeSync'
import { InMemoryStoreStrategySync } from '../src/SerializeStrategySync'
import { NumericComparator } from '../src/base/ValueComparator'

describe('Debug TX Split', () => {
  test('transaction should split nodes when order is reached', () => {
    const strategy = new InMemoryStoreStrategySync(3)
    const tree = new BPTreeSync(strategy, new NumericComparator())
    tree.init()

    const tx = tree.createTransaction()
    console.log('TX Order before inserts:', tx.getOrder())

    tx.insert(1, 1)
    tx.insert(2, 2)
    tx.insert(3, 3) // With order 3, this SHOULD cause a split if order is set.

    console.log(1, tree.getRootId())
    const result = tx.commit()
    expect(result.success).toBe(true)

    const rootId = tree.getRootId()
    console.log(2, rootId)
    const rootNode = (tree as any).getNode(rootId)

    // If it split, root should be an internal node (leaf: false) with 1 value and 2 children.
    // If it didn't split, root should be a leaf (leaf: true) with 3 values.

    console.log('Root Node values length:', rootNode.values.length)
    console.log('Is root leaf?', rootNode.leaf)

    expect(tx.getOrder()).toBe(3)
    expect(rootNode.values.length).toBeLessThan(3)
    expect(rootNode.leaf).toBe(false)
  })
})
