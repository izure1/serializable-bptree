import { BPTreeSync } from '../src/BPTreeSync'
import { InMemoryStoreStrategySync } from '../src/SerializeStrategySync'
import { NumericComparator } from '../src/base/ValueComparator'

describe('Repro Stress Test', () => {
  test('reproduce stress failure', async () => {
    const strategy = new InMemoryStoreStrategySync<number, number>(3)
    const tree = new BPTreeSync(strategy, new NumericComparator())

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

    // Set seed for reproducibility if needed, but random is fine for now
    for (let i = 0; i < 200; i++) {
      const key = Math.floor(Math.random() * (initialCount + 300))
      tx.delete(key, key)
      removedKeys.push(key)
    }

    const result = tx.commit()
    expect(result.success).toBe(true)

    tree.init()

    let missingCount = 0
    for (const key of addedKeys) {
      if (!removedKeys.includes(key)) {
        const val = tree.get(key)
        if (val === undefined) {
          console.log(`Missing key: ${key}`)
          missingCount++
        }
      }
    }

    console.log(`Total missing: ${missingCount}`)

    // Check link list integrity
    let node: any = (tree as any).leftestNode()
    const visited = new Set()
    let count = 0
    while (node) {
      if (visited.has(node.id)) {
        console.log(`Cycle detected at ${node.id}`)
        break
      }
      visited.add(node.id)
      count += node.values.length
      if (node.next) {
        const nextNode: any = (tree as any).getNode(node.next)
        if (nextNode.prev !== node.id) {
          console.log(`Link broken: ${node.id}.next=${nextNode.id} but ${nextNode.id}.prev=${nextNode.prev}`)
        }
      }
      node = node.next ? (tree as any).getNode(node.next) : null
    }
    console.log(`Total values found in link list: ${count}`)

    expect(missingCount).toBe(0)
  })
})
