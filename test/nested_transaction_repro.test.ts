import { BPTreeSync } from '../src/BPTreeSync'
import { BPTreeAsync } from '../src/BPTreeAsync'
import { InMemoryStoreStrategySync } from '../src/SerializeStrategySync'
import { InMemoryStoreStrategyAsync } from '../src/SerializeStrategyAsync'
import { NumericComparator } from '../src/base/ValueComparator'

const COMPARE = new NumericComparator()

describe('Nested Transaction Restriction Reproduction (Fixed)', () => {
  test('Sync: init/clear behavior on nested transaction', () => {
    const strategy = new InMemoryStoreStrategySync(100)
    const tree = new BPTreeSync(strategy, COMPARE)
    tree.init()
    const tx = tree.createTransaction()

    console.log('--- Sync Test Start ---')
    try {
      tx.init()
      console.log('Sync nested init() succeeded (Unexpected!)')
    } catch (e: any) {
      console.log('Sync nested init() restricted successfully:', e.message)
    }

    try {
      tx.clear()
      console.log('Sync nested clear() succeeded (Unexpected!)')
    } catch (e: any) {
      console.log('Sync nested clear() restricted successfully:', e.message)
    }
    console.log('--- Sync Test End ---')
  })

  test('Async: init/clear behavior on nested transaction', async () => {
    const strategy = new InMemoryStoreStrategyAsync(100)
    const tree = new BPTreeAsync(strategy, COMPARE)
    await tree.init()
    const tx = await tree.createTransaction()

    console.log('--- Async Test Start ---')
    try {
      await tx.init()
      console.log('Async nested init() succeeded (Unexpected!)')
    } catch (e: any) {
      console.log('Async nested init() restricted successfully:', e.message)
    }

    try {
      tx.clear()
      console.log('Async nested clear() succeeded (Unexpected!)')
    } catch (e: any) {
      console.log('Async nested clear() restricted successfully:', e.message)
    }
    console.log('--- Async Test End ---')
  })
})
