import {
  BPTreeSync,
  BPTreeAsync,
  InMemoryStoreStrategySync,
  InMemoryStoreStrategyAsync,
  NumericComparator
} from '../src'

const comparator = new NumericComparator()

describe('BPTreeTransaction Lifecycle', () => {
  test('Sync: Double init throws error', async () => {
    const strategy = new InMemoryStoreStrategySync(4)
    const tree = new BPTreeSync(strategy, comparator)
    await tree.init()

    try {
      await tree.init()
      throw new Error('Should have thrown')
    } catch (e: any) {
      if (e.message !== 'Transaction already initialized') {
        throw e
      }
    }
  })

  test('Sync: Double clear throws error', async () => {
    const strategy = new InMemoryStoreStrategySync(4)
    const tree = new BPTreeSync(strategy, comparator)
    await tree.init()
    tree.clear()

    try {
      tree.clear()
      throw new Error('Should have thrown')
    } catch (e: any) {
      if (e.message !== 'Transaction already destroyed') {
        throw e
      }
    }
  })

  test('Async: Double init throws error', async () => {
    const strategy = new InMemoryStoreStrategyAsync(4)
    const tree = new BPTreeAsync(strategy, comparator)
    await tree.init()

    try {
      await tree.init()
      throw new Error('Should have thrown')
    } catch (e: any) {
      if (e.message !== 'Transaction already initialized') {
        throw e
      }
    }
  })

  test('Async: Double clear throws error', async () => {
    const strategy = new InMemoryStoreStrategyAsync(4)
    const tree = new BPTreeAsync(strategy, comparator)
    await tree.init()
    tree.clear()

    try {
      tree.clear()
      throw new Error('Should have thrown')
    } catch (e: any) {
      if (e.message !== 'Transaction already destroyed') {
        throw e
      }
    }
  })
})
