import { BPTreeSync } from '../src/BPTreeSync'
import { BPTreeAsync } from '../src/BPTreeAsync'
import { InMemoryStoreStrategySync } from '../src/SerializeStrategySync'
import { InMemoryStoreStrategyAsync } from '../src/SerializeStrategyAsync'
import { NumericComparator } from '../src/base/ValueComparator'

const COMPARE = new NumericComparator()

describe('중첩 트랜잭션 제한 검증 (최종)', () => {
  test('Sync: 중첩 트랜잭션에서 init/clear 호출 시 에러 발생 확인', () => {
    const strategy = new InMemoryStoreStrategySync(100)
    const tree = new BPTreeSync(strategy, COMPARE)
    tree.init()
    const tx = tree.createTransaction()

    expect(() => tx.init()).toThrow('Cannot call init on a nested transaction')
    expect(() => tx.clear()).toThrow('Cannot call clear on a nested transaction')
  })

  test('Async: 중첩 트랜잭션에서 init/clear 호출 시 에러 발생 확인', async () => {
    const strategy = new InMemoryStoreStrategyAsync(100)
    const tree = new BPTreeAsync(strategy, COMPARE)
    await tree.init()
    const tx = await tree.createTransaction()

    await expect(tx.init()).rejects.toThrow('Cannot call init on a nested transaction')
    expect(() => tx.clear()).toThrow('Cannot call clear on a nested transaction')
  })

  test('Root: 루트 트랜잭션에서는 init/clear가 정상 작동해야 함', async () => {
    const strategySync = new InMemoryStoreStrategySync(100)
    const treeSync = new BPTreeSync(strategySync, COMPARE)

    expect(() => treeSync.init()).not.toThrow()
    expect(() => treeSync.clear()).not.toThrow()

    const strategyAsync = new InMemoryStoreStrategyAsync(100)
    const treeAsync = new BPTreeAsync(strategyAsync, COMPARE)
    await expect(treeAsync.init()).resolves.not.toThrow()
    expect(() => treeAsync.clear()).not.toThrow()
  })
})
