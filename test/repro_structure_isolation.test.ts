import { BPTreeSync, InMemoryStoreStrategySync, NumericComparator } from '../src'

describe('Transaction Structure Isolation Test', () => {
  test('structural changes within a transaction should be invisible until commit', () => {
    // order = 3: 리프 노드는 최대 2개의 값을 가질 수 있음 (ceil((3-1)/2) = 1, 실질적으로 2개까지 수용)
    // 3번째 값이 들어오면 분할 발생
    const order = 3
    const tree = new BPTreeSync<number, number>(
      new InMemoryStoreStrategySync(order),
      new NumericComparator()
    )

    tree.init()

    // 1단계: 데이터 100개 삽입 (안정적인 트리 구조 형성)
    for (let i = 1; i <= 100; i++) {
      tree.insert(i, i * 100)
    }

    const initialRootId = tree.getRootId()
    expect(tree.get(1)).toBe(100)
    expect(tree.get(100)).toBe(10000)
    expect(tree.get(101)).toBeUndefined()

    // 2단계: 트랜잭션 시작 후 추가 100개 데이터 삽입 (수차례의 분할 및 루트 교체 유도)
    const tx = tree.createTransaction()
    for (let i = 101; i <= 200; i++) {
      tx.insert(i, i * 100)
    }

    // tx 내에서는 200번이 보여야 하고, 루트 ID도 바뀌어 있어야 함
    expect(tx.get(200)).toBe(20000)
    const newRootId = tx.getRootId()
    expect(newRootId).not.toBe(initialRootId)

    // 3단계: 커밋 전 트리(외부) 상태 확인
    expect(tree.get(101)).toBeUndefined()
    expect(tree.getRootId()).toBe(initialRootId)

    // 4단계: 커밋 수행
    const commitResult = tx.commit()
    expect(commitResult.success).toBe(true)

    // 5단계: 커밋 후 트리 상태 확인
    expect(tree.get(200)).toBe(20000)
    expect(tree.getRootId()).toBe(newRootId)

    // 전체 무결성 확인
    const all = tree.where({ gte: 0 })
    expect(all.size).toBe(200)
    for (let i = 1; i <= 200; i++) {
      expect(tree.get(i)).toBe(i * 100)
    }
  })
})
