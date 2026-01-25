import { BPTreeSync, InMemoryStoreStrategySync, NumericComparator } from '../src'

describe('Concurrent Transaction Isolation and Non-Pollution Test', () => {
  test('concurrent transactions should not interfere with each other and not pollute nodes after rollback/commit', () => {
    const order = 4
    const tree = new BPTreeSync<number, number>(
      new InMemoryStoreStrategySync(order),
      new NumericComparator()
    )

    tree.init()

    // 초기 데이터 삽입 (1~200) -> 멀티 레벨 노드 분할 발생
    const txInit = tree.createTransaction()
    for (let i = 1; i <= 200; i++) {
      txInit.insert(i, i * 100)
    }
    txInit.commit()

    // 두 개의 동시 트랜잭션 생성 (동일한 데이터 스냅샷 공유)
    const txRollback = tree.createTransaction()
    const txCommit = tree.createTransaction()

    // 1. txRollback: 대규모 삭제 (1~100)
    for (let i = 1; i <= 100; i++) {
      txRollback.delete(i, i * 100)
    }
    expect(txRollback.get(1)).toBeUndefined()
    expect(txRollback.get(101)).toBe(10100)

    // 2. txCommit: 신규 삽입 (201~300)
    for (let i = 201; i <= 300; i++) {
      txCommit.insert(i, i * 100)
    }
    // Isolation 확인: txCommit에서는 여전히 1~100이 보여야 함
    expect(txCommit.get(1)).toBe(100)
    expect(txCommit.get(201)).toBe(20100)

    // 서로의 변경 사항이 보이지 않는지 확인
    expect(txRollback.get(201)).toBeUndefined()
    expect(txCommit.get(1)).toBe(100)

    // 3. txRollback을 먼저 롤백
    txRollback.rollback()

    // 4. txCommit을 커밋
    const commitResult = txCommit.commit()
    expect(commitResult.success).toBe(true)

    // 5. 최종 트리 상태 검증
    // 원본 1~200 유지 + 신규 201~300 추가 = 총 300개
    for (let i = 1; i <= 300; i++) {
      expect(tree.get(i)).toBe(i * 100)
    }

    // 6. 트리 무결성 및 노드 오염 체크 (전수 조사)
    const allValues = tree.where({ gte: 0 })
    expect(allValues.size).toBe(300)
  })
})
