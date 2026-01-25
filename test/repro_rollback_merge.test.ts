import { BPTreeSync, InMemoryStoreStrategySync, NumericComparator } from '../src'

describe('Rollback After Node Merge Test', () => {
  test('should restore tree state after rollback even if node split and merge occurred', () => {
    // order = 4: 부모 노드는 최소 1개, 최대 3개의 값을 가질 수 있음 (자식 노드는 2~4개)
    // 리프 노드는 최소 ceil((4-1)/2) = 2개, 최대 3개의 값을 가질 수 있음
    const order = 4
    const tree = new BPTreeSync<number, number>(
      new InMemoryStoreStrategySync(order),
      new NumericComparator()
    )

    tree.init()

    // 1단계: 노드 분할 유도 (100개 삽입하여 멀티 레벨 트리 구성)
    const tx1 = tree.createTransaction()
    for (let i = 1; i <= 100; i++) {
      tx1.insert(i, i * 100)
    }
    const result1 = tx1.commit()
    expect(result1.success).toBe(true)

    // 초기 상태 검증
    for (let i = 1; i <= 100; i++) {
      expect(tree.get(i)).toBe(i * 100)
    }

    // 2단계: 대규모 노드 병합 유도 및 롤백
    const tx2 = tree.createTransaction()
    // 1~50번 삭제 (대규모 병합 및 재분배 발생 유도)
    for (let i = 1; i <= 50; i++) {
      tx2.delete(i, i * 100)
    }

    // 트랜잭션 도중 상태 확인
    for (let i = 1; i <= 50; i++) {
      expect(tx2.get(i)).toBeUndefined()
    }
    expect(tx2.get(51)).toBe(5100)

    // 롤백 수행
    tx2.rollback()

    // 3단계: 최종 상태 검증
    // 원래 데이터 100개가 모두 정상적으로 조회되어야 함
    for (let i = 1; i <= 100; i++) {
      expect(tree.get(i)).toBe(i * 100)
    }

    // 추가 삽입 및 조회 테스트 (트리 구조 무결성 재확인)
    const tx3 = tree.createTransaction()
    tx3.insert(101, 10100)
    tx3.commit()

    expect(tree.get(1)).toBe(100)
    expect(tree.get(101)).toBe(10100)
  })
})
