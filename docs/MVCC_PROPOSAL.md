# MVCC (Copy-on-Write) 도입 제안서

## 1. 개요
현재 `serializable-bptree`에 트랜잭션 격리(Snapshot Isolation)를 지원하는 MVCC를 도입하기 위해 **Copy-on-Write (CoW)** 방식을 제안합니다.

## 2. 핵심 아키텍처: Copy-on-Write
기존의 In-Place Update 방식 대신, 변경이 필요한 노드를 **복제(Clone)**하여 사용합니다.

### 2.1 동작 원리
1.  **쓰기(Write)**:
    *   데이터 변경 시 해당 리프 노드를 복제하여 새로운 ID를 부여합니다.
    *   변경된 자식 노드를 가리키도록 부모 노드 또한 복제 및 갱신합니다.
    *   이 과정이 루트(Root)까지 전파(Bubble Up)됩니다.
    *   최종적으로 새로운 루트 ID가 생성됩니다.
2.  **커밋(Commit)**:
    *   트랜잭션이 완료되면 `SerializeStrategy`의 Head가 새로운 루트 ID를 가리키게 합니다.
    *   이 갱신은 원자적(Atomic)으로 이루어져야 합니다.
3.  **읽기(Read)**:
    *   트랜잭션 시작 시점의 루트 ID를 기준으로 탐색합니다.
    *   구버전 노드들은 불변(Immutable) 상태로 남아있으므로 읽기 일관성이 보장됩니다.

### 2.2 장점
*   **Lock-Free Read**: 읽기 작업이 쓰기 작업을 차단하지 않습니다.
*   **Time Travel**: 과거 시점의 루트 ID만 있으면 언제든 과거 데이터를 조회할 수 있습니다.
*   **Crash Recovery**: 쓰기 도중 실패해도 기존 트리는 온전합니다.

## 3. 구현 제안

### 3.1 `BPTreeTransaction` 클래스
기존 `BPTree`를 상속하거나 래핑하여 트랜잭션 컨텍스트를 관리합니다.

```typescript
export class BPTreeTransaction<K, V> extends BPTree<K, V> {
  // 트랜잭션 내에서 생성된 노드만 관리하는 버퍼
  private txNodeBuffer: Map<string, BPTreeNode<K, V>>

  // CoW 로직이 적용된 insert/delete 구현
  // ...
  
  async commit() {
     // 1. txNodeBuffer의 내용을 스토리지에 저장 (New IDs)
     // 2. Head Update
  }
}
```

### 3.2 `SerializeStrategy` 요구사항
*   `id(isLeaf)` 메서드는 항상 **새로운 고유 ID**를 반환해야 합니다. (기존 ID 재사용 금지)
*   `delete(id)`는 트랜잭션 중에는 호출되지 않아야 하며, GC에 의해 지연 처리되어야 합니다.

## 4. 고려사항
*   **Write Amplification**: 작은 변경에도 루트까지 경로가 복사되므로 쓰기 비용이 증가합니다.
*   **Garbage Collection**: 참조되지 않는 노드를 정리하는 정책이 필요합니다.
