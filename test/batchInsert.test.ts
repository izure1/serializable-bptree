import {
  BPTreeSync,
  BPTreeAsync,
  InMemoryStoreStrategySync,
  InMemoryStoreStrategyAsync,
  NumericComparator,
} from '../src'

describe('batchInsert', () => {

  describe('Sync', () => {
    const createSyncTree = (order = 5) => {
      const tree = new BPTreeSync(
        new InMemoryStoreStrategySync(order),
        new NumericComparator()
      )
      tree.init()
      return tree
    }

    test('기본 동작: batchInsert 후 모든 값이 조회됨', () => {
      const tree = createSyncTree()
      const entries: [number, number][] = []
      for (let i = 1; i <= 50; i++) {
        entries.push([i, i * 10])
      }
      tree.batchInsert(entries)

      const result = tree.where({ gte: 0 })
      expect(result.size).toBe(50)
      for (const [key, value] of entries) {
        expect(result.get(key)).toBe(value)
      }
      tree.clear()
    })

    test('정합성: batchInsert와 개별 insert 결과가 동일', () => {
      const treeBatch = createSyncTree()
      const treeIndiv = createSyncTree()

      const entries: [number, number][] = []
      for (let i = 1; i <= 30; i++) {
        entries.push([i, i * 10])
      }

      treeBatch.batchInsert(entries)
      for (const [key, value] of entries) {
        treeIndiv.insert(key, value)
      }

      const resultBatch = treeBatch.where({ gte: 0 })
      const resultIndiv = treeIndiv.where({ gte: 0 })

      expect(resultBatch.size).toBe(resultIndiv.size)
      for (const [key, value] of resultBatch) {
        expect(resultIndiv.get(key)).toBe(value)
      }
      treeBatch.clear()
      treeIndiv.clear()
    })

    test('빈 배열: 에러 없이 처리', () => {
      const tree = createSyncTree()
      expect(() => tree.batchInsert([])).not.toThrow()
      tree.clear()
    })

    test('중복 키: 같은 key + 같은 value는 중복 삽입 안됨', () => {
      const tree = createSyncTree()
      tree.batchInsert([[1, 10], [1, 10], [2, 20]])
      const result = tree.where({ gte: 0 })
      expect(result.size).toBe(2)
      expect(result.get(1)).toBe(10)
      expect(result.get(2)).toBe(20)
      tree.clear()
    })

    test('대량 삽입: 1000개 데이터', () => {
      const tree = createSyncTree(50)
      const entries: [number, number][] = []
      for (let i = 1; i <= 1000; i++) {
        entries.push([i, i])
      }
      tree.batchInsert(entries)

      const result = tree.where({ gte: 0 })
      expect(result.size).toBe(1000)
      tree.clear()
    })

    test('역순 데이터도 올바르게 처리', () => {
      const tree = createSyncTree()
      const entries: [number, number][] = []
      for (let i = 50; i >= 1; i--) {
        entries.push([i, i * 10])
      }
      tree.batchInsert(entries)

      const result = tree.where({ gte: 0 })
      expect(result.size).toBe(50)
      tree.clear()
    })

    test('트랜잭션 레벨에서도 동작', () => {
      const tree = createSyncTree()
      const tx = tree.createTransaction()
      tx.batchInsert([[1, 10], [2, 20], [3, 30]])
      tx.commit()

      const result = tree.where({ gte: 0 })
      expect(result.size).toBe(3)
      tree.clear()
    })
  })

  describe('Async', () => {
    const createAsyncTree = (order = 5) => {
      const tree = new BPTreeAsync(
        new InMemoryStoreStrategyAsync(order),
        new NumericComparator()
      )
      return tree
    }

    test('기본 동작: batchInsert 후 모든 값이 조회됨', async () => {
      const tree = createAsyncTree()
      await tree.init()

      const entries: [number, number][] = []
      for (let i = 1; i <= 50; i++) {
        entries.push([i, i * 10])
      }
      await tree.batchInsert(entries)

      const result = await tree.where({ gte: 0 })
      expect(result.size).toBe(50)
      for (const [key, value] of entries) {
        expect(result.get(key)).toBe(value)
      }
      tree.clear()
    })

    test('정합성: batchInsert와 개별 insert 결과가 동일', async () => {
      const treeBatch = createAsyncTree()
      const treeIndiv = createAsyncTree()
      await treeBatch.init()
      await treeIndiv.init()

      const entries: [number, number][] = []
      for (let i = 1; i <= 30; i++) {
        entries.push([i, i * 10])
      }

      await treeBatch.batchInsert(entries)
      for (const [key, value] of entries) {
        await treeIndiv.insert(key, value)
      }

      const resultBatch = await treeBatch.where({ gte: 0 })
      const resultIndiv = await treeIndiv.where({ gte: 0 })

      expect(resultBatch.size).toBe(resultIndiv.size)
      for (const [key, value] of resultBatch) {
        expect(resultIndiv.get(key)).toBe(value)
      }
      treeBatch.clear()
      treeIndiv.clear()
    })

    test('빈 배열: 에러 없이 처리', async () => {
      const tree = createAsyncTree()
      await tree.init()
      await expect(tree.batchInsert([])).resolves.toBeUndefined()
      tree.clear()
    })

    test('대량 삽입: 1000개 데이터', async () => {
      const tree = createAsyncTree(50)
      await tree.init()

      const entries: [number, number][] = []
      for (let i = 1; i <= 1000; i++) {
        entries.push([i, i])
      }
      await tree.batchInsert(entries)

      const result = await tree.where({ gte: 0 })
      expect(result.size).toBe(1000)
      tree.clear()
    })

    test('트랜잭션 레벨에서도 동작', async () => {
      const tree = createAsyncTree()
      await tree.init()

      const tx = await tree.createTransaction()
      await tx.batchInsert([[1, 10], [2, 20], [3, 30]])
      await tx.commit()

      const result = await tree.where({ gte: 0 })
      expect(result.size).toBe(3)
      tree.clear()
    })

    test('성능 비교: batchInsert vs 개별 insert', async () => {
      const N = 500
      const entries: [number, number][] = []
      for (let i = 1; i <= N; i++) {
        entries.push([i, i])
      }

      // 개별 insert
      const treeIndiv = createAsyncTree(50)
      await treeIndiv.init()
      const startIndiv = performance.now()
      for (const [key, value] of entries) {
        await treeIndiv.insert(key, value)
      }
      const endIndiv = performance.now()
      const indivTime = endIndiv - startIndiv

      // batchInsert
      const treeBatch = createAsyncTree(50)
      await treeBatch.init()
      const startBatch = performance.now()
      await treeBatch.batchInsert(entries)
      const endBatch = performance.now()
      const batchTime = endBatch - startBatch

      console.log(`[성능 비교] N=${N}`)
      console.log(`  개별 insert: ${indivTime.toFixed(2)}ms`)
      console.log(`  batchInsert: ${batchTime.toFixed(2)}ms`)
      console.log(`  개선율: ${((1 - batchTime / indivTime) * 100).toFixed(1)}%`)

      // batchInsert가 올바르게 동작하는지만 확인 (성능은 로그로 확인)
      const result = await treeBatch.where({ gte: 0 })
      expect(result.size).toBe(N)

      treeIndiv.clear()
      treeBatch.clear()
    }, 30000)
  })
})
