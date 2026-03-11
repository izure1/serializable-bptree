import {
  BPTreeSync,
  BPTreeAsync,
  InMemoryStoreStrategySync,
  InMemoryStoreStrategyAsync,
  NumericComparator,
} from '../src'

describe('bulkLoad', () => {

  describe('Sync', () => {
    const createSyncTree = (order = 5) => {
      const tree = new BPTreeSync(
        new InMemoryStoreStrategySync(order),
        new NumericComparator()
      )
      tree.init()
      return tree
    }

    test('기본 동작: bulkLoad 후 모든 값이 조회됨', () => {
      const tree = createSyncTree()
      const entries: [number, number][] = []
      for (let i = 1; i <= 50; i++) {
        entries.push([i, i * 10])
      }
      tree.bulkLoad(entries)

      const result = tree.where({ gte: 0 })
      expect(result.size).toBe(50)
      for (const [key, value] of entries) {
        expect(result.get(key)).toBe(value)
      }
      tree.clear()
    })

    test('정합성: bulkLoad와 개별 insert 결과가 동일', () => {
      const treeBulk = createSyncTree()
      const treeIndiv = createSyncTree()

      const entries: [number, number][] = []
      for (let i = 1; i <= 30; i++) {
        entries.push([i, i * 10])
      }

      treeBulk.bulkLoad(entries)
      for (const [key, value] of entries) {
        treeIndiv.insert(key, value)
      }

      const resultBulk = treeBulk.where({ gte: 0 })
      const resultIndiv = treeIndiv.where({ gte: 0 })

      expect(resultBulk.size).toBe(resultIndiv.size)
      for (const [key, value] of resultBulk) {
        expect(resultIndiv.get(key)).toBe(value)
      }
      treeBulk.clear()
      treeIndiv.clear()
    })

    test('빈 배열: 에러 없이 처리', () => {
      const tree = createSyncTree()
      expect(() => tree.bulkLoad([])).not.toThrow()
      tree.clear()
    })

    test('중복 value에 여러 key 매핑', () => {
      const tree = createSyncTree()
      // key 1, 2, 3 모두 value 10을 가짐
      tree.bulkLoad([[1, 10], [2, 10], [3, 20]])
      const result = tree.where({ equal: 10 })
      expect(result.size).toBe(2) // key 1, 2
      expect(result.get(1)).toBe(10)
      expect(result.get(2)).toBe(10)
      tree.clear()
    })

    test('같은 key+value 중복 제거', () => {
      const tree = createSyncTree()
      tree.bulkLoad([[1, 10], [1, 10], [2, 20]])
      const result = tree.where({ gte: 0 })
      expect(result.size).toBe(2)
      tree.clear()
    })

    test('대량 삽입: 1000개 데이터', () => {
      const tree = createSyncTree(50)
      const entries: [number, number][] = []
      for (let i = 1; i <= 1000; i++) {
        entries.push([i, i])
      }
      tree.bulkLoad(entries)

      const result = tree.where({ gte: 0 })
      expect(result.size).toBe(1000)
      tree.clear()
    })

    test('대량 삽입: 10000개 데이터', () => {
      const tree = createSyncTree(100)
      const entries: [number, number][] = []
      for (let i = 1; i <= 10000; i++) {
        entries.push([i, i])
      }
      tree.bulkLoad(entries)

      const result = tree.where({ gte: 0 })
      expect(result.size).toBe(10000)
      tree.clear()
    })

    test('역순 데이터도 올바르게 처리', () => {
      const tree = createSyncTree()
      const entries: [number, number][] = []
      for (let i = 50; i >= 1; i--) {
        entries.push([i, i * 10])
      }
      tree.bulkLoad(entries)

      const result = tree.where({ gte: 0 })
      expect(result.size).toBe(50)
      tree.clear()
    })

    test('비어있지 않은 트리에서 호출 시 에러', () => {
      const tree = createSyncTree()
      tree.insert(1, 10)
      expect(() => tree.bulkLoad([[2, 20]])).toThrow('bulkLoad can only be called on an empty tree')
      tree.clear()
    })

    test('트랜잭션 레벨에서도 동작', () => {
      const tree = createSyncTree()
      const tx = tree.createTransaction()
      tx.bulkLoad([[1, 10], [2, 20], [3, 30]])
      tx.commit()

      const result = tree.where({ gte: 0 })
      expect(result.size).toBe(3)
      tree.clear()
    })

    test('bulkLoad 후 삭제 동작', () => {
      const tree = createSyncTree()
      tree.bulkLoad([[1, 10], [2, 20], [3, 30], [4, 40], [5, 50]])

      tree.delete(3, 30)
      const result = tree.where({ gte: 0 })
      expect(result.size).toBe(4)
      expect(result.has(3)).toBe(false)
      tree.clear()
    })

    test('bulkLoad 후 추가 insert 동작', () => {
      const tree = createSyncTree()
      tree.bulkLoad([[1, 10], [2, 20], [3, 30]])

      tree.insert(4, 40)
      tree.insert(5, 50)

      const result = tree.where({ gte: 0 })
      expect(result.size).toBe(5)
      expect(result.get(4)).toBe(40)
      expect(result.get(5)).toBe(50)
      tree.clear()
    })

    test('range 쿼리 정합성', () => {
      const tree = createSyncTree()
      const entries: [number, number][] = []
      for (let i = 1; i <= 100; i++) {
        entries.push([i, i])
      }
      tree.bulkLoad(entries)

      const gtResult = tree.where({ gt: 50 })
      expect(gtResult.size).toBe(50)

      const ltResult = tree.where({ lt: 20 })
      expect(ltResult.size).toBe(19)

      const rangeResult = tree.where({ gte: 30, lte: 40 })
      expect(rangeResult.size).toBe(11)

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

    test('기본 동작: bulkLoad 후 모든 값이 조회됨', async () => {
      const tree = createAsyncTree()
      await tree.init()

      const entries: [number, number][] = []
      for (let i = 1; i <= 50; i++) {
        entries.push([i, i * 10])
      }
      await tree.bulkLoad(entries)

      const result = await tree.where({ gte: 0 })
      expect(result.size).toBe(50)
      for (const [key, value] of entries) {
        expect(result.get(key)).toBe(value)
      }
      tree.clear()
    })

    test('정합성: bulkLoad와 개별 insert 결과가 동일', async () => {
      const treeBulk = createAsyncTree()
      const treeIndiv = createAsyncTree()
      await treeBulk.init()
      await treeIndiv.init()

      const entries: [number, number][] = []
      for (let i = 1; i <= 30; i++) {
        entries.push([i, i * 10])
      }

      await treeBulk.bulkLoad(entries)
      for (const [key, value] of entries) {
        await treeIndiv.insert(key, value)
      }

      const resultBulk = await treeBulk.where({ gte: 0 })
      const resultIndiv = await treeIndiv.where({ gte: 0 })

      expect(resultBulk.size).toBe(resultIndiv.size)
      for (const [key, value] of resultBulk) {
        expect(resultIndiv.get(key)).toBe(value)
      }
      treeBulk.clear()
      treeIndiv.clear()
    })

    test('빈 배열: 에러 없이 처리', async () => {
      const tree = createAsyncTree()
      await tree.init()
      await expect(tree.bulkLoad([])).resolves.toBeUndefined()
      tree.clear()
    })

    test('대량 삽입: 1000개 데이터', async () => {
      const tree = createAsyncTree(50)
      await tree.init()

      const entries: [number, number][] = []
      for (let i = 1; i <= 1000; i++) {
        entries.push([i, i])
      }
      await tree.bulkLoad(entries)

      const result = await tree.where({ gte: 0 })
      expect(result.size).toBe(1000)
      tree.clear()
    })

    test('트랜잭션 레벨에서도 동작', async () => {
      const tree = createAsyncTree()
      await tree.init()

      const tx = await tree.createTransaction()
      await tx.bulkLoad([[1, 10], [2, 20], [3, 30]])
      await tx.commit()

      const result = await tree.where({ gte: 0 })
      expect(result.size).toBe(3)
      tree.clear()
    })

    test('성능 비교: bulkLoad vs batchInsert', async () => {
      const N = 5000
      const entries: [number, number][] = []
      for (let i = 1; i <= N; i++) {
        entries.push([i, i])
      }

      // batchInsert
      const treeBatch = createAsyncTree(50)
      await treeBatch.init()
      const startBatch = performance.now()
      await treeBatch.batchInsert(entries)
      const batchTime = performance.now() - startBatch

      // bulkLoad
      const treeBulk = createAsyncTree(50)
      await treeBulk.init()
      const startBulk = performance.now()
      await treeBulk.bulkLoad(entries)
      const bulkTime = performance.now() - startBulk

      console.log(`[성능 비교] N=${N}`)
      console.log(`  batchInsert: ${batchTime.toFixed(2)}ms`)
      console.log(`  bulkLoad:    ${bulkTime.toFixed(2)}ms`)
      console.log(`  개선율: ${((1 - bulkTime / batchTime) * 100).toFixed(1)}%`)

      // 정합성 확인
      const resultBatch = await treeBatch.where({ gte: 0 })
      const resultBulk = await treeBulk.where({ gte: 0 })
      expect(resultBulk.size).toBe(N)
      expect(resultBulk.size).toBe(resultBatch.size)

      treeBatch.clear()
      treeBulk.clear()
    }, 30000)
  })
})
