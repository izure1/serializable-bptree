import {
  BPTreeSync,
  BPTreeAsync,
  InMemoryStoreStrategySync,
  InMemoryStoreStrategyAsync,
  NumericComparator,
} from '../src'

describe('batchDelete', () => {

  describe('Sync', () => {
    const createSyncTree = (order = 5) => {
      const tree = new BPTreeSync(
        new InMemoryStoreStrategySync(order),
        new NumericComparator()
      )
      tree.init()
      return tree
    }

    test('기본 동작: batchDelete 후 모든 값이 삭제됨', () => {
      const tree = createSyncTree()
      const entries: [number, number][] = []
      for (let i = 1; i <= 50; i++) {
        entries.push([i, i * 10])
      }
      tree.batchInsert(entries)

      tree.batchDelete(entries)

      const result = tree.where({ gte: 0 })
      expect(result.size).toBe(0)
      tree.clear()
    })

    test('정합성: batchDelete와 개별 delete 결과가 동일', () => {
      const treeBatch = createSyncTree()
      const treeIndiv = createSyncTree()

      const entries: [number, number][] = []
      for (let i = 1; i <= 30; i++) {
        entries.push([i, i * 10])
      }

      treeBatch.batchInsert(entries)
      treeIndiv.batchInsert(entries)

      treeBatch.batchDelete(entries)
      for (const [key, value] of entries) {
        treeIndiv.delete(key, value)
      }

      const resultBatch = treeBatch.where({ gte: 0 })
      const resultIndiv = treeIndiv.where({ gte: 0 })

      expect(resultBatch.size).toBe(resultIndiv.size)
      expect(resultBatch.size).toBe(0)
      treeBatch.clear()
      treeIndiv.clear()
    })

    test('빈 배열: 에러 없이 처리', () => {
      const tree = createSyncTree()
      expect(() => tree.batchDelete([])).not.toThrow()
      tree.clear()
    })

    test('부분 삭제: 50개 중 25개만 삭제', () => {
      const tree = createSyncTree()
      const entries: [number, number][] = []
      for (let i = 1; i <= 50; i++) {
        entries.push([i, i * 10])
      }
      tree.batchInsert(entries)

      const toDelete = entries.slice(0, 25)
      tree.batchDelete(toDelete)

      const result = tree.where({ gte: 0 })
      expect(result.size).toBe(25)
      for (const [key, value] of entries.slice(25)) {
        expect(result.get(key)).toBe(value)
      }
      tree.clear()
    })

    test('트랜잭션 레벨에서도 동작', () => {
      const tree = createSyncTree()
      tree.batchInsert([[1, 10], [2, 20], [3, 30]])

      const tx = tree.createTransaction()
      tx.batchDelete([[1, 10], [2, 20]])
      tx.commit()

      const result = tree.where({ gte: 0 })
      expect(result.size).toBe(1)
      expect(result.get(3)).toBe(30)
      tree.clear()
    })

    test('value 없이 삭제', () => {
      const tree = createSyncTree()
      tree.batchInsert([[1, 10], [2, 20], [3, 30]])

      tree.batchDelete([[1], [2]])

      const result = tree.where({ gte: 0 })
      expect(result.size).toBe(1)
      expect(result.get(3)).toBe(30)
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

    test('기본 동작: batchDelete 후 모든 값이 삭제됨', async () => {
      const tree = createAsyncTree()
      await tree.init()

      const entries: [number, number][] = []
      for (let i = 1; i <= 50; i++) {
        entries.push([i, i * 10])
      }
      await tree.batchInsert(entries)

      await tree.batchDelete(entries)

      const result = await tree.where({ gte: 0 })
      expect(result.size).toBe(0)
      tree.clear()
    })

    test('정합성: batchDelete와 개별 delete 결과가 동일', async () => {
      const treeBatch = createAsyncTree()
      const treeIndiv = createAsyncTree()
      await treeBatch.init()
      await treeIndiv.init()

      const entries: [number, number][] = []
      for (let i = 1; i <= 30; i++) {
        entries.push([i, i * 10])
      }

      await treeBatch.batchInsert(entries)
      await treeIndiv.batchInsert(entries)

      await treeBatch.batchDelete(entries)
      for (const [key, value] of entries) {
        await treeIndiv.delete(key, value)
      }

      const resultBatch = await treeBatch.where({ gte: 0 })
      const resultIndiv = await treeIndiv.where({ gte: 0 })

      expect(resultBatch.size).toBe(resultIndiv.size)
      expect(resultBatch.size).toBe(0)
      treeBatch.clear()
      treeIndiv.clear()
    })

    test('빈 배열: 에러 없이 처리', async () => {
      const tree = createAsyncTree()
      await tree.init()
      await expect(tree.batchDelete([])).resolves.toBeUndefined()
      tree.clear()
    })

    test('부분 삭제: 50개 중 25개만 삭제', async () => {
      const tree = createAsyncTree()
      await tree.init()

      const entries: [number, number][] = []
      for (let i = 1; i <= 50; i++) {
        entries.push([i, i * 10])
      }
      await tree.batchInsert(entries)

      const toDelete = entries.slice(0, 25)
      await tree.batchDelete(toDelete)

      const result = await tree.where({ gte: 0 })
      expect(result.size).toBe(25)
      for (const [key, value] of entries.slice(25)) {
        expect(result.get(key)).toBe(value)
      }
      tree.clear()
    })

    test('트랜잭션 레벨에서도 동작', async () => {
      const tree = createAsyncTree()
      await tree.init()

      await tree.batchInsert([[1, 10], [2, 20], [3, 30]])

      const tx = await tree.createTransaction()
      await tx.batchDelete([[1, 10], [2, 20]])
      await tx.commit()

      const result = await tree.where({ gte: 0 })
      expect(result.size).toBe(1)
      expect(result.get(3)).toBe(30)
      tree.clear()
    })

    test('value 없이 삭제', async () => {
      const tree = createAsyncTree()
      await tree.init()

      await tree.batchInsert([[1, 10], [2, 20], [3, 30]])

      await tree.batchDelete([[1], [2]])

      const result = await tree.where({ gte: 0 })
      expect(result.size).toBe(1)
      expect(result.get(3)).toBe(30)
      tree.clear()
    })
  })
})
