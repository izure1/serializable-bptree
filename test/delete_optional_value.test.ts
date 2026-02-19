import { BPTreeSync } from '../src/BPTreeSync'
import { BPTreeAsync } from '../src/BPTreeAsync'
import { InMemoryStoreStrategySync } from '../src/SerializeStrategySync'
import { InMemoryStoreStrategyAsync } from '../src/SerializeStrategyAsync'
import { NumericComparator } from '../src/base/ValueComparator'

describe('Delete with optional value', () => {
  describe('Sync', () => {
    it('should delete by key only', () => {
      const tree = new BPTreeSync<number, number>(
        new InMemoryStoreStrategySync(5),
        new NumericComparator()
      )
      const tx = tree.createTransaction()

      tx.insert(1, 10)
      tx.insert(2, 20)

      expect(tx.exists(1, 10)).toBe(true)

      // Delete with key only
      tx.delete(1)

      expect(tx.exists(1, 10)).toBe(false)
      expect(tx.get(1)).toBeUndefined()
      expect(tx.get(2)).toBe(20)
    })

    it('should do nothing if key not found', () => {
      const tree = new BPTreeSync<number, number>(
        new InMemoryStoreStrategySync(5),
        new NumericComparator()
      )
      const tx = tree.createTransaction()

      tx.insert(1, 10)

      tx.delete(999) // Non-existent key

      expect(tx.get(1)).toBe(10)
    })
  })

  describe('Async', () => {
    it('should delete by key only', async () => {
      const tree = new BPTreeAsync<number, number>(
        new InMemoryStoreStrategyAsync(5),
        new NumericComparator()
      )
      const tx = await tree.createTransaction()

      await tx.insert(1, 10)
      await tx.insert(2, 20)

      expect(await tx.exists(1, 10)).toBe(true)

      // Delete with key only
      await tx.delete(1)

      expect(await tx.exists(1, 10)).toBe(false)
      expect(await tx.get(1)).toBeUndefined()
      expect(await tx.get(2)).toBe(20)
    })

    it('should do nothing if key not found', async () => {
      const tree = new BPTreeAsync<number, number>(
        new InMemoryStoreStrategyAsync(5),
        new NumericComparator()
      )
      const tx = await tree.createTransaction()

      await tx.insert(1, 10)

      await tx.delete(999) // Non-existent key

      expect(await tx.get(1)).toBe(10)
    })
  })
})
