import {
  BPTreeAsync,
  NumericComparator,
  InMemoryStoreStrategyAsync
} from '../src'

describe('repro-delete-bug', () => {
  test('reproduction: deleteEntry bug with duplicate values', async () => {
    // Order 3: Max 2 entries per node.
    const tree = new BPTreeAsync(
      new InMemoryStoreStrategyAsync(3),
      new NumericComparator()
    )
    await tree.init()

    // Insert multiple items with same value to force split distribution
    await tree.insert('a', 2)
    await tree.insert('b', 2)
    await tree.insert('c', 2)
    // Now leaf should be full and split.
    // Likely: Left=[ (a,2) ], Right=[ (b,2), (c,2) ], Parent=[ (2) ] (pointing to b or c?)
    // Depends on split logic.

    await tree.insert('d', 2)
    await tree.insert('e', 2)

    // Try to delete 'a' (which was inserted first, likely in Left node)
    await tree.delete('a', 2)

    // Verify deletion
    const result = await tree.where({ equal: 2 })
    // If bug exists, 'a' will still be found.
    // Because delete went Right, but 'a' is Left.
    expect(result.has('a')).toBe(false)
    expect(result.size).toBe(4) // b, c, d, e

    // Also verify 'e' (likely in Right node)
    await tree.delete('e', 2)
    const result2 = await tree.where({ equal: 2 })
    expect(result2.has('e')).toBe(false)
  })
})
