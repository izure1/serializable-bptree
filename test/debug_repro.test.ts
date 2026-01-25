import {
  BPTreeAsync,
  NumericComparator,
  InMemoryStoreStrategyAsync
} from '../src'

describe('debug-repro', () => {
  test('inspect tree structure', async () => {
    const tree = new BPTreeAsync(
      new InMemoryStoreStrategyAsync(3),
      new NumericComparator()
    )
    await tree.init()

    await tree.insert('a', 2)
    await tree.insert('b', 2)
    await tree.insert('c', 2)
    await tree.insert('d', 2)
    await tree.insert('e', 2)

    console.log('--- Initial Tree Structure ---')
    await printTree(tree)

    console.log('--- Deleting a ---')
    await tree.delete('a', 2)

    console.log('--- After Delete a ---')
    await printTree(tree)

    const result = await tree.where({ equal: 2 })
    console.log('Result for equal: 2', Array.from(result.keys()))

    expect(result.has('a')).toBe(false)
  })
})

async function printTree(tree: BPTreeAsync<any, any>) {
  const rootId = tree.getRootId()
  await printNode(tree, rootId, 0)
}

async function printNode(tree: BPTreeAsync<any, any>, nodeId: string, depth: number) {
  // @ts-ignore - accessing protected method for debug
  const node = await tree.getNode(nodeId)
  const indent = '  '.repeat(depth)
  console.log(`${indent}Node ${node.id} (Leaf: ${node.leaf}): keys=[${JSON.stringify(node.keys)}], values=[${JSON.stringify(node.values)}]`)
  if (!node.leaf) {
    for (const key of node.keys) {
      await printNode(tree, key as string, depth + 1)
    }
  }
}
