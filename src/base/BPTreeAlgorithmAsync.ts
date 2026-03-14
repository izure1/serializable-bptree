import type {
  BPTreeCondition,
  BPTreeInternalNode,
  BPTreeLeafNode,
  BPTreeNodeKey,
  BPTreeUnknownNode,
  BPTreeSearchOption,
  SerializableData,
  SerializeStrategyHead,
} from '../types'
import type { BPTreeNodeOpsAsync, BPTreeAlgoContext } from './BPTreeNodeOps'
import { ValueComparator } from './ValueComparator'
import {
  cloneNode,
  binarySearchValues,
  insertValueIntoLeaf,
  createVerifierMap,
  resolveStartEndConfigs,
  verify,
} from './BPTreeAlgorithmSync'

// Re-export sync utilities used by both versions
export { cloneNode, binarySearchValues, insertValueIntoLeaf, createVerifierMap, resolveStartEndConfigs, verify }

// ─── Search config (async) ───────────────────────────────────────────

type AsyncSearchConfigEntry<K, V> = {
  start: (rootId: string, ops: BPTreeNodeOpsAsync<K, V>, v: V[]) => Promise<BPTreeLeafNode<K, V> | null>
  end: (rootId: string, ops: BPTreeNodeOpsAsync<K, V>, v: V[]) => Promise<BPTreeLeafNode<K, V> | null>
  direction: 1 | -1
  earlyTerminate: boolean
}

export function createSearchConfigsAsync<K, V>(
  comparator: ValueComparator<V>,
  ensureValues: (v: V | V[]) => V[],
): Record<keyof BPTreeCondition<V>, Record<'asc' | 'desc', AsyncSearchConfigEntry<K, V>>> {
  const lowest = (v: V[]): V => [...v].sort((a, b) => comparator.asc(a, b))[0]
  const highest = (v: V[]): V => [...v].sort((a, b) => comparator.asc(a, b))[v.length - 1]
  const lowestPrimary = (v: V[]): V => [...v].sort((a, b) => comparator.primaryAsc(a, b))[0]
  const highestPrimary = (v: V[]): V => [...v].sort((a, b) => comparator.primaryAsc(a, b))[v.length - 1]

  return {
    gt: {
      asc: {
        start: (r, ops, v) => findUpperBoundLeafAsync(ops, r, v[0], comparator),
        end: async () => null,
        direction: 1, earlyTerminate: false,
      },
      desc: {
        start: (r, ops) => rightestNodeAsync(ops, r),
        end: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, v[0], -1, comparator),
        direction: -1, earlyTerminate: true,
      },
    },
    gte: {
      asc: {
        start: (r, ops, v) => findLowerBoundLeafAsync(ops, r, v[0], comparator),
        end: async () => null,
        direction: 1, earlyTerminate: false,
      },
      desc: {
        start: (r, ops) => rightestNodeAsync(ops, r),
        end: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, v[0], -1, comparator),
        direction: -1, earlyTerminate: true,
      },
    },
    lt: {
      asc: {
        start: (r, ops) => leftestNodeAsync(ops, r),
        end: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, v[0], 1, comparator),
        direction: 1, earlyTerminate: true,
      },
      desc: {
        start: (r, ops, v) => findLowerBoundLeafAsync(ops, r, v[0], comparator),
        end: async () => null,
        direction: -1, earlyTerminate: false,
      },
    },
    lte: {
      asc: {
        start: (r, ops) => leftestNodeAsync(ops, r),
        end: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, v[0], 1, comparator),
        direction: 1, earlyTerminate: true,
      },
      desc: {
        start: (r, ops, v) => findUpperBoundLeafAsync(ops, r, v[0], comparator),
        end: async () => null,
        direction: -1, earlyTerminate: false,
      },
    },
    equal: {
      asc: {
        start: (r, ops, v) => findLowerBoundLeafAsync(ops, r, v[0], comparator),
        end: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, v[0], 1, comparator),
        direction: 1, earlyTerminate: true,
      },
      desc: {
        start: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, v[0], 1, comparator),
        end: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, v[0], -1, comparator),
        direction: -1, earlyTerminate: true,
      },
    },
    notEqual: {
      asc: {
        start: (r, ops) => leftestNodeAsync(ops, r),
        end: async () => null,
        direction: 1, earlyTerminate: false,
      },
      desc: {
        start: (r, ops) => rightestNodeAsync(ops, r),
        end: async () => null,
        direction: -1, earlyTerminate: false,
      },
    },
    or: {
      asc: {
        start: (r, ops, v) => findLowerBoundLeafAsync(ops, r, lowest(ensureValues(v as any)), comparator),
        end: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, highest(ensureValues(v as any)), 1, comparator),
        direction: 1, earlyTerminate: false,
      },
      desc: {
        start: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, highest(ensureValues(v as any)), 1, comparator),
        end: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, lowest(ensureValues(v as any)), -1, comparator),
        direction: -1, earlyTerminate: false,
      },
    },
    primaryGt: {
      asc: {
        start: (r, ops, v) => findUpperBoundLeafAsync(ops, r, v[0], comparator),
        end: async () => null, direction: 1, earlyTerminate: false,
      },
      desc: {
        start: (r, ops) => rightestNodeAsync(ops, r),
        end: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, v[0], -1, comparator),
        direction: -1, earlyTerminate: true,
      },
    },
    primaryGte: {
      asc: {
        start: (r, ops, v) => findLowerBoundLeafAsync(ops, r, v[0], comparator),
        end: async () => null, direction: 1, earlyTerminate: false,
      },
      desc: {
        start: (r, ops) => rightestNodeAsync(ops, r),
        end: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, v[0], -1, comparator),
        direction: -1, earlyTerminate: true,
      },
    },
    primaryLt: {
      asc: {
        start: (r, ops) => leftestNodeAsync(ops, r),
        end: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, v[0], 1, comparator),
        direction: 1, earlyTerminate: true,
      },
      desc: {
        start: (r, ops, v) => findLowerBoundLeafAsync(ops, r, v[0], comparator),
        end: async () => null, direction: -1, earlyTerminate: false,
      },
    },
    primaryLte: {
      asc: {
        start: (r, ops) => leftestNodeAsync(ops, r),
        end: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, v[0], 1, comparator),
        direction: 1, earlyTerminate: true,
      },
      desc: {
        start: (r, ops, v) => findUpperBoundLeafAsync(ops, r, v[0], comparator),
        end: async () => null, direction: -1, earlyTerminate: false,
      },
    },
    primaryEqual: {
      asc: {
        start: (r, ops, v) => findLowerBoundLeafAsync(ops, r, v[0], comparator),
        end: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, v[0], 1, comparator),
        direction: 1, earlyTerminate: true,
      },
      desc: {
        start: (r, ops, v) => findUpperBoundLeafAsync(ops, r, v[0], comparator),
        end: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, v[0], -1, comparator),
        direction: -1, earlyTerminate: true,
      },
    },
    primaryNotEqual: {
      asc: {
        start: (r, ops) => leftestNodeAsync(ops, r),
        end: async () => null, direction: 1, earlyTerminate: false,
      },
      desc: {
        start: (r, ops) => rightestNodeAsync(ops, r),
        end: async () => null, direction: -1, earlyTerminate: false,
      },
    },
    primaryOr: {
      asc: {
        start: (r, ops, v) => findLowerBoundLeafAsync(ops, r, lowestPrimary(ensureValues(v as any)), comparator),
        end: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, highestPrimary(ensureValues(v as any)), 1, comparator),
        direction: 1, earlyTerminate: false,
      },
      desc: {
        start: (r, ops, v) => findUpperBoundLeafAsync(ops, r, highestPrimary(ensureValues(v as any)), comparator),
        end: (r, ops, v) => findOuterBoundaryLeafAsync(ops, r, lowestPrimary(ensureValues(v as any)), -1, comparator),
        direction: -1, earlyTerminate: false,
      },
    },
    like: {
      asc: {
        start: (r, ops) => leftestNodeAsync(ops, r),
        end: async () => null, direction: 1, earlyTerminate: false,
      },
      desc: {
        start: (r, ops) => rightestNodeAsync(ops, r),
        end: async () => null, direction: -1, earlyTerminate: false,
      },
    },
  }
}

// ─── Tree traversal (async) ──────────────────────────────────────────

export async function locateLeafAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, rootId: string, value: V, comparator: ValueComparator<V>,
): Promise<BPTreeLeafNode<K, V>> {
  let node = await ops.getNode(rootId)
  while (!node.leaf) {
    const { index } = binarySearchValues(node.values, value, comparator, false, true)
    node = await ops.getNode(node.keys[index])
  }
  return node as BPTreeLeafNode<K, V>
}

export async function findLowerBoundLeafAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, rootId: string, value: V, comparator: ValueComparator<V>,
): Promise<BPTreeLeafNode<K, V>> {
  let node = await ops.getNode(rootId)
  while (!node.leaf) {
    const { index } = binarySearchValues(node.values, value, comparator, true, false)
    node = await ops.getNode(node.keys[index])
  }
  return node as BPTreeLeafNode<K, V>
}

export async function findUpperBoundLeafAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, rootId: string, value: V, comparator: ValueComparator<V>,
): Promise<BPTreeLeafNode<K, V>> {
  let node = await ops.getNode(rootId)
  while (!node.leaf) {
    const { index } = binarySearchValues(node.values, value, comparator, true, true)
    node = await ops.getNode(node.keys[index])
  }
  return node as BPTreeLeafNode<K, V>
}

export async function findOuterBoundaryLeafAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, rootId: string, value: V, direction: 1 | -1, comparator: ValueComparator<V>,
): Promise<BPTreeLeafNode<K, V> | null> {
  const insertableNode = direction === -1
    ? await findLowerBoundLeafAsync(ops, rootId, value, comparator)
    : await findUpperBoundLeafAsync(ops, rootId, value, comparator)
  const key: 'next' | 'prev' = direction === -1 ? 'prev' : 'next'
  const guessNode = insertableNode[key]
  if (!guessNode) return null
  return await ops.getNode(guessNode) as BPTreeLeafNode<K, V>
}

export async function leftestNodeAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, rootId: string,
): Promise<BPTreeLeafNode<K, V>> {
  let node = await ops.getNode(rootId)
  while (!node.leaf) {
    node = await ops.getNode(node.keys[0])
  }
  return node as BPTreeLeafNode<K, V>
}

export async function rightestNodeAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, rootId: string,
): Promise<BPTreeLeafNode<K, V>> {
  let node = await ops.getNode(rootId)
  while (!node.leaf) {
    node = await ops.getNode(node.keys[node.keys.length - 1])
  }
  return node as BPTreeLeafNode<K, V>
}

// ─── Generator (async) ──────────────────────────────────────────────

export async function* getPairsGeneratorAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>,
  startNode: BPTreeLeafNode<K, V>,
  endNode: BPTreeLeafNode<K, V> | null,
  direction: 1 | -1,
): AsyncGenerator<[K, V]> {
  let node = startNode
  let nextNodePromise: Promise<BPTreeUnknownNode<K, V>> | null = null

  while (true) {
    if (endNode && node.id === endNode.id) break

    // Read-ahead
    if (direction === 1) {
      if (node.next) nextNodePromise = ops.getNode(node.next)
    } else {
      if (node.prev) nextNodePromise = ops.getNode(node.prev)
    }

    const len = node.values.length
    if (direction === 1) {
      for (let i = 0; i < len; i++) {
        const nValue = node.values[i]
        const keys = node.keys[i]
        for (let j = 0, kLen = keys.length; j < kLen; j++) {
          yield [keys[j], nValue]
        }
      }
    } else {
      let i = len
      while (i--) {
        const nValue = node.values[i]
        const keys = node.keys[i]
        let j = keys.length
        while (j--) { yield [keys[j], nValue] }
      }
    }

    if (nextNodePromise) {
      node = await nextNodePromise as BPTreeLeafNode<K, V>
      nextNodePromise = null
    } else {
      break
    }
  }
}

// ─── Mutation: insert (async) ────────────────────────────────────────

export async function insertAtLeafAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, node: BPTreeUnknownNode<K, V>,
  key: BPTreeNodeKey<K>, value: V, comparator: ValueComparator<V>,
): Promise<BPTreeUnknownNode<K, V>> {
  let leaf = node as BPTreeLeafNode<K, V>
  if (leaf.values.length) {
    for (let i = 0, len = leaf.values.length; i < len; i++) {
      const nValue = leaf.values[i]
      if (comparator.isSame(value, nValue)) {
        if (leaf.keys[i].includes(key as K)) break
        leaf = cloneNode(leaf)
        leaf.keys[i].push(key as K)
        await ops.updateNode(leaf)
        return leaf
      } else if (comparator.isLower(value, nValue)) {
        leaf = cloneNode(leaf)
        leaf.values.splice(i, 0, value)
        leaf.keys.splice(i, 0, [key as K])
        await ops.updateNode(leaf)
        return leaf
      } else if (i + 1 === leaf.values.length) {
        leaf = cloneNode(leaf)
        leaf.values.push(value)
        leaf.keys.push([key as K])
        await ops.updateNode(leaf)
        return leaf
      }
    }
  } else {
    leaf = cloneNode(leaf)
    leaf.values = [value]
    leaf.keys = [[key as K]]
    await ops.updateNode(leaf)
    return leaf
  }
  return leaf
}

export async function insertInParentAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, ctx: BPTreeAlgoContext<K, V>,
  node: BPTreeUnknownNode<K, V>, value: V, newSiblingNode: BPTreeUnknownNode<K, V>,
): Promise<void> {
  if (ctx.rootId === node.id) {
    node = cloneNode(node)
    newSiblingNode = cloneNode(newSiblingNode)
    const root = await ops.createNode(false, [node.id, newSiblingNode.id], [value])
    ctx.rootId = root.id
    node.parent = root.id
    newSiblingNode.parent = root.id
    if (newSiblingNode.leaf) {
      ; (node as any).next = newSiblingNode.id
        ; (newSiblingNode as any).prev = node.id
    }
    await ops.writeHead({ root: root.id, order: ctx.order, data: ctx.headData() })
    await ops.updateNode(node)
    await ops.updateNode(newSiblingNode)
    return
  }

  const parentNode = cloneNode(await ops.getNode(node.parent!)) as BPTreeInternalNode<K, V>
  const nodeIndex = parentNode.keys.indexOf(node.id)
  if (nodeIndex === -1) throw new Error(`Node ${node.id} not found in parent ${parentNode.id}`)

  parentNode.values.splice(nodeIndex, 0, value)
  parentNode.keys.splice(nodeIndex + 1, 0, newSiblingNode.id)

  newSiblingNode = cloneNode(newSiblingNode)
  newSiblingNode.parent = parentNode.id

  if (newSiblingNode.leaf) {
    const leftSibling = cloneNode(node) as unknown as BPTreeLeafNode<K, V>
    const oldNextId = leftSibling.next
    newSiblingNode.prev = leftSibling.id
    newSiblingNode.next = oldNextId
    leftSibling.next = newSiblingNode.id
    await ops.updateNode(leftSibling)
    if (oldNextId) {
      const oldNext = cloneNode(await ops.getNode(oldNextId)) as BPTreeLeafNode<K, V>
      oldNext.prev = newSiblingNode.id
      await ops.updateNode(oldNext)
    }
  }

  await ops.updateNode(parentNode)
  await ops.updateNode(newSiblingNode)

  if (parentNode.keys.length > ctx.order) {
    const rec = await ops.createNode(false, [], []) as BPTreeInternalNode<K, V>
    rec.parent = parentNode.parent
    const mid = Math.ceil(ctx.order / 2) - 1
    rec.values = parentNode.values.slice(mid + 1)
    rec.keys = parentNode.keys.slice(mid + 1)
    const midValue = parentNode.values[mid]
    parentNode.values = parentNode.values.slice(0, mid)
    parentNode.keys = parentNode.keys.slice(0, mid + 1)
    for (let i = 0, len = rec.keys.length; i < len; i++) {
      const n = cloneNode(await ops.getNode(rec.keys[i]))
      n.parent = rec.id
      await ops.updateNode(n)
    }
    await ops.updateNode(parentNode)
    await insertInParentAsync(ops, ctx, parentNode, midValue, rec)
  }
}

export async function insertOpAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, ctx: BPTreeAlgoContext<K, V>,
  key: K, value: V, comparator: ValueComparator<V>,
): Promise<void> {
  let before = await locateLeafAsync(ops, ctx.rootId, value, comparator)
  before = await insertAtLeafAsync(ops, before, key, value, comparator) as BPTreeLeafNode<K, V>
  if (before.values.length === ctx.order) {
    let after = await ops.createNode(true, [], [], before.parent, null, null) as BPTreeLeafNode<K, V>
    const mid = Math.ceil(ctx.order / 2) - 1
    after = cloneNode(after)
    after.values = before.values.slice(mid + 1)
    after.keys = before.keys.slice(mid + 1)
    before.values = before.values.slice(0, mid + 1)
    before.keys = before.keys.slice(0, mid + 1)
    await ops.updateNode(before)
    await ops.updateNode(after)
    await insertInParentAsync(ops, ctx, before, after.values[0], after)
  }
}

// ─── Mutation: delete (async) ────────────────────────────────────────

export async function deleteEntryAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, ctx: BPTreeAlgoContext<K, V>,
  node: BPTreeUnknownNode<K, V>, key: BPTreeNodeKey<K>, comparator: ValueComparator<V>,
): Promise<BPTreeUnknownNode<K, V>> {
  if (!node.leaf) {
    let keyIndex = -1
    for (let i = 0, len = node.keys.length; i < len; i++) {
      if (node.keys[i] === key) { keyIndex = i; break }
    }
    if (keyIndex !== -1) {
      node = cloneNode(node)
      node.keys.splice(keyIndex, 1)
      const valueIndex = keyIndex > 0 ? keyIndex - 1 : 0
      node.values.splice(valueIndex, 1)
      await ops.updateNode(node)
    }
  }

  if (ctx.rootId === node.id && node.keys.length === 1 && !node.leaf) {
    const keys = node.keys as string[]
    await ops.deleteNode(node)
    const newRoot = cloneNode(await ops.getNode(keys[0]))
    newRoot.parent = null
    await ops.updateNode(newRoot)
    await ops.writeHead({ root: newRoot.id, order: ctx.order, data: ctx.headData() })
    ctx.rootId = newRoot.id
    return node
  } else if (ctx.rootId === node.id) {
    await ops.writeHead({ root: node.id, order: ctx.order, data: ctx.headData() })
    return node
  } else if (
    (node.keys.length < Math.ceil(ctx.order / 2) && !node.leaf) ||
    (node.values.length < Math.ceil((ctx.order - 1) / 2) && node.leaf)
  ) {
    if (node.parent === null) return node
    let isPredecessor = false
    let parentNode = await ops.getNode(node.parent) as BPTreeInternalNode<K, V>
    let prevNode: BPTreeInternalNode<K, V> | null = null
    let nextNode: BPTreeInternalNode<K, V> | null = null
    let prevValue: V | null = null
    let postValue: V | null = null

    for (let i = 0, len = parentNode.keys.length; i < len; i++) {
      if (parentNode.keys[i] === node.id) {
        if (i > 0) {
          prevNode = await ops.getNode(parentNode.keys[i - 1]) as BPTreeInternalNode<K, V>
          prevValue = parentNode.values[i - 1]
        }
        if (i < parentNode.keys.length - 1) {
          nextNode = await ops.getNode(parentNode.keys[i + 1]) as BPTreeInternalNode<K, V>
          postValue = parentNode.values[i]
        }
      }
    }

    let siblingNode: BPTreeUnknownNode<K, V>
    let guess: V | null
    if (prevNode === null) { siblingNode = nextNode!; guess = postValue }
    else if (nextNode === null) { isPredecessor = true; siblingNode = prevNode; guess = prevValue }
    else {
      if (node.values.length + nextNode.values.length < ctx.order) { siblingNode = nextNode; guess = postValue }
      else { isPredecessor = true; siblingNode = prevNode; guess = prevValue }
    }
    if (!siblingNode!) return node

    node = cloneNode(node)
    siblingNode = cloneNode(siblingNode)

    if (node.values.length + siblingNode.values.length < ctx.order) {
      if (!isPredecessor) { const t = siblingNode; siblingNode = node as any; node = t }
      siblingNode.keys.push(...node.keys as any)
      if (!node.leaf) { siblingNode.values.push(guess!) }
      else {
        siblingNode.next = node.next
        if (siblingNode.next) {
          const n = cloneNode(await ops.getNode(siblingNode.next))
          n.prev = siblingNode.id
          await ops.updateNode(n)
        }
      }
      siblingNode.values.push(...node.values)
      if (!siblingNode.leaf) {
        for (let i = 0, len = siblingNode.keys.length; i < len; i++) {
          const n = cloneNode(await ops.getNode(siblingNode.keys[i]))
          n.parent = siblingNode.id
          await ops.updateNode(n)
        }
      }
      await ops.deleteNode(node)
      await ops.updateNode(siblingNode)
      await deleteEntryAsync(ops, ctx, await ops.getNode(node.parent!), node.id, comparator)
    } else {
      if (isPredecessor) {
        let pointerPm, pointerKm
        if (!node.leaf) {
          pointerPm = siblingNode.keys.splice(-1)[0]
          pointerKm = siblingNode.values.splice(-1)[0]
          node.keys = [pointerPm, ...node.keys]
          node.values = [guess!, ...node.values]
          parentNode = cloneNode(await ops.getNode(node.parent!)) as BPTreeInternalNode<K, V>
          const ni = parentNode.keys.indexOf(node.id)
          if (ni > 0) { parentNode.values[ni - 1] = pointerKm; await ops.updateNode(parentNode) }
        } else {
          pointerPm = siblingNode.keys.splice(-1)[0] as unknown as K[]
          pointerKm = siblingNode.values.splice(-1)[0]
          node.keys = [pointerPm, ...node.keys]
          node.values = [pointerKm, ...node.values]
          parentNode = cloneNode(await ops.getNode(node.parent!)) as BPTreeInternalNode<K, V>
          const ni = parentNode.keys.indexOf(node.id)
          if (ni > 0) { parentNode.values[ni - 1] = pointerKm; await ops.updateNode(parentNode) }
        }
        await ops.updateNode(node)
        await ops.updateNode(siblingNode)
      } else {
        let pointerP0, pointerK0
        if (!node.leaf) {
          pointerP0 = siblingNode.keys.splice(0, 1)[0]
          pointerK0 = siblingNode.values.splice(0, 1)[0]
          node.keys = [...node.keys, pointerP0]
          node.values = [...node.values, guess!]
          parentNode = cloneNode(await ops.getNode(node.parent!)) as BPTreeInternalNode<K, V>
          const pi = parentNode.keys.indexOf(siblingNode.id)
          if (pi > 0) { parentNode.values[pi - 1] = pointerK0; await ops.updateNode(parentNode) }
        } else {
          pointerP0 = siblingNode.keys.splice(0, 1)[0] as unknown as K[]
          pointerK0 = siblingNode.values.splice(0, 1)[0]
          node.keys = [...node.keys, pointerP0]
          node.values = [...node.values, pointerK0]
          parentNode = cloneNode(await ops.getNode(node.parent!)) as BPTreeInternalNode<K, V>
          const pi = parentNode.keys.indexOf(siblingNode.id)
          if (pi > 0) { parentNode.values[pi - 1] = siblingNode.values[0]; await ops.updateNode(parentNode) }
        }
        await ops.updateNode(node)
        await ops.updateNode(siblingNode)
      }
      if (!siblingNode.leaf) {
        for (let i = 0, len = siblingNode.keys.length; i < len; i++) {
          const n = cloneNode(await ops.getNode(siblingNode.keys[i]))
          n.parent = siblingNode.id; await ops.updateNode(n)
        }
      }
      if (!node.leaf) {
        for (let i = 0, len = node.keys.length; i < len; i++) {
          const n = cloneNode(await ops.getNode(node.keys[i]))
          n.parent = node.id; await ops.updateNode(n)
        }
      }
      if (!parentNode.leaf) {
        for (let i = 0, len = parentNode.keys.length; i < len; i++) {
          const n = cloneNode(await ops.getNode(parentNode.keys[i]))
          n.parent = parentNode.id; await ops.updateNode(n)
        }
      }
    }
  } else {
    await ops.updateNode(cloneNode(node))
  }
  return node
}

export async function deleteOpAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, ctx: BPTreeAlgoContext<K, V>,
  key: K, comparator: ValueComparator<V>, value?: V,
): Promise<void> {
  if (value === undefined) {
    value = await getOpAsync(ops, ctx.rootId, key)
  }
  if (value === undefined) return

  let node = await findLowerBoundLeafAsync(ops, ctx.rootId, value, comparator)
  let found = false
  while (true) {
    let i = node.values.length
    while (i--) {
      if (comparator.isSame(value!, node.values[i])) {
        const keyIndex = node.keys[i].indexOf(key)
        if (keyIndex !== -1) {
          node = cloneNode(node)
          node.keys[i].splice(keyIndex, 1)
          if (node.keys[i].length === 0) {
            node.keys.splice(i, 1)
            node.values.splice(i, 1)
          }
          await ops.updateNode(node)
          node = await deleteEntryAsync(ops, ctx, node, key, comparator) as BPTreeLeafNode<K, V>
          found = true
          break
        }
      }
    }
    if (found) break
    if (node.next) { node = await ops.getNode(node.next) as BPTreeLeafNode<K, V>; continue }
    break
  }
}

// ─── Mutation: batchInsert (async) ───────────────────────────────────

export async function batchInsertOpAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, ctx: BPTreeAlgoContext<K, V>,
  entries: [K, V][], comparator: ValueComparator<V>,
): Promise<void> {
  if (entries.length === 0) return
  const sorted = [...entries].sort((a, b) => comparator.asc(a[1], b[1]))
  let currentLeaf: BPTreeLeafNode<K, V> | null = null
  let modified = false
  let cachedLeafId: string | null = null
  let cachedLeafMaxValue: V | null = null

  for (let i = 0, len = sorted.length; i < len; i++) {
    const [key, value] = sorted[i]
    let targetLeaf: BPTreeLeafNode<K, V>
    if (cachedLeafId !== null && cachedLeafMaxValue !== null && currentLeaf !== null &&
      (comparator.isLower(value, cachedLeafMaxValue) || comparator.isSame(value, cachedLeafMaxValue))) {
      targetLeaf = currentLeaf
    } else {
      targetLeaf = await locateLeafAsync(ops, ctx.rootId, value, comparator)
    }

    if (currentLeaf !== null && currentLeaf.id === targetLeaf.id) { /* same leaf */ }
    else {
      if (currentLeaf !== null && modified) await ops.updateNode(currentLeaf)
      currentLeaf = cloneNode(targetLeaf)
      modified = false
    }

    cachedLeafId = currentLeaf.id
    const changed = insertValueIntoLeaf(currentLeaf, key as K, value, comparator)
    modified = modified || changed
    cachedLeafMaxValue = currentLeaf.values[currentLeaf.values.length - 1]

    if (currentLeaf.values.length === ctx.order) {
      await ops.updateNode(currentLeaf)
      let after = await ops.createNode(true, [], [], currentLeaf.parent, null, null) as BPTreeLeafNode<K, V>
      const mid = Math.ceil(ctx.order / 2) - 1
      after = cloneNode(after)
      after.values = currentLeaf.values.slice(mid + 1)
      after.keys = currentLeaf.keys.slice(mid + 1)
      currentLeaf.values = currentLeaf.values.slice(0, mid + 1)
      currentLeaf.keys = currentLeaf.keys.slice(0, mid + 1)
      await ops.updateNode(currentLeaf)
      await ops.updateNode(after)
      await insertInParentAsync(ops, ctx, currentLeaf, after.values[0], after)
      currentLeaf = null; cachedLeafId = null; cachedLeafMaxValue = null; modified = false
    }
  }
  if (currentLeaf !== null && modified) await ops.updateNode(currentLeaf)
}

// ─── Mutation: bulkLoad (async) ──────────────────────────────────────

export async function bulkLoadOpAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, ctx: BPTreeAlgoContext<K, V>,
  entries: [K, V][], comparator: ValueComparator<V>,
): Promise<void> {
  if (entries.length === 0) return
  const root = await ops.getNode(ctx.rootId)
  if (!root.leaf || root.values.length > 0) {
    throw new Error('bulkLoad can only be called on an empty tree. Use batchInsert for non-empty trees.')
  }
  const sorted = [...entries].sort((a, b) => comparator.asc(a[1], b[1]))
  const grouped: { keys: K[], value: V }[] = []
  for (let i = 0, len = sorted.length; i < len; i++) {
    const [key, value] = sorted[i]
    const last = grouped[grouped.length - 1]
    if (last && comparator.isSame(last.value, value)) {
      if (!last.keys.includes(key)) last.keys.push(key)
    } else { grouped.push({ keys: [key], value }) }
  }
  await ops.deleteNode(root)
  const maxLeafSize = ctx.order - 1
  const leaves: BPTreeLeafNode<K, V>[] = []
  for (let i = 0, len = grouped.length; i < len; i += maxLeafSize) {
    const chunk = grouped.slice(i, i + maxLeafSize)
    const leaf = await ops.createNode(true, chunk.map(g => g.keys), chunk.map(g => g.value), null, null, null) as BPTreeLeafNode<K, V>
    leaves.push(leaf)
  }
  for (let i = 0, len = leaves.length; i < len; i++) {
    if (i > 0) leaves[i].prev = leaves[i - 1].id
    if (i < len - 1) leaves[i].next = leaves[i + 1].id
    await ops.updateNode(leaves[i])
  }
  let currentLevel: BPTreeUnknownNode<K, V>[] = leaves
  while (currentLevel.length > 1) {
    const nextLevel: BPTreeUnknownNode<K, V>[] = []
    for (let i = 0, len = currentLevel.length; i < len; i += ctx.order) {
      const children = currentLevel.slice(i, i + ctx.order)
      const separators: V[] = []
      for (let j = 1, cLen = children.length; j < cLen; j++) separators.push(children[j].values[0])
      const internalNode = await ops.createNode(false, children.map(c => c.id), separators, null, null, null) as BPTreeInternalNode<K, V>
      for (let j = 0, cLen = children.length; j < cLen; j++) {
        children[j].parent = internalNode.id
        await ops.updateNode(children[j])
      }
      nextLevel.push(internalNode)
    }
    currentLevel = nextLevel
  }
  const newRoot = currentLevel[0]
  await ops.writeHead({ root: newRoot.id, order: ctx.order, data: ctx.headData() })
  ctx.rootId = newRoot.id
}

// ─── Query (async) ───────────────────────────────────────────────────

export async function existsOpAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, rootId: string, key: K, value: V, comparator: ValueComparator<V>,
): Promise<boolean> {
  const node = await locateLeafAsync(ops, rootId, value, comparator)
  const { index, found } = binarySearchValues(node.values, value, comparator)
  if (found && node.keys[index].includes(key)) return true
  return false
}

export async function getOpAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, rootId: string, key: K,
): Promise<V | undefined> {
  let node = await leftestNodeAsync(ops, rootId)
  while (true) {
    for (let i = 0, len = node.values.length; i < len; i++) {
      const keys = node.keys[i]
      for (let j = 0, kLen = keys.length; j < kLen; j++) {
        if (keys[j] === key) return node.values[i]
      }
    }
    if (!node.next) break
    node = await ops.getNode(node.next) as BPTreeLeafNode<K, V>
  }
  return undefined
}

export async function* whereStreamOpAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, rootId: string,
  condition: BPTreeCondition<V>, comparator: ValueComparator<V>,
  verifierMap: Record<keyof BPTreeCondition<V>, (nodeValue: V, value: V | V[]) => boolean>,
  searchConfigs: Record<keyof BPTreeCondition<V>, Record<'asc' | 'desc', AsyncSearchConfigEntry<K, V>>>,
  ensureValues: (v: V | V[]) => V[],
  options?: BPTreeSearchOption<K>,
): AsyncGenerator<[K, V]> {
  const { filterValues, limit, order = 'asc' } = options ?? {}
  const conditionKeys = Object.keys(condition)
  if (conditionKeys.length === 0) return

  const resolved = resolveStartEndConfigs(condition, order, comparator, ensureValues)
  const direction = resolved.direction

  let startNode: BPTreeLeafNode<K, V> | null
  if (resolved.startKey) {
    const startConfig = searchConfigs[resolved.startKey][order]
    startNode = await startConfig.start(rootId, ops, resolved.startValues) as BPTreeLeafNode<K, V> | null
  } else {
    startNode = order === 'asc'
      ? await leftestNodeAsync(ops, rootId)
      : await rightestNodeAsync(ops, rootId)
  }

  let endNode: BPTreeLeafNode<K, V> | null = null
  if (resolved.endKey) {
    const endConfig = searchConfigs[resolved.endKey][order]
    endNode = await endConfig.end(rootId, ops, resolved.endValues) as BPTreeLeafNode<K, V> | null
  }
  if (!startNode) return

  const generator = getPairsGeneratorAsync(ops, startNode, endNode, direction)
  let count = 0
  const intersection = filterValues && filterValues.size > 0 ? filterValues : null
  for await (const pair of generator) {
    const [k, v] = pair
    if (intersection && !intersection.has(k)) continue
    if (verify(v, condition, verifierMap)) {
      yield pair
      count++
      if (limit !== undefined && count >= limit) break
    }
  }
}

export async function* keysStreamOpAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, rootId: string,
  condition: BPTreeCondition<V>, comparator: ValueComparator<V>,
  verifierMap: Record<keyof BPTreeCondition<V>, (nodeValue: V, value: V | V[]) => boolean>,
  searchConfigs: Record<keyof BPTreeCondition<V>, Record<'asc' | 'desc', AsyncSearchConfigEntry<K, V>>>,
  ensureValues: (v: V | V[]) => V[],
  options?: BPTreeSearchOption<K>,
): AsyncGenerator<K> {
  const { filterValues, limit } = options ?? {}
  const stream = whereStreamOpAsync(ops, rootId, condition, comparator, verifierMap, searchConfigs, ensureValues, options)
  const intersection = filterValues && filterValues.size > 0 ? filterValues : null
  let count = 0
  for await (const [key] of stream) {
    if (intersection && !intersection.has(key)) continue
    yield key
    count++
    if (limit !== undefined && count >= limit) break
  }
}

// ─── Init (async) ────────────────────────────────────────────────────

export async function initOpAsync<K, V>(
  ops: BPTreeNodeOpsAsync<K, V>, ctx: BPTreeAlgoContext<K, V>,
  strategyOrder: number,
  strategyHead: { data: SerializableData },
  setStrategyHead: (head: SerializeStrategyHead) => void,
): Promise<void> {
  const head = await ops.readHead()
  if (head === null) {
    ctx.order = strategyOrder
    const root = await ops.createNode(true, [], [])
    await ops.writeHead({ root: root.id, order: ctx.order, data: strategyHead.data })
    ctx.rootId = root.id
  } else {
    const { root, order } = head
    setStrategyHead(head)
    ctx.order = order
    ctx.rootId = root!
  }
  if (ctx.order < 3) {
    throw new Error(`The 'order' parameter must be greater than 2. but got a '${ctx.order}'.`)
  }
}
