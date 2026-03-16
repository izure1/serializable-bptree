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
import type { BPTreeNodeOps, BPTreeAlgoContext } from './BPTreeNodeOps'
import { ValueComparator } from './ValueComparator'

// ─── Utility functions ───────────────────────────────────────────────

export function cloneNode<K, V, T extends BPTreeUnknownNode<K, V>>(node: T): T {
  return JSON.parse(JSON.stringify(node)) as T
}

export function binarySearchValues<V>(
  values: V[],
  target: V,
  comparator: ValueComparator<V>,
  usePrimary: boolean = false,
  upperBound: boolean = false
): { index: number, found: boolean } {
  let low = 0
  let high = values.length
  let found = false
  while (low < high) {
    const mid = (low + high) >>> 1
    const cmp = usePrimary
      ? comparator.primaryAsc(target, values[mid])
      : comparator.asc(target, values[mid])
    if (cmp === 0) {
      found = true
      if (upperBound) low = mid + 1
      else high = mid
    }
    else if (cmp < 0) {
      high = mid
    }
    else {
      low = mid + 1
    }
  }
  return { index: low, found }
}

export function insertValueIntoLeaf<K, V>(
  leaf: BPTreeLeafNode<K, V>,
  key: K,
  value: V,
  comparator: ValueComparator<V>,
): boolean {
  if (leaf.values.length) {
    const { index, found } = binarySearchValues(leaf.values, value, comparator)
    if (found) {
      if (leaf.keys[index].includes(key)) {
        return false
      }
      leaf.keys[index].push(key)
      return true
    }
    leaf.values.splice(index, 0, value)
    leaf.keys.splice(index, 0, [key])
    return true
  }
  else {
    leaf.values = [value]
    leaf.keys = [[key]]
    return true
  }
}

// ─── Verifier / Search config factories ──────────────────────────────

export function createVerifierMap<V>(
  comparator: ValueComparator<V>,
  cachedRegexp: Map<string, RegExp>,
  ensureValues: (v: V | V[]) => V[],
): Record<keyof BPTreeCondition<V>, (nodeValue: V, value: V | V[]) => boolean> {
  return {
    gt: (nv, v) => comparator.isHigher(nv, v as V),
    gte: (nv, v) => comparator.isHigher(nv, v as V) || comparator.isSame(nv, v as V),
    lt: (nv, v) => comparator.isLower(nv, v as V),
    lte: (nv, v) => comparator.isLower(nv, v as V) || comparator.isSame(nv, v as V),
    equal: (nv, v) => comparator.isSame(nv, v as V),
    notEqual: (nv, v) => comparator.isSame(nv, v as V) === false,
    or: (nv, v) => ensureValues(v).some((v) => comparator.isSame(nv, v)),
    primaryGt: (nv, v) => comparator.isPrimaryHigher(nv, v as V),
    primaryGte: (nv, v) => comparator.isPrimaryHigher(nv, v as V) || comparator.isPrimarySame(nv, v as V),
    primaryLt: (nv, v) => comparator.isPrimaryLower(nv, v as V),
    primaryLte: (nv, v) => comparator.isPrimaryLower(nv, v as V) || comparator.isPrimarySame(nv, v as V),
    primaryEqual: (nv, v) => comparator.isPrimarySame(nv, v as V),
    primaryNotEqual: (nv, v) => comparator.isPrimarySame(nv, v as V) === false,
    primaryOr: (nv, v) => ensureValues(v).some((v) => comparator.isPrimarySame(nv, v)),
    like: (nv, v) => {
      const nodeValue = comparator.match(nv)
      const value = v as unknown as string
      if (!cachedRegexp.has(value)) {
        const pattern = value.replace(/%/g, '.*').replace(/_/g, '.')
        const regexp = new RegExp(`^${pattern}$`, 'i')
        cachedRegexp.set(value, regexp)
      }
      const regexp = cachedRegexp.get(value) as RegExp
      return regexp.test(nodeValue)
    },
  }
}

type SearchConfigEntry<K, V> = {
  start: (rootId: string, ops: BPTreeNodeOps<K, V>, v: V[]) => BPTreeLeafNode<K, V> | null
  end: (rootId: string, ops: BPTreeNodeOps<K, V>, v: V[]) => BPTreeLeafNode<K, V> | null
  direction: 1 | -1
  earlyTerminate: boolean
}

export function createSearchConfigs<K, V>(
  comparator: ValueComparator<V>,
  ensureValues: (v: V | V[]) => V[],
): Record<keyof BPTreeCondition<V>, Record<'asc' | 'desc', SearchConfigEntry<K, V>>> {
  const lowest = (v: V[]): V => [...v].sort((a, b) => comparator.asc(a, b))[0]
  const highest = (v: V[]): V => [...v].sort((a, b) => comparator.asc(a, b))[v.length - 1]
  const lowestPrimary = (v: V[]): V => [...v].sort((a, b) => comparator.primaryAsc(a, b))[0]
  const highestPrimary = (v: V[]): V => [...v].sort((a, b) => comparator.primaryAsc(a, b))[v.length - 1]

  return {
    gt: {
      asc: {
        start: (r, ops, v) => findUpperBoundLeaf(ops, r, v[0], comparator),
        end: () => null,
        direction: 1,
        earlyTerminate: false,
      },
      desc: {
        start: (r, ops) => rightestNode(ops, r),
        end: (r, ops, v) => findOuterBoundaryLeaf(ops, r, v[0], -1, comparator),
        direction: -1,
        earlyTerminate: true,
      },
    },
    gte: {
      asc: {
        start: (r, ops, v) => findLowerBoundLeaf(ops, r, v[0], comparator),
        end: () => null,
        direction: 1,
        earlyTerminate: false,
      },
      desc: {
        start: (r, ops) => rightestNode(ops, r),
        end: (r, ops, v) => findOuterBoundaryLeaf(ops, r, v[0], -1, comparator),
        direction: -1,
        earlyTerminate: true,
      },
    },
    lt: {
      asc: {
        start: (r, ops) => leftestNode(ops, r),
        end: (r, ops, v) => findOuterBoundaryLeaf(ops, r, v[0], 1, comparator),
        direction: 1,
        earlyTerminate: true,
      },
      desc: {
        start: (r, ops, v) => findLowerBoundLeaf(ops, r, v[0], comparator),
        end: () => null,
        direction: -1,
        earlyTerminate: false,
      },
    },
    lte: {
      asc: {
        start: (r, ops) => leftestNode(ops, r),
        end: (r, ops, v) => findOuterBoundaryLeaf(ops, r, v[0], 1, comparator),
        direction: 1,
        earlyTerminate: true,
      },
      desc: {
        start: (r, ops, v) => findUpperBoundLeaf(ops, r, v[0], comparator),
        end: () => null,
        direction: -1,
        earlyTerminate: false,
      },
    },
    equal: {
      asc: {
        start: (r, ops, v) => findLowerBoundLeaf(ops, r, v[0], comparator),
        end: (r, ops, v) => findOuterBoundaryLeaf(ops, r, v[0], 1, comparator),
        direction: 1,
        earlyTerminate: true,
      },
      desc: {
        start: (r, ops, v) => findOuterBoundaryLeaf(ops, r, v[0], 1, comparator),
        end: (r, ops, v) => findOuterBoundaryLeaf(ops, r, v[0], -1, comparator),
        direction: -1,
        earlyTerminate: true,
      },
    },
    notEqual: {
      asc: {
        start: (r, ops) => leftestNode(ops, r),
        end: () => null,
        direction: 1,
        earlyTerminate: false,
      },
      desc: {
        start: (r, ops) => rightestNode(ops, r),
        end: () => null,
        direction: -1,
        earlyTerminate: false,
      },
    },
    or: {
      asc: {
        start: (r, ops, v) => findLowerBoundLeaf(ops, r, lowest(ensureValues(v as any)), comparator),
        end: (r, ops, v) => findOuterBoundaryLeaf(ops, r, highest(ensureValues(v as any)), 1, comparator),
        direction: 1,
        earlyTerminate: false,
      },
      desc: {
        start: (r, ops, v) => findOuterBoundaryLeaf(ops, r, highest(ensureValues(v as any)), 1, comparator),
        end: (r, ops, v) => findOuterBoundaryLeaf(ops, r, lowest(ensureValues(v as any)), -1, comparator),
        direction: -1,
        earlyTerminate: false,
      },
    },
    primaryGt: {
      asc: {
        start: (r, ops, v) => findUpperBoundLeaf(ops, r, v[0], comparator),
        end: () => null,
        direction: 1,
        earlyTerminate: false,
      },
      desc: {
        start: (r, ops) => rightestNode(ops, r),
        end: (r, ops, v) => findOuterBoundaryLeaf(ops, r, v[0], -1, comparator),
        direction: -1,
        earlyTerminate: true,
      },
    },
    primaryGte: {
      asc: {
        start: (r, ops, v) => findLowerBoundLeaf(ops, r, v[0], comparator),
        end: () => null,
        direction: 1,
        earlyTerminate: false,
      },
      desc: {
        start: (r, ops) => rightestNode(ops, r),
        end: (r, ops, v) => findOuterBoundaryLeaf(ops, r, v[0], -1, comparator),
        direction: -1,
        earlyTerminate: true,
      },
    },
    primaryLt: {
      asc: {
        start: (r, ops) => leftestNode(ops, r),
        end: (r, ops, v) => findOuterBoundaryLeaf(ops, r, v[0], 1, comparator),
        direction: 1,
        earlyTerminate: true,
      },
      desc: {
        start: (r, ops, v) => findLowerBoundLeaf(ops, r, v[0], comparator),
        end: () => null,
        direction: -1,
        earlyTerminate: false,
      },
    },
    primaryLte: {
      asc: {
        start: (r, ops) => leftestNode(ops, r),
        end: (r, ops, v) => findOuterBoundaryLeaf(ops, r, v[0], 1, comparator),
        direction: 1,
        earlyTerminate: true,
      },
      desc: {
        start: (r, ops, v) => findUpperBoundLeaf(ops, r, v[0], comparator),
        end: () => null,
        direction: -1,
        earlyTerminate: false,
      },
    },
    primaryEqual: {
      asc: {
        start: (r, ops, v) => findLowerBoundLeaf(ops, r, v[0], comparator),
        end: (r, ops, v) => findOuterBoundaryLeaf(ops, r, v[0], 1, comparator),
        direction: 1,
        earlyTerminate: true,
      },
      desc: {
        start: (r, ops, v) => findUpperBoundLeaf(ops, r, v[0], comparator),
        end: (r, ops, v) => findOuterBoundaryLeaf(ops, r, v[0], -1, comparator),
        direction: -1,
        earlyTerminate: true,
      },
    },
    primaryNotEqual: {
      asc: {
        start: (r, ops) => leftestNode(ops, r),
        end: () => null,
        direction: 1,
        earlyTerminate: false,
      },
      desc: {
        start: (r, ops) => rightestNode(ops, r),
        end: () => null,
        direction: -1,
        earlyTerminate: false,
      },
    },
    primaryOr: {
      asc: {
        start: (r, ops, v) => findLowerBoundLeaf(ops, r, lowestPrimary(ensureValues(v as any)), comparator),
        end: (r, ops, v) => findOuterBoundaryLeaf(ops, r, highestPrimary(ensureValues(v as any)), 1, comparator),
        direction: 1,
        earlyTerminate: false,
      },
      desc: {
        start: (r, ops, v) => findUpperBoundLeaf(ops, r, highestPrimary(ensureValues(v as any)), comparator),
        end: (r, ops, v) => findOuterBoundaryLeaf(ops, r, lowestPrimary(ensureValues(v as any)), -1, comparator),
        direction: -1,
        earlyTerminate: false,
      },
    },
    like: {
      asc: {
        start: (r, ops) => leftestNode(ops, r),
        end: () => null,
        direction: 1,
        earlyTerminate: false,
      },
      desc: {
        start: (r, ops) => rightestNode(ops, r),
        end: () => null,
        direction: -1,
        earlyTerminate: false,
      },
    },
  }
}

// Lower bound providers, ordered by selectivity (tightest first)
const _lowerBoundKeys: (keyof BPTreeCondition<unknown>)[] = [
  'primaryEqual', 'equal',
  'primaryGt', 'gt', 'primaryGte', 'gte',
  'primaryOr', 'or',
]

// Upper bound providers, ordered by selectivity (tightest first)
const _upperBoundKeys: (keyof BPTreeCondition<unknown>)[] = [
  'primaryEqual', 'equal',
  'primaryLt', 'lt', 'primaryLte', 'lte',
  'primaryOr', 'or',
]

const _multiValueKeys: (keyof BPTreeCondition<unknown>)[] = [
  'or',
  'primaryOr',
]

export function resolveStartEndConfigs<V>(
  condition: BPTreeCondition<V>,
  order: 'asc' | 'desc',
  comparator: ValueComparator<V>,
  ensureValues: (v: V | V[]) => V[],
): {
  startKey: keyof BPTreeCondition<V> | null
  endKey: keyof BPTreeCondition<V> | null
  startValues: V[]
  endValues: V[]
  direction: 1 | -1
} {
  const direction: 1 | -1 = order === 'asc' ? 1 : -1

  const startCandidates = order === 'asc' ? _lowerBoundKeys : _upperBoundKeys
  const endCandidates = order === 'asc' ? _upperBoundKeys : _lowerBoundKeys

  let startKey: keyof BPTreeCondition<V> | null = null
  let endKey: keyof BPTreeCondition<V> | null = null
  let startValues: V[] = []
  let endValues: V[] = []

  for (let i = 0, len = startCandidates.length; i < len; i++) {
    const key = startCandidates[i]
    if (key in condition) {
      startKey = key
      startValues = _multiValueKeys.includes(key)
        ? ensureValues(condition[key] as V)
        : [condition[key] as V]
      break
    }
  }

  for (let i = 0, len = endCandidates.length; i < len; i++) {
    const key = endCandidates[i]
    if (key in condition) {
      endKey = key
      endValues = _multiValueKeys.includes(key)
        ? ensureValues(condition[key] as V)
        : [condition[key] as V]
      break
    }
  }

  return { startKey, endKey, startValues, endValues, direction }
}

export function verify<V>(
  nodeValue: V,
  condition: BPTreeCondition<V>,
  verifierMap: Record<keyof BPTreeCondition<V>, (nodeValue: V, value: V | V[]) => boolean>,
): boolean {
  for (const key in condition) {
    const verifyFn = verifierMap[key as keyof BPTreeCondition<V>]
    const condValue = condition[key as keyof BPTreeCondition<V>] as V
    if (!verifyFn(nodeValue, condValue)) {
      return false
    }
  }
  return true
}

// ─── Tree traversal ──────────────────────────────────────────────────

export function locateLeaf<K, V>(
  ops: BPTreeNodeOps<K, V>,
  rootId: string,
  value: V,
  comparator: ValueComparator<V>,
): BPTreeLeafNode<K, V> {
  let node = ops.getNode(rootId)
  while (!node.leaf) {
    const { index } = binarySearchValues(node.values, value, comparator, false, true)
    node = ops.getNode(node.keys[index])
  }
  return node as BPTreeLeafNode<K, V>
}

export function findLowerBoundLeaf<K, V>(
  ops: BPTreeNodeOps<K, V>,
  rootId: string,
  value: V,
  comparator: ValueComparator<V>,
): BPTreeLeafNode<K, V> {
  let node = ops.getNode(rootId)
  while (!node.leaf) {
    const { index } = binarySearchValues(node.values, value, comparator, true, false)
    node = ops.getNode(node.keys[index])
  }
  return node as BPTreeLeafNode<K, V>
}

export function findUpperBoundLeaf<K, V>(
  ops: BPTreeNodeOps<K, V>,
  rootId: string,
  value: V,
  comparator: ValueComparator<V>,
): BPTreeLeafNode<K, V> {
  let node = ops.getNode(rootId)
  while (!node.leaf) {
    const { index } = binarySearchValues(node.values, value, comparator, true, true)
    node = ops.getNode(node.keys[index])
  }
  return node as BPTreeLeafNode<K, V>
}

export function findOuterBoundaryLeaf<K, V>(
  ops: BPTreeNodeOps<K, V>,
  rootId: string,
  value: V,
  direction: 1 | -1,
  comparator: ValueComparator<V>,
): BPTreeLeafNode<K, V> | null {
  const insertableNode = direction === -1
    ? findLowerBoundLeaf(ops, rootId, value, comparator)
    : findUpperBoundLeaf(ops, rootId, value, comparator)
  let key: 'next' | 'prev'
  switch (direction) {
    case -1:
      key = 'prev'
      break
    case +1:
      key = 'next'
      break
    default:
      throw new Error(`Direction must be -1 or 1. but got a ${direction}`)
  }
  const guessNode = insertableNode[key]
  if (!guessNode) {
    return null
  }
  return ops.getNode(guessNode) as BPTreeLeafNode<K, V>
}

export function leftestNode<K, V>(
  ops: BPTreeNodeOps<K, V>,
  rootId: string,
): BPTreeLeafNode<K, V> {
  let node = ops.getNode(rootId)
  while (!node.leaf) {
    const keys = node.keys
    node = ops.getNode(keys[0])
  }
  return node as BPTreeLeafNode<K, V>
}

export function rightestNode<K, V>(
  ops: BPTreeNodeOps<K, V>,
  rootId: string,
): BPTreeLeafNode<K, V> {
  let node = ops.getNode(rootId)
  while (!node.leaf) {
    const keys = node.keys
    node = ops.getNode(keys[keys.length - 1])
  }
  return node as BPTreeLeafNode<K, V>
}

// ─── Generator ───────────────────────────────────────────────────────

export function* getPairsGenerator<K, V>(
  ops: BPTreeNodeOps<K, V>,
  startNode: BPTreeLeafNode<K, V>,
  endNode: BPTreeLeafNode<K, V> | null,
  direction: 1 | -1,
): Generator<[K, V]> {
  let node = startNode

  while (true) {
    if (endNode && node.id === endNode.id) {
      break
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
    }
    else {
      let i = len
      while (i--) {
        const nValue = node.values[i]
        const keys = node.keys[i]
        let j = keys.length
        while (j--) {
          yield [keys[j], nValue]
        }
      }
    }

    if (direction === 1) {
      if (!node.next) break
      node = ops.getNode(node.next) as BPTreeLeafNode<K, V>
    }
    else {
      if (!node.prev) break
      node = ops.getNode(node.prev) as BPTreeLeafNode<K, V>
    }
  }
}

// ─── Mutation: insert ────────────────────────────────────────────────

export function insertAtLeaf<K, V>(
  ops: BPTreeNodeOps<K, V>,
  node: BPTreeUnknownNode<K, V>,
  key: BPTreeNodeKey<K>,
  value: V,
  comparator: ValueComparator<V>,
): BPTreeUnknownNode<K, V> {
  let leaf = node as BPTreeLeafNode<K, V>
  if (leaf.values.length) {
    for (let i = 0, len = leaf.values.length; i < len; i++) {
      const nValue = leaf.values[i]
      if (comparator.isSame(value, nValue)) {
        const keys = leaf.keys[i]
        if (keys.includes(key as K)) {
          break
        }
        leaf = cloneNode(leaf)
        leaf.keys[i].push(key as K)
        ops.updateNode(leaf)
        return leaf
      }
      else if (comparator.isLower(value, nValue)) {
        leaf = cloneNode(leaf)
        leaf.values.splice(i, 0, value)
        leaf.keys.splice(i, 0, [key as K])
        ops.updateNode(leaf)
        return leaf
      }
      else if (i + 1 === leaf.values.length) {
        leaf = cloneNode(leaf)
        leaf.values.push(value)
        leaf.keys.push([key as K])
        ops.updateNode(leaf)
        return leaf
      }
    }
  }
  else {
    leaf = cloneNode(leaf)
    leaf.values = [value]
    leaf.keys = [[key as K]]
    ops.updateNode(leaf)
    return leaf
  }
  return leaf
}

export function insertInParent<K, V>(
  ops: BPTreeNodeOps<K, V>,
  ctx: BPTreeAlgoContext<K, V>,
  node: BPTreeUnknownNode<K, V>,
  value: V,
  newSiblingNode: BPTreeUnknownNode<K, V>,
): void {
  if (ctx.rootId === node.id) {
    node = cloneNode(node)
    newSiblingNode = cloneNode(newSiblingNode)
    const root = ops.createNode(false, [node.id, newSiblingNode.id], [value])
    ctx.rootId = root.id
    node.parent = root.id
    newSiblingNode.parent = root.id

    if (newSiblingNode.leaf) {
      ; (node as any).next = newSiblingNode.id
        ; (newSiblingNode as any).prev = node.id
    }

    ops.writeHead({
      root: root.id,
      order: ctx.order,
      data: ctx.headData(),
    })

    ops.updateNode(node)
    ops.updateNode(newSiblingNode)
    return
  }

  const parentNode = cloneNode(ops.getNode(node.parent!)) as BPTreeInternalNode<K, V>
  const nodeIndex = parentNode.keys.indexOf(node.id)

  if (nodeIndex === -1) {
    throw new Error(`Node ${node.id} not found in parent ${parentNode.id}`)
  }

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

    ops.updateNode(leftSibling)

    if (oldNextId) {
      const oldNext = cloneNode(ops.getNode(oldNextId)) as BPTreeLeafNode<K, V>
      oldNext.prev = newSiblingNode.id
      ops.updateNode(oldNext)
    }
  }

  ops.updateNode(parentNode)
  ops.updateNode(newSiblingNode)

  if (parentNode.keys.length > ctx.order) {
    const newSiblingNodeRecursive = ops.createNode(false, [], []) as BPTreeInternalNode<K, V>
    newSiblingNodeRecursive.parent = parentNode.parent
    const mid = Math.ceil(ctx.order / 2) - 1
    newSiblingNodeRecursive.values = parentNode.values.slice(mid + 1)
    newSiblingNodeRecursive.keys = parentNode.keys.slice(mid + 1)
    const midValue = parentNode.values[mid]
    parentNode.values = parentNode.values.slice(0, mid)
    parentNode.keys = parentNode.keys.slice(0, mid + 1)

    for (let i = 0, len = newSiblingNodeRecursive.keys.length; i < len; i++) {
      const k = newSiblingNodeRecursive.keys[i]
      const n = cloneNode(ops.getNode(k))
      n.parent = newSiblingNodeRecursive.id
      ops.updateNode(n)
    }

    ops.updateNode(parentNode)
    insertInParent(ops, ctx, parentNode, midValue, newSiblingNodeRecursive)
  }
}

export function insertOp<K, V>(
  ops: BPTreeNodeOps<K, V>,
  ctx: BPTreeAlgoContext<K, V>,
  key: K,
  value: V,
  comparator: ValueComparator<V>,
): void {
  let before = locateLeaf(ops, ctx.rootId, value, comparator)
  before = insertAtLeaf(ops, before, key, value, comparator) as BPTreeLeafNode<K, V>

  if (before.values.length === ctx.order) {
    let after = ops.createNode(
      true, [], [], before.parent, null, null,
    ) as BPTreeLeafNode<K, V>
    const mid = Math.ceil(ctx.order / 2) - 1
    after = cloneNode(after)
    after.values = before.values.slice(mid + 1)
    after.keys = before.keys.slice(mid + 1)
    before.values = before.values.slice(0, mid + 1)
    before.keys = before.keys.slice(0, mid + 1)
    ops.updateNode(before)
    ops.updateNode(after)
    insertInParent(ops, ctx, before, after.values[0], after)
  }
}

// ─── Mutation: delete ────────────────────────────────────────────────

export function deleteEntry<K, V>(
  ops: BPTreeNodeOps<K, V>,
  ctx: BPTreeAlgoContext<K, V>,
  node: BPTreeUnknownNode<K, V>,
  key: BPTreeNodeKey<K>,
  comparator: ValueComparator<V>,
): BPTreeUnknownNode<K, V> {
  if (!node.leaf) {
    let keyIndex = node.keys.indexOf(key as string)
    if (keyIndex !== -1) {
      node = cloneNode(node)
      node.keys.splice(keyIndex, 1)
      const valueIndex = keyIndex > 0 ? keyIndex - 1 : 0
      node.values.splice(valueIndex, 1)
      ops.updateNode(node)
    }
  }

  if (ctx.rootId === node.id && node.keys.length === 1 && !node.leaf) {
    const keys = node.keys as string[]
    ops.deleteNode(node)
    const newRoot = cloneNode(ops.getNode(keys[0]))
    newRoot.parent = null
    ops.updateNode(newRoot)
    ops.writeHead({
      root: newRoot.id,
      order: ctx.order,
      data: ctx.headData(),
    })
    ctx.rootId = newRoot.id
    return node
  }
  else if (ctx.rootId === node.id) {
    ops.writeHead({
      root: node.id,
      order: ctx.order,
      data: ctx.headData(),
    })
    return node
  }
  else if (
    (node.keys.length < Math.ceil(ctx.order / 2) && !node.leaf) ||
    (node.values.length < Math.ceil((ctx.order - 1) / 2) && node.leaf)
  ) {
    if (node.parent === null) {
      return node
    }
    let isPredecessor = false
    let parentNode = ops.getNode(node.parent) as BPTreeInternalNode<K, V>
    let prevNode: BPTreeInternalNode<K, V> | null = null
    let nextNode: BPTreeInternalNode<K, V> | null = null
    let prevValue: V | null = null
    let postValue: V | null = null

    let keyIndex = parentNode.keys.indexOf(node.id as string)
    if (keyIndex !== -1) {
      if (keyIndex > 0) {
        prevNode = ops.getNode(parentNode.keys[keyIndex - 1]) as BPTreeInternalNode<K, V>
        prevValue = parentNode.values[keyIndex - 1]
      }
      if (keyIndex < parentNode.keys.length - 1) {
        nextNode = ops.getNode(parentNode.keys[keyIndex + 1]) as BPTreeInternalNode<K, V>
        postValue = parentNode.values[keyIndex]
      }
    }

    let siblingNode: BPTreeUnknownNode<K, V>
    let guess: V | null
    if (prevNode === null) {
      siblingNode = nextNode!
      guess = postValue
    }
    else if (nextNode === null) {
      isPredecessor = true
      siblingNode = prevNode
      guess = prevValue
    }
    else {
      if (node.values.length + nextNode.values.length < ctx.order) {
        siblingNode = nextNode
        guess = postValue
      }
      else {
        isPredecessor = true
        siblingNode = prevNode
        guess = prevValue
      }
    }
    if (!siblingNode!) {
      return node
    }

    node = cloneNode(node)
    siblingNode = cloneNode(siblingNode)

    if (node.values.length + siblingNode.values.length < ctx.order) {
      if (!isPredecessor) {
        const pTemp = siblingNode
        siblingNode = node as BPTreeInternalNode<K, V>
        node = pTemp
      }
      siblingNode.keys = siblingNode.keys.concat(node.keys as any)
      if (!node.leaf) {
        siblingNode.values.push(guess!)
      }
      else {
        siblingNode.next = node.next
        if (siblingNode.next) {
          const n = cloneNode(ops.getNode(siblingNode.next))
          n.prev = siblingNode.id
          ops.updateNode(n)
        }
      }
      siblingNode.values = siblingNode.values.concat(node.values)

      if (!siblingNode.leaf) {
        const keys = siblingNode.keys
        for (let i = 0, len = keys.length; i < len; i++) {
          const k = keys[i]
          const n = cloneNode(ops.getNode(k))
          n.parent = siblingNode.id
          ops.updateNode(n)
        }
      }

      ops.deleteNode(node)
      ops.updateNode(siblingNode)
      deleteEntry(ops, ctx, ops.getNode(node.parent!), node.id, comparator)
    }
    else {
      if (isPredecessor) {
        let pointerPm
        let pointerKm
        if (!node.leaf) {
          pointerPm = siblingNode.keys.splice(-1)[0]
          pointerKm = siblingNode.values.splice(-1)[0]
          node.keys = [pointerPm, ...node.keys]
          node.values = [guess!, ...node.values]
          parentNode = cloneNode(ops.getNode(node.parent!)) as BPTreeInternalNode<K, V>
          const nodeIndex = parentNode.keys.indexOf(node.id)
          if (nodeIndex > 0) {
            parentNode.values[nodeIndex - 1] = pointerKm
            ops.updateNode(parentNode)
          }
        }
        else {
          pointerPm = siblingNode.keys.splice(-1)[0] as unknown as K[]
          pointerKm = siblingNode.values.splice(-1)[0]
          node.keys = [pointerPm, ...node.keys]
          node.values = [pointerKm, ...node.values]
          parentNode = cloneNode(ops.getNode(node.parent!)) as BPTreeInternalNode<K, V>
          const nodeIndex = parentNode.keys.indexOf(node.id)
          if (nodeIndex > 0) {
            parentNode.values[nodeIndex - 1] = pointerKm
            ops.updateNode(parentNode)
          }
        }
        ops.updateNode(node)
        ops.updateNode(siblingNode)
      }
      else {
        let pointerP0
        let pointerK0
        if (!node.leaf) {
          pointerP0 = siblingNode.keys.splice(0, 1)[0]
          pointerK0 = siblingNode.values.splice(0, 1)[0]
          node.keys = node.keys.concat(pointerP0)
          node.values = node.values.concat(guess!)
          parentNode = cloneNode(ops.getNode(node.parent!)) as BPTreeInternalNode<K, V>
          const pointerIndex = parentNode.keys.indexOf(siblingNode.id)
          if (pointerIndex > 0) {
            parentNode.values[pointerIndex - 1] = pointerK0
            ops.updateNode(parentNode)
          }
        }
        else {
          pointerP0 = siblingNode.keys.splice(0, 1)[0] as unknown as K[]
          pointerK0 = siblingNode.values.splice(0, 1)[0]
          node.keys = node.keys.concat(pointerP0)
          node.values = node.values.concat(pointerK0)
          parentNode = cloneNode(ops.getNode(node.parent!)) as BPTreeInternalNode<K, V>
          const pointerIndex = parentNode.keys.indexOf(siblingNode.id)
          if (pointerIndex > 0) {
            parentNode.values[pointerIndex - 1] = siblingNode.values[0]
            ops.updateNode(parentNode)
          }
        }
        ops.updateNode(node)
        ops.updateNode(siblingNode)
      }
      if (!siblingNode.leaf) {
        for (let i = 0, len = siblingNode.keys.length; i < len; i++) {
          const k = siblingNode.keys[i]
          const n = cloneNode(ops.getNode(k))
          n.parent = siblingNode.id
          ops.updateNode(n)
        }
      }
      if (!node.leaf) {
        for (let i = 0, len = node.keys.length; i < len; i++) {
          const k = node.keys[i]
          const n = cloneNode(ops.getNode(k))
          n.parent = node.id
          ops.updateNode(n)
        }
      }
      if (!parentNode.leaf) {
        for (let i = 0, len = parentNode.keys.length; i < len; i++) {
          const k = parentNode.keys[i]
          const n = cloneNode(ops.getNode(k))
          n.parent = parentNode.id
          ops.updateNode(n)
        }
      }
    }
  }
  else {
    ops.updateNode(cloneNode(node))
  }
  return node
}

export function deleteOp<K, V>(
  ops: BPTreeNodeOps<K, V>,
  ctx: BPTreeAlgoContext<K, V>,
  key: K,
  comparator: ValueComparator<V>,
  value?: V,
): void {
  if (value === undefined) {
    value = getOp(ops, ctx.rootId, key)
  }

  if (value === undefined) {
    return
  }

  let node = findLowerBoundLeaf(ops, ctx.rootId, value, comparator)
  let found = false
  while (true) {
    let i = node.values.length
    while (i--) {
      const nValue = node.values[i]
      if (comparator.isSame(value!, nValue)) {
        const keys = node.keys[i]
        const keyIndex = keys.indexOf(key)
        if (keyIndex !== -1) {
          node = cloneNode(node)
          const freshKeys = node.keys[i]
          freshKeys.splice(keyIndex, 1)
          if (freshKeys.length === 0) {
            node.keys.splice(i, 1)
            node.values.splice(i, 1)
          }
          ops.updateNode(node)
          node = deleteEntry(ops, ctx, node, key, comparator) as BPTreeLeafNode<K, V>
          found = true
          break
        }
      }
    }
    if (found) break
    if (node.next) {
      node = ops.getNode(node.next) as BPTreeLeafNode<K, V>
      continue
    }
    break
  }
}

// ─── Mutation: batchInsert ───────────────────────────────────────────

export function batchInsertOp<K, V>(
  ops: BPTreeNodeOps<K, V>,
  ctx: BPTreeAlgoContext<K, V>,
  entries: [K, V][],
  comparator: ValueComparator<V>,
): void {
  if (entries.length === 0) return
  const sorted = [...entries].sort((a, b) => comparator.asc(a[1], b[1]))
  let currentLeaf: BPTreeLeafNode<K, V> | null = null
  let modified = false
  let cachedLeafId: string | null = null
  let cachedLeafMaxValue: V | null = null

  for (let i = 0, len = sorted.length; i < len; i++) {
    const [key, value] = sorted[i]
    let targetLeaf: BPTreeLeafNode<K, V>
    if (
      cachedLeafId !== null &&
      cachedLeafMaxValue !== null &&
      currentLeaf !== null &&
      (comparator.isLower(value, cachedLeafMaxValue) || comparator.isSame(value, cachedLeafMaxValue))
    ) {
      targetLeaf = currentLeaf
    }
    else {
      targetLeaf = locateLeaf(ops, ctx.rootId, value, comparator)
    }

    if (currentLeaf !== null && currentLeaf.id === targetLeaf.id) {
      // same leaf
    }
    else {
      if (currentLeaf !== null && modified) {
        ops.updateNode(currentLeaf)
      }
      currentLeaf = cloneNode(targetLeaf)
      modified = false
    }

    cachedLeafId = currentLeaf.id
    const changed = insertValueIntoLeaf(currentLeaf, key as K, value, comparator)
    modified = modified || changed
    cachedLeafMaxValue = currentLeaf.values[currentLeaf.values.length - 1]

    if (currentLeaf.values.length === ctx.order) {
      ops.updateNode(currentLeaf)
      let after = ops.createNode(
        true, [], [], currentLeaf.parent, null, null,
      ) as BPTreeLeafNode<K, V>
      const mid = Math.ceil(ctx.order / 2) - 1
      after = cloneNode(after)
      after.values = currentLeaf.values.slice(mid + 1)
      after.keys = currentLeaf.keys.slice(mid + 1)
      currentLeaf.values = currentLeaf.values.slice(0, mid + 1)
      currentLeaf.keys = currentLeaf.keys.slice(0, mid + 1)
      ops.updateNode(currentLeaf)
      ops.updateNode(after)
      insertInParent(ops, ctx, currentLeaf, after.values[0], after)
      currentLeaf = null
      cachedLeafId = null
      cachedLeafMaxValue = null
      modified = false
    }
  }

  if (currentLeaf !== null && modified) {
    ops.updateNode(currentLeaf)
  }
}

// ─── Mutation: bulkLoad ──────────────────────────────────────────────

export function bulkLoadOp<K, V>(
  ops: BPTreeNodeOps<K, V>,
  ctx: BPTreeAlgoContext<K, V>,
  entries: [K, V][],
  comparator: ValueComparator<V>,
): void {
  if (entries.length === 0) return

  const root = ops.getNode(ctx.rootId)
  if (!root.leaf || root.values.length > 0) {
    throw new Error('bulkLoad can only be called on an empty tree. Use batchInsert for non-empty trees.')
  }

  const sorted = [...entries].sort((a, b) => comparator.asc(a[1], b[1]))

  const grouped: { keys: K[], value: V }[] = []
  for (let i = 0, len = sorted.length; i < len; i++) {
    const [key, value] = sorted[i]
    const last = grouped[grouped.length - 1]
    if (last && comparator.isSame(last.value, value)) {
      if (!last.keys.includes(key)) {
        last.keys.push(key)
      }
    }
    else {
      grouped.push({ keys: [key], value })
    }
  }

  ops.deleteNode(root)

  const maxLeafSize = ctx.order - 1
  const leaves: BPTreeLeafNode<K, V>[] = []

  for (let i = 0, len = grouped.length; i < len; i += maxLeafSize) {
    const chunk = grouped.slice(i, i + maxLeafSize)
    const leafKeys = chunk.map(g => g.keys)
    const leafValues = chunk.map(g => g.value)
    const leaf = ops.createNode(
      true, leafKeys, leafValues, null, null, null,
    ) as BPTreeLeafNode<K, V>
    leaves.push(leaf)
  }

  for (let i = 0, len = leaves.length; i < len; i++) {
    if (i > 0) {
      leaves[i].prev = leaves[i - 1].id
    }
    if (i < len - 1) {
      leaves[i].next = leaves[i + 1].id
    }
    ops.updateNode(leaves[i])
  }

  let currentLevel: BPTreeUnknownNode<K, V>[] = leaves

  while (currentLevel.length > 1) {
    const nextLevel: BPTreeUnknownNode<K, V>[] = []

    for (let i = 0, len = currentLevel.length; i < len; i += ctx.order) {
      const children = currentLevel.slice(i, i + ctx.order)
      const childIds = children.map(c => c.id)

      const separators: V[] = []
      for (let j = 1, cLen = children.length; j < cLen; j++) {
        separators.push(children[j].values[0])
      }

      const internalNode = ops.createNode(
        false, childIds, separators, null, null, null,
      ) as BPTreeInternalNode<K, V>

      for (let j = 0, cLen = children.length; j < cLen; j++) {
        const child = children[j]
        child.parent = internalNode.id
        ops.updateNode(child)
      }

      nextLevel.push(internalNode)
    }

    currentLevel = nextLevel
  }

  const newRoot = currentLevel[0]
  ops.writeHead({
    root: newRoot.id,
    order: ctx.order,
    data: ctx.headData(),
  })
  ctx.rootId = newRoot.id
}

// ─── Query ───────────────────────────────────────────────────────────

export function existsOp<K, V>(
  ops: BPTreeNodeOps<K, V>,
  rootId: string,
  key: K,
  value: V,
  comparator: ValueComparator<V>,
): boolean {
  const node = locateLeaf(ops, rootId, value, comparator)
  const { index, found } = binarySearchValues(node.values, value, comparator)
  if (found) {
    const keys = node.keys[index]
    if (keys.includes(key)) {
      return true
    }
  }
  return false
}

export function getOp<K, V>(
  ops: BPTreeNodeOps<K, V>,
  rootId: string,
  key: K,
): V | undefined {
  let node = leftestNode(ops, rootId)
  while (true) {
    for (let i = 0, len = node.values.length; i < len; i++) {
      const keys = node.keys[i]
      for (let j = 0, kLen = keys.length; j < kLen; j++) {
        if (keys[j] === key) {
          return node.values[i]
        }
      }
    }
    if (!node.next) break
    node = ops.getNode(node.next) as BPTreeLeafNode<K, V>
  }
  return undefined
}

export function* whereStreamOp<K, V>(
  ops: BPTreeNodeOps<K, V>,
  rootId: string,
  condition: BPTreeCondition<V>,
  comparator: ValueComparator<V>,
  verifierMap: Record<keyof BPTreeCondition<V>, (nodeValue: V, value: V | V[]) => boolean>,
  searchConfigs: Record<keyof BPTreeCondition<V>, Record<'asc' | 'desc', SearchConfigEntry<K, V>>>,
  ensureValues: (v: V | V[]) => V[],
  options?: BPTreeSearchOption<K>,
): Generator<[K, V]> {
  const { filterValues, limit, order = 'asc' } = options ?? {}
  const conditionKeys = Object.keys(condition)
  if (conditionKeys.length === 0) return

  const resolved = resolveStartEndConfigs(condition, order, comparator, ensureValues)
  const direction = resolved.direction

  let startNode: BPTreeLeafNode<K, V> | null
  if (resolved.startKey) {
    const startConfig = searchConfigs[resolved.startKey][order]
    startNode = startConfig.start(rootId, ops, resolved.startValues) as BPTreeLeafNode<K, V> | null
  }
  else {
    startNode = order === 'asc' ? leftestNode(ops, rootId) : rightestNode(ops, rootId)
  }

  let endNode: BPTreeLeafNode<K, V> | null = null
  if (resolved.endKey) {
    const endConfig = searchConfigs[resolved.endKey][order]
    endNode = endConfig.end(rootId, ops, resolved.endValues) as BPTreeLeafNode<K, V> | null
  }

  if (!startNode) return

  const generator = getPairsGenerator(ops, startNode, endNode, direction)

  let count = 0
  const intersection = filterValues && filterValues.size > 0 ? filterValues : null
  for (const pair of generator) {
    const [k, v] = pair
    if (intersection && !intersection.has(k)) {
      continue
    }
    if (verify(v, condition, verifierMap)) {
      yield pair
      count++
      if (limit !== undefined && count >= limit) {
        break
      }
    }
  }
}

export function* keysStreamOp<K, V>(
  ops: BPTreeNodeOps<K, V>,
  rootId: string,
  condition: BPTreeCondition<V>,
  comparator: ValueComparator<V>,
  verifierMap: Record<keyof BPTreeCondition<V>, (nodeValue: V, value: V | V[]) => boolean>,
  searchConfigs: Record<keyof BPTreeCondition<V>, Record<'asc' | 'desc', SearchConfigEntry<K, V>>>,
  ensureValues: (v: V | V[]) => V[],
  options?: BPTreeSearchOption<K>,
): Generator<K> {
  const { filterValues, limit } = options ?? {}
  const stream = whereStreamOp(ops, rootId, condition, comparator, verifierMap, searchConfigs, ensureValues, options)
  const intersection = filterValues && filterValues.size > 0 ? filterValues : null
  let count = 0
  for (const [key] of stream) {
    if (intersection && !intersection.has(key)) {
      continue
    }
    yield key
    count++
    if (limit !== undefined && count >= limit) {
      break
    }
  }
}

// ─── Init ────────────────────────────────────────────────────────────

export function initOp<K, V>(
  ops: BPTreeNodeOps<K, V>,
  ctx: BPTreeAlgoContext<K, V>,
  strategyOrder: number,
  strategyHead: { data: SerializableData },
  setStrategyHead: (head: SerializeStrategyHead) => void,
): void {
  const head = ops.readHead()
  if (head === null) {
    ctx.order = strategyOrder
    const root = ops.createNode(true, [], [])
    ops.writeHead({
      root: root.id,
      order: ctx.order,
      data: strategyHead.data,
    })
    ctx.rootId = root.id
  }
  else {
    const { root, order } = head
    setStrategyHead(head)
    ctx.order = order
    ctx.rootId = root!
  }
  if (ctx.order < 3) {
    throw new Error(`The 'order' parameter must be greater than 2. but got a '${ctx.order}'.`)
  }
}

