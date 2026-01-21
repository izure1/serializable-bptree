import type { AsyncMVCCTransaction } from 'mvcc-api'
import type { BPTreeAsync } from '../BPTreeAsync'
import type { BPTreeMVCCStrategyAsync } from './BPTreeMVCCStrategyAsync'
import type {
  BPTreeCondition,
  BPTreeLeafNode,
  BPTreePair,
  BPTreeTransactionResult,
  BPTreeUnknownNode,
  BPTreeInternalNode
} from '../types'

/**
 * Asynchronous B+Tree Transaction with MVCC support.
 * Provides snapshot isolation - changes are only visible after commit.
 */
export class BPTreeAsyncTransaction<K, V> {
  private readonly tree: BPTreeAsync<K, V>
  private readonly mvccTx: AsyncMVCCTransaction<BPTreeMVCCStrategyAsync<K, V>, string, BPTreeUnknownNode<K, V> | null>

  private rootId: string
  private order: number
  private isCommitted: boolean = false
  private isRolledBack: boolean = false

  private readonly createdIds: Set<string> = new Set()
  private readonly updatedIds: Set<string> = new Set()
  private readonly deletedIds: Set<string> = new Set()

  constructor(
    tree: BPTreeAsync<K, V>,
    mvccTx: AsyncMVCCTransaction<BPTreeMVCCStrategyAsync<K, V>, string, BPTreeUnknownNode<K, V> | null>
  ) {
    this.tree = tree
    this.mvccTx = mvccTx
    this.rootId = tree.getRootId()
    this.order = tree.getOrder()
  }

  /**
   * Initialize the transaction - must be called after construction.
   */
  async initTransaction(): Promise<void> {
    this.rootId = this.tree.getRootId()
    this.order = this.tree.getOrder()
  }

  private ensureNotFinished(): void {
    if (this.isCommitted) {
      throw new Error('Transaction has already been committed')
    }
    if (this.isRolledBack) {
      throw new Error('Transaction has already been rolled back')
    }
  }

  /**
   * Get a node by ID, using MVCC read.
   */
  private async getNode(id: string): Promise<BPTreeUnknownNode<K, V>> {
    const node = await this.mvccTx.read(id)
    if (!node) {
      throw new Error(`Node not found: ${id}`)
    }
    return node
  }

  /**
   * Write a node, using MVCC write.
   */
  private async writeNode(node: BPTreeUnknownNode<K, V>): Promise<void> {
    await this.mvccTx.write(node.id, node)
    if (!this.createdIds.has(node.id)) {
      this.updatedIds.add(node.id)
    }
  }

  /**
   * Create a new node with a unique ID.
   */
  private async createNode(
    isLeaf: boolean,
    keys: string[] | K[][],
    values: V[],
    parent: string | null = null,
    next: string | null = null,
    prev: string | null = null
  ): Promise<BPTreeUnknownNode<K, V>> {
    const id = await (this.tree as any).strategy.id(isLeaf)
    const node = {
      id,
      keys,
      values,
      leaf: isLeaf,
      parent,
      next,
      prev
    } as BPTreeUnknownNode<K, V>

    await this.mvccTx.create(id, node)
    this.createdIds.add(id)
    return node
  }

  /**
   * Delete a node by ID.
   */
  private async deleteNode(id: string): Promise<void> {
    await this.mvccTx.delete(id)
    this.deletedIds.add(id)
    this.createdIds.delete(id)
    this.updatedIds.delete(id)
  }

  // ============================================
  // Read Operations (B+Tree interface)
  // ============================================

  /**
   * Get the order of the B+Tree.
   */
  getOrder(): number {
    return this.order
  }

  /**
   * Get the root node ID.
   */
  getRootId(): string {
    return this.rootId
  }

  /**
   * Get value by key.
   */
  async get(key: K): Promise<V | undefined> {
    this.ensureNotFinished()

    let node = await this.leftestNode()
    while (true) {
      for (let i = 0; i < node.values.length; i++) {
        const keys = node.keys[i]
        for (let j = 0; j < keys.length; j++) {
          if (keys[j] === key) {
            return node.values[i]
          }
        }
      }
      if (!node.next) break
      node = await this.getNode(node.next) as BPTreeLeafNode<K, V>
    }
    return undefined
  }

  /**
   * Check if key-value pair exists.
   */
  async exists(key: K, value: V): Promise<boolean> {
    this.ensureNotFinished()

    const comparator = (this.tree as any).comparator
    const node = await this.insertableNode(value)

    for (let i = 0; i < node.values.length; i++) {
      if (comparator.isSame(value, node.values[i])) {
        const keys = node.keys[i]
        if (keys.includes(key)) {
          return true
        }
      }
    }
    return false
  }

  /**
   * Search keys by condition.
   */
  async keys(condition: BPTreeCondition<V>, filterValues?: Set<K>): Promise<Set<K>> {
    this.ensureNotFinished()

    const set = new Set<K>()
    for await (const key of this.keysStream(condition, filterValues)) {
      set.add(key)
    }
    return set
  }

  /**
   * Search pairs by condition.
   */
  async where(condition: BPTreeCondition<V>): Promise<BPTreePair<K, V>> {
    this.ensureNotFinished()

    const map = new Map<K, V>()
    for await (const [key, value] of this.whereStream(condition)) {
      map.set(key, value)
    }
    return map
  }

  async *keysStream(
    condition: BPTreeCondition<V>,
    filterValues?: Set<K>,
    limit?: number
  ): AsyncGenerator<K> {
    const stream = this.whereStream(condition, limit)
    const intersection = filterValues && filterValues.size > 0 ? filterValues : null
    for await (const [key] of stream) {
      if (intersection && !intersection.has(key)) {
        continue
      }
      yield key
    }
  }

  async *whereStream(
    condition: BPTreeCondition<V>,
    limit?: number
  ): AsyncGenerator<[K, V]> {
    const treeAny = this.tree as any
    const verifierMap = treeAny.verifierMap
    const verifierDirection = treeAny.verifierDirection
    const verifierEarlyTerminate = treeAny.verifierEarlyTerminate

    let driverKey: keyof BPTreeCondition<V> | null = null

    if ('primaryEqual' in condition) driverKey = 'primaryEqual'
    else if ('equal' in condition) driverKey = 'equal'
    else if ('gt' in condition) driverKey = 'gt'
    else if ('gte' in condition) driverKey = 'gte'
    else if ('lt' in condition) driverKey = 'lt'
    else if ('lte' in condition) driverKey = 'lte'
    else if ('primaryGt' in condition) driverKey = 'primaryGt'
    else if ('primaryGte' in condition) driverKey = 'primaryGte'
    else if ('primaryLt' in condition) driverKey = 'primaryLt'
    else if ('primaryLte' in condition) driverKey = 'primaryLte'
    else if ('like' in condition) driverKey = 'like'
    else if ('notEqual' in condition) driverKey = 'notEqual'
    else if ('primaryNotEqual' in condition) driverKey = 'primaryNotEqual'
    else if ('or' in condition) driverKey = 'or'
    else if ('primaryOr' in condition) driverKey = 'primaryOr'

    if (!driverKey) return

    const value = condition[driverKey] as V
    const startNode = await this.getStartNode(driverKey, value)
    const endNode = await this.getEndNode(driverKey, value)
    const direction = verifierDirection[driverKey]
    const comparator = verifierMap[driverKey]
    const earlyTerminate = verifierEarlyTerminate[driverKey]

    const generator = this.getPairsGenerator(
      value,
      startNode,
      endNode,
      comparator,
      direction,
      earlyTerminate
    )

    let count = 0
    for await (const pair of generator) {
      const [k, v] = pair
      let isMatch = true

      for (const key in condition) {
        if (key === driverKey) continue
        const verify = verifierMap[key as keyof BPTreeCondition<V>]
        const condValue = condition[key as keyof BPTreeCondition<V>] as V
        if (!verify(v, condValue)) {
          isMatch = false
          break
        }
      }

      if (isMatch) {
        yield pair
        count++
        if (limit !== undefined && count >= limit) {
          break
        }
      }
    }
  }

  private async *getPairsGenerator(
    value: V,
    startNode: BPTreeLeafNode<K, V>,
    endNode: BPTreeLeafNode<K, V> | null,
    comparator: (nodeValue: V, value: V) => boolean,
    direction: 1 | -1,
    earlyTerminate: boolean
  ): AsyncGenerator<[K, V]> {
    let node = startNode
    let done = false
    let hasMatched = false

    while (!done) {
      if (endNode && node.id === endNode.id) {
        done = true
        break
      }

      const len = node.values.length
      if (direction === 1) {
        for (let i = 0; i < len; i++) {
          const nValue = node.values[i]
          const keys = node.keys[i]
          if (comparator(nValue, value)) {
            hasMatched = true
            for (let j = 0; j < keys.length; j++) {
              yield [keys[j], nValue]
            }
          } else if (earlyTerminate && hasMatched) {
            done = true
            break
          }
        }
      } else {
        let i = len
        while (i--) {
          const nValue = node.values[i]
          const keys = node.keys[i]
          if (comparator(nValue, value)) {
            hasMatched = true
            let j = keys.length
            while (j--) {
              yield [keys[j], nValue]
            }
          } else if (earlyTerminate && hasMatched) {
            done = true
            break
          }
        }
      }

      if (done) break

      if (direction === 1) {
        if (!node.next) {
          done = true
          break
        }
        node = await this.getNode(node.next) as BPTreeLeafNode<K, V>
      } else {
        if (!node.prev) {
          done = true
          break
        }
        node = await this.getNode(node.prev) as BPTreeLeafNode<K, V>
      }
    }
  }

  // ============================================
  // Write Operations (B+Tree interface)
  // ============================================

  /**
   * Insert a key-value pair.
   */
  async insert(key: K, value: V): Promise<void> {
    this.ensureNotFinished()

    const before = await this.insertableNode(value)
    await this.insertAtLeaf(before, key, value)

    if (before.values.length === this.order) {
      const after = await this.createNode(
        true,
        [],
        [],
        before.parent,
        null,
        null
      ) as BPTreeLeafNode<K, V>

      const mid = Math.ceil(this.order / 2) - 1
      after.values = before.values.slice(mid + 1)
      after.keys = before.keys.slice(mid + 1) as K[][]
      before.values = before.values.slice(0, mid + 1)
      before.keys = before.keys.slice(0, mid + 1) as K[][]

      await this.writeNode(after)
      await this.insertInParent(before, after.values[0], after)
      await this.writeNode(before)
    }
  }

  /**
   * Delete a key-value pair.
   */
  async delete(key: K, value: V): Promise<void> {
    this.ensureNotFinished()

    const comparator = (this.tree as any).comparator
    const node = await this.insertableNode(value)

    let i = node.values.length
    while (i--) {
      const nValue = node.values[i]
      if (comparator.isSame(value, nValue)) {
        const keys = node.keys[i]
        const keyIndex = keys.indexOf(key)
        if (keyIndex !== -1) {
          keys.splice(keyIndex, 1)
          if (keys.length === 0) {
            node.keys.splice(i, 1)
            node.values.splice(i, 1)
          }
          await this.deleteEntry(node, key, value)
          break
        }
      }
    }
  }

  // ============================================
  // Transaction Control
  // ============================================

  /**
   * Commit the transaction.
   */
  async commit(): Promise<BPTreeTransactionResult> {
    this.ensureNotFinished()

    const result = await this.mvccTx.commit()

    if (!result.success) {
      this.isRolledBack = true
      return {
        success: false,
        createdIds: [],
        obsoleteIds: [],
        error: result.error || 'Commit conflict'
      }
    }

    this.isCommitted = true

    // Apply changes to the base tree
    await this.tree.applyCommit(this.rootId, this.order, {
      created: Array.from(this.createdIds),
      updated: Array.from(this.updatedIds),
      deleted: Array.from(this.deletedIds)
    })

    return {
      success: true,
      createdIds: Array.from(this.createdIds),
      obsoleteIds: Array.from(this.deletedIds)
    }
  }

  /**
   * Rollback the transaction.
   */
  async rollback(): Promise<BPTreeTransactionResult> {
    this.ensureNotFinished()

    await this.mvccTx.rollback()
    this.isRolledBack = true

    return {
      success: true,
      createdIds: [],
      obsoleteIds: []
    }
  }

  // ============================================
  // Helper Methods (B+Tree internals)
  // ============================================

  private async leftestNode(): Promise<BPTreeLeafNode<K, V>> {
    let node = await this.getNode(this.rootId)
    while (!node.leaf) {
      const keys = node.keys as string[]
      node = await this.getNode(keys[0])
    }
    return node as BPTreeLeafNode<K, V>
  }

  private async rightestNode(): Promise<BPTreeLeafNode<K, V>> {
    let node = await this.getNode(this.rootId)
    while (!node.leaf) {
      const keys = node.keys as string[]
      node = await this.getNode(keys[keys.length - 1])
    }
    return node as BPTreeLeafNode<K, V>
  }

  private async insertableNode(value: V): Promise<BPTreeLeafNode<K, V>> {
    const comparator = (this.tree as any).comparator
    let node = await this.getNode(this.rootId)

    while (!node.leaf) {
      for (let i = 0; i < node.values.length; i++) {
        const nValue = node.values[i]
        const k = node.keys as string[]
        if (comparator.isSame(value, nValue)) {
          node = await this.getNode(k[i + 1])
          break
        } else if (comparator.isLower(value, nValue)) {
          node = await this.getNode(k[i])
          break
        } else if (i + 1 === node.values.length) {
          node = await this.getNode(k[i + 1])
          break
        }
      }
    }
    return node as BPTreeLeafNode<K, V>
  }

  private async insertableNodeByPrimary(value: V): Promise<BPTreeLeafNode<K, V>> {
    const comparator = (this.tree as any).comparator
    let node = await this.getNode(this.rootId)

    while (!node.leaf) {
      for (let i = 0; i < node.values.length; i++) {
        const nValue = node.values[i]
        const k = node.keys as string[]
        if (comparator.isPrimarySame(value, nValue)) {
          node = await this.getNode(k[i])
          break
        } else if (comparator.isPrimaryLower(value, nValue)) {
          node = await this.getNode(k[i])
          break
        } else if (i + 1 === node.values.length) {
          node = await this.getNode(k[i + 1])
          break
        }
      }
    }
    return node as BPTreeLeafNode<K, V>
  }

  private async insertableRightestNodeByPrimary(value: V): Promise<BPTreeLeafNode<K, V>> {
    const comparator = (this.tree as any).comparator
    let node = await this.getNode(this.rootId)

    while (!node.leaf) {
      for (let i = 0; i < node.values.length; i++) {
        const nValue = node.values[i]
        const k = node.keys as string[]
        if (comparator.isPrimaryLower(value, nValue)) {
          node = await this.getNode(k[i])
          break
        }
        if (i + 1 === node.values.length) {
          node = await this.getNode(k[i + 1])
          break
        }
      }
    }
    return node as BPTreeLeafNode<K, V>
  }

  private async getStartNode(driverKey: keyof BPTreeCondition<V>, value: V): Promise<BPTreeLeafNode<K, V>> {
    const treeAny = this.tree as any

    switch (driverKey) {
      case 'gt':
      case 'gte':
      case 'equal':
      case 'primaryGt':
      case 'primaryGte':
      case 'primaryEqual':
        return await this.insertableNodeByPrimary(value)
      case 'lt':
      case 'primaryLt':
        return await this.insertableNodeByPrimary(value)
      case 'lte':
      case 'primaryLte':
        return await this.insertableRightestNodeByPrimary(value)
      case 'or':
      case 'primaryOr': {
        const values = treeAny.ensureValues(value)
        const lowest = treeAny.lowestPrimaryValue(values)
        return await this.insertableNodeByPrimary(lowest)
      }
      default:
        return await this.leftestNode()
    }
  }

  private async getEndNode(driverKey: keyof BPTreeCondition<V>, value: V): Promise<BPTreeLeafNode<K, V> | null> {
    const treeAny = this.tree as any

    switch (driverKey) {
      case 'equal': {
        const node = await this.insertableNode(value)
        if (!node.next) return null
        return await this.getNode(node.next) as BPTreeLeafNode<K, V>
      }
      case 'primaryEqual': {
        const node = await this.insertableRightestNodeByPrimary(value)
        if (!node.next) return null
        return await this.getNode(node.next) as BPTreeLeafNode<K, V>
      }
      case 'or': {
        const values = treeAny.ensureValues(value)
        const highest = treeAny.highestValue(values)
        const node = await this.insertableNode(highest)
        if (!node.next) return null
        return await this.getNode(node.next) as BPTreeLeafNode<K, V>
      }
      case 'primaryOr': {
        const values = treeAny.ensureValues(value)
        const highest = treeAny.highestPrimaryValue(values)
        const node = await this.insertableRightestNodeByPrimary(highest)
        if (!node.next) return null
        return await this.getNode(node.next) as BPTreeLeafNode<K, V>
      }
      default:
        return null
    }
  }

  private async insertAtLeaf(node: BPTreeLeafNode<K, V>, key: K, value: V): Promise<void> {
    const comparator = (this.tree as any).comparator

    if (node.values.length) {
      for (let i = 0; i < node.values.length; i++) {
        const nValue = node.values[i]
        if (comparator.isSame(value, nValue)) {
          const keys = node.keys[i]
          if (keys.includes(key)) {
            break
          }
          keys.push(key)
          await this.writeNode(node)
          return
        } else if (comparator.isLower(value, nValue)) {
          node.values.splice(i, 0, value)
          node.keys.splice(i, 0, [key])
          await this.writeNode(node)
          return
        } else if (i + 1 === node.values.length) {
          node.values.push(value)
          node.keys.push([key])
          await this.writeNode(node)
          return
        }
      }
    } else {
      node.values = [value]
      node.keys = [[key]]
      await this.writeNode(node)
    }
  }

  private async insertInParent(
    node: BPTreeUnknownNode<K, V>,
    value: V,
    pointer: BPTreeUnknownNode<K, V>
  ): Promise<void> {
    if (this.rootId === node.id) {
      const root = await this.createNode(false, [node.id, pointer.id], [value])
      this.rootId = root.id
      node.parent = root.id
      pointer.parent = root.id

      if (pointer.leaf) {
        (node as any).next = pointer.id
          ; (pointer as any).prev = node.id
      }

      await this.writeNode(node)
      await this.writeNode(pointer)
      return
    }

    const parentNode = await this.getNode(node.parent!) as BPTreeInternalNode<K, V>
    const nodeIndex = parentNode.keys.indexOf(node.id)

    if (nodeIndex === -1) {
      throw new Error(`Node ${node.id} not found in parent ${parentNode.id}`)
    }

    parentNode.values.splice(nodeIndex, 0, value)
    parentNode.keys.splice(nodeIndex + 1, 0, pointer.id)
    pointer.parent = parentNode.id

    if (pointer.leaf) {
      const leftSibling = node as BPTreeLeafNode<K, V>
      const oldNextId = leftSibling.next

      pointer.prev = leftSibling.id
      pointer.next = oldNextId
      leftSibling.next = pointer.id

      await this.writeNode(leftSibling)

      if (oldNextId) {
        const oldNext = await this.getNode(oldNextId) as BPTreeLeafNode<K, V>
        oldNext.prev = pointer.id
        await this.writeNode(oldNext)
      }
    }

    await this.writeNode(parentNode)
    await this.writeNode(pointer)

    if (parentNode.keys.length > this.order) {
      const parentPointer = await this.createNode(false, [], []) as BPTreeInternalNode<K, V>
      parentPointer.parent = parentNode.parent
      const mid = Math.ceil(this.order / 2) - 1
      parentPointer.values = parentNode.values.slice(mid + 1)
      parentPointer.keys = parentNode.keys.slice(mid + 1)
      const midValue = parentNode.values[mid]
      parentNode.values = parentNode.values.slice(0, mid)
      parentNode.keys = parentNode.keys.slice(0, mid + 1)

      for (const k of parentNode.keys) {
        const n = await this.getNode(k)
        n.parent = parentNode.id
        await this.writeNode(n)
      }
      for (const k of parentPointer.keys) {
        const n = await this.getNode(k)
        n.parent = parentPointer.id
        await this.writeNode(n)
      }

      await this.insertInParent(parentNode, midValue, parentPointer)
      await this.writeNode(parentNode)
    }
  }

  private async deleteEntry(
    node: BPTreeUnknownNode<K, V>,
    key: K | string,
    value: V
  ): Promise<void> {
    if (!node.leaf) {
      let keyIndex = -1
      for (let i = 0; i < node.keys.length; i++) {
        if (node.keys[i] === key) {
          keyIndex = i
          break
        }
      }

      if (keyIndex !== -1) {
        node.keys.splice(keyIndex, 1)
        const valueIndex = keyIndex > 0 ? keyIndex - 1 : 0
        node.values.splice(valueIndex, 1)
        await this.writeNode(node)
      }
    }

    if (this.rootId === node.id && node.keys.length === 1 && !node.leaf) {
      const keys = node.keys as string[]
      await this.deleteNode(node.id)
      const newRoot = await this.getNode(keys[0])
      this.rootId = newRoot.id
      newRoot.parent = null
      await this.writeNode(newRoot)
      return
    } else if (this.rootId === node.id) {
      await this.writeNode(node)
      return
    } else if (
      (node.keys.length < Math.ceil(this.order / 2) && !node.leaf) ||
      (node.values.length < Math.ceil((this.order - 1) / 2) && node.leaf)
    ) {
      if (node.parent === null) {
        return
      }

      let isPredecessor = false
      let parentNode = await this.getNode(node.parent) as BPTreeInternalNode<K, V>
      let prevNode: BPTreeInternalNode<K, V> | null = null
      let nextNode: BPTreeInternalNode<K, V> | null = null
      let prevValue: V | null = null
      let postValue: V | null = null

      for (let i = 0; i < parentNode.keys.length; i++) {
        const nKey = parentNode.keys[i]
        if (nKey === node.id) {
          if (i > 0) {
            prevNode = await this.getNode(parentNode.keys[i - 1]) as BPTreeInternalNode<K, V>
            prevValue = parentNode.values[i - 1]
          }
          if (i < parentNode.keys.length - 1) {
            nextNode = await this.getNode(parentNode.keys[i + 1]) as BPTreeInternalNode<K, V>
            postValue = parentNode.values[i]
          }
        }
      }

      let pointer: BPTreeUnknownNode<K, V>
      let guess: V | null
      if (prevNode === null) {
        pointer = nextNode!
        guess = postValue
      } else if (nextNode === null) {
        isPredecessor = true
        pointer = prevNode
        guess = prevValue
      } else {
        if (node.values.length + nextNode.values.length < this.order) {
          pointer = nextNode
          guess = postValue
        } else {
          isPredecessor = true
          pointer = prevNode
          guess = prevValue
        }
      }

      if (!pointer) {
        return
      }

      if (node.values.length + pointer.values.length < this.order) {
        if (!isPredecessor) {
          const pTemp = pointer
          pointer = node as BPTreeInternalNode<K, V>
          node = pTemp
        }
        pointer.keys.push(...node.keys as any)
        if (!node.leaf) {
          pointer.values.push(guess!)
        } else {
          pointer.next = node.next
          if (pointer.next) {
            const n = await this.getNode(pointer.next)
            n.prev = pointer.id
            await this.writeNode(n)
          }
        }
        pointer.values.push(...node.values)

        if (!pointer.leaf) {
          const keys = pointer.keys as string[]
          for (const k of keys) {
            const n = await this.getNode(k)
            n.parent = pointer.id
            await this.writeNode(n)
          }
        }

        await this.deleteEntry(await this.getNode(node.parent!), node.id, guess!)
        await this.writeNode(pointer)
        await this.deleteNode(node.id)
      } else {
        if (isPredecessor) {
          let pointerPm
          let pointerKm
          if (!node.leaf) {
            pointerPm = (pointer.keys as string[]).splice(-1)[0]
            pointerKm = pointer.values.splice(-1)[0]
            node.keys = [pointerPm, ...node.keys]
            node.values = [guess!, ...node.values]
            parentNode = await this.getNode(node.parent!) as BPTreeInternalNode<K, V>
            const nodeIndex = parentNode.keys.indexOf(node.id)
            if (nodeIndex > 0) {
              parentNode.values[nodeIndex - 1] = pointerKm
              await this.writeNode(parentNode)
            }
          } else {
            const leafPointer = pointer as unknown as BPTreeLeafNode<K, V>
            pointerPm = leafPointer.keys.splice(-1)[0]
            pointerKm = pointer.values.splice(-1)[0]
            node.keys = [pointerPm, ...node.keys as K[][]] as any
            node.values = [pointerKm, ...node.values]
            parentNode = await this.getNode(node.parent!) as BPTreeInternalNode<K, V>
            const nodeIndex = parentNode.keys.indexOf(node.id)
            if (nodeIndex > 0) {
              parentNode.values[nodeIndex - 1] = pointerKm
              await this.writeNode(parentNode)
            }
          }
          await this.writeNode(node)
          await this.writeNode(pointer)
        } else {
          let pointerP0
          let pointerK0
          if (!node.leaf) {
            pointerP0 = (pointer.keys as string[]).splice(0, 1)[0]
            pointerK0 = pointer.values.splice(0, 1)[0]
            node.keys = [...node.keys, pointerP0] as string[]
            node.values = [...node.values, guess!]
            parentNode = await this.getNode(node.parent!) as BPTreeInternalNode<K, V>
            const pointerIndex = parentNode.keys.indexOf(pointer.id)
            if (pointerIndex > 0) {
              parentNode.values[pointerIndex - 1] = pointerK0
              await this.writeNode(parentNode)
            }
          } else {
            const leafPointer = pointer as unknown as BPTreeLeafNode<K, V>
            pointerP0 = leafPointer.keys.splice(0, 1)[0]
            pointerK0 = pointer.values.splice(0, 1)[0]
            node.keys = [...node.keys as K[][], pointerP0] as any
            node.values = [...node.values, pointerK0]
            parentNode = await this.getNode(node.parent!) as BPTreeInternalNode<K, V>
            const pointerIndex = parentNode.keys.indexOf(pointer.id)
            if (pointerIndex > 0) {
              parentNode.values[pointerIndex - 1] = pointer.values[0]
              await this.writeNode(parentNode)
            }
          }
          await this.writeNode(node)
          await this.writeNode(pointer)
        }

        if (!pointer.leaf) {
          const keys = pointer.keys as string[]
          for (const k of keys) {
            const n = await this.getNode(k)
            n.parent = pointer.id
            await this.writeNode(n)
          }
        }
        if (!node.leaf) {
          const keys = node.keys as string[]
          for (const k of keys) {
            const n = await this.getNode(k)
            n.parent = node.id
            await this.writeNode(n)
          }
        }
        if (!parentNode.leaf) {
          const keys = parentNode.keys as string[]
          for (const k of keys) {
            const n = await this.getNode(k)
            n.parent = parentNode.id
            await this.writeNode(n)
          }
        }
      }
    } else {
      await this.writeNode(node)
    }
  }
}
