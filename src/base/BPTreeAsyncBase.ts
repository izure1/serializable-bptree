import type { BPTreeCondition, BPTreeConstructorOption, BPTreeUnknownNode, BPTreeLeafNode, BPTreeNodeKey, SerializableData, BPTreeInternalNode, BPTreePair } from '../types'
import { CacheEntanglementAsync } from 'cache-entanglement'
import { SerializeStrategyAsync } from '../SerializeStrategyAsync'
import { BPTree } from './BPTree'
import { ValueComparator } from './ValueComparator'

export abstract class BPTreeAsyncBase<K, V> extends BPTree<K, V> {
  declare protected readonly strategy: SerializeStrategyAsync<K, V>
  declare protected readonly nodes: ReturnType<typeof this._createCachedNode>

  constructor(
    strategy: SerializeStrategyAsync<K, V>,
    comparator: ValueComparator<V>,
    option?: BPTreeConstructorOption
  ) {
    super(strategy, comparator, option)
    this.nodes = this._createCachedNode()
  }

  private _createCachedNode() {
    return new CacheEntanglementAsync(async (key) => {
      return await this.strategy.read(key) as BPTreeUnknownNode<K, V>
    }, {
      capacity: this.option.capacity ?? 1000
    })
  }

  protected async *getPairsGenerator(
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

  protected async _createNodeId(isLeaf: boolean): Promise<string> {
    const id = await this.strategy.id(isLeaf)
    if (id === null) {
      throw new Error(`The node's id should never be null.`)
    }
    return id
  }

  protected async _createNode(
    isLeaf: boolean,
    keys: string[] | K[][],
    values: V[],
    leaf = isLeaf,
    parent: string | null = null,
    next: string | null = null,
    prev: string | null = null
  ): Promise<BPTreeUnknownNode<K, V>> {
    const id = await this._createNodeId(isLeaf)
    const node = {
      id,
      keys,
      values,
      leaf: leaf as any,
      parent,
      next,
      prev
    } as any as BPTreeUnknownNode<K, V>
    await this.bufferForNodeCreate(node)
    return node
  }

  protected async _deleteEntry(
    node: BPTreeUnknownNode<K, V>,
    key: BPTreeNodeKey<K>,
    value: V
  ): Promise<void> {
    if (!node.leaf) {
      let keyIndex = -1
      for (let i = 0, len = node.keys.length; i < len; i++) {
        if (node.keys[i] === key) {
          keyIndex = i
          break
        }
      }

      if (keyIndex !== -1) {
        node.keys.splice(keyIndex, 1)
        // In internal node, we remove the separator that corresponds to the key.
        const valueIndex = keyIndex > 0 ? keyIndex - 1 : 0
        node.values.splice(valueIndex, 1)
        await this.bufferForNodeUpdate(node)
      }
    }

    if (this.rootId === node.id && node.keys.length === 1 && !node.leaf) {
      const keys = node.keys as string[]
      await this.bufferForNodeDelete(node)
      const newRoot = await this.getNode(keys[0])
      this.rootId = newRoot.id
      newRoot.parent = null
      this.strategy.head.root = this.rootId
      await this.bufferForNodeUpdate(newRoot)
      return
    }
    else if (this.rootId === node.id) {
      const root = await this.getNode(this.rootId)
      await this.bufferForNodeUpdate(root)
      return
    }
    else if (
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

      for (let i = 0, len = parentNode.keys.length; i < len; i++) {
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
      }
      else if (nextNode === null) {
        isPredecessor = true
        pointer = prevNode
        guess = prevValue
      }
      else {
        if (node.values.length + nextNode.values.length < this.order) {
          pointer = nextNode
          guess = postValue
        }
        else {
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
        }
        else {
          pointer.next = node.next
          if (pointer.next) {
            const n = await this.getNode(pointer.next)
            n.prev = pointer.id
            await this.bufferForNodeUpdate(n)
          }
        }
        pointer.values.push(...node.values)

        if (!pointer.leaf) {
          const keys = pointer.keys
          for (const key of keys) {
            const n = await this.getNode(key)
            n.parent = pointer.id
            await this.bufferForNodeUpdate(n)
          }
        }

        await this._deleteEntry(await this.getNode(node.parent!), node.id, guess!)
        await this.bufferForNodeUpdate(pointer)
        await this.bufferForNodeDelete(node)
      }
      else {
        if (isPredecessor) {
          let pointerPm
          let pointerKm
          if (!node.leaf) {
            pointerPm = pointer.keys.splice(-1)[0]
            pointerKm = pointer.values.splice(-1)[0]
            node.keys = [pointerPm, ...node.keys]
            node.values = [guess!, ...node.values]
            parentNode = await this.getNode(node.parent!) as BPTreeInternalNode<K, V>
            const nodeIndex = parentNode.keys.indexOf(node.id)
            if (nodeIndex > 0) {
              parentNode.values[nodeIndex - 1] = pointerKm
              await this.bufferForNodeUpdate(parentNode)
            }
          }
          else {
            pointerPm = pointer.keys.splice(-1)[0] as unknown as K[]
            pointerKm = pointer.values.splice(-1)[0]
            node.keys = [pointerPm, ...node.keys]
            node.values = [pointerKm, ...node.values]
            parentNode = await this.getNode(node.parent!) as BPTreeInternalNode<K, V>
            const nodeIndex = parentNode.keys.indexOf(node.id)
            if (nodeIndex > 0) {
              parentNode.values[nodeIndex - 1] = pointerKm
              await this.bufferForNodeUpdate(parentNode)
            }
          }
          await this.bufferForNodeUpdate(node)
          await this.bufferForNodeUpdate(pointer)
        }
        else {
          let pointerP0
          let pointerK0
          if (!node.leaf) {
            pointerP0 = pointer.keys.splice(0, 1)[0]
            pointerK0 = pointer.values.splice(0, 1)[0]
            node.keys = [...node.keys, pointerP0]
            node.values = [...node.values, guess!]
            parentNode = await this.getNode(node.parent!) as BPTreeInternalNode<K, V>
            const pointerIndex = parentNode.keys.indexOf(pointer.id)
            if (pointerIndex > 0) {
              parentNode.values[pointerIndex - 1] = pointerK0
              await this.bufferForNodeUpdate(parentNode)
            }
          }
          else {
            pointerP0 = pointer.keys.splice(0, 1)[0] as unknown as K[]
            pointerK0 = pointer.values.splice(0, 1)[0]
            node.keys = [...node.keys, pointerP0]
            node.values = [...node.values, pointerK0]
            parentNode = await this.getNode(node.parent!) as BPTreeInternalNode<K, V>
            const pointerIndex = parentNode.keys.indexOf(pointer.id)
            if (pointerIndex > 0) {
              parentNode.values[pointerIndex - 1] = pointer.values[0]
              await this.bufferForNodeUpdate(parentNode)
            }
          }
          await this.bufferForNodeUpdate(node)
          await this.bufferForNodeUpdate(pointer)
        }
        if (!pointer.leaf) {
          const keys = pointer.keys as string[]
          for (const key of keys) {
            const n = await this.getNode(key)
            n.parent = pointer.id
            await this.bufferForNodeUpdate(n)
          }
        }
        if (!node.leaf) {
          const keys = node.keys as string[]
          for (const key of keys) {
            const n = await this.getNode(key)
            n.parent = node.id
            await this.bufferForNodeUpdate(n)
          }
        }
        if (!parentNode.leaf) {
          const keys = parentNode.keys as string[]
          for (const key of keys) {
            const n = await this.getNode(key)
            n.parent = parentNode.id
            await this.bufferForNodeUpdate(n)
          }
        }
      }
    } else {
      await this.bufferForNodeUpdate(node)
    }
  }

  protected async _insertInParent(
    node: BPTreeUnknownNode<K, V>,
    value: V,
    pointer: BPTreeUnknownNode<K, V>
  ): Promise<void> {
    if (this.rootId === node.id) {
      const root = await this._createNode(false, [node.id, pointer.id], [value])
      this.rootId = root.id
      this.strategy.head.root = root.id
      node.parent = root.id
      pointer.parent = root.id

      if (pointer.leaf) {
        const nNode = node as any
        nNode.next = pointer.id
        const nPointer = pointer as any
        nPointer.prev = node.id
      }

      await this.bufferForNodeUpdate(node)
      await this.bufferForNodeUpdate(pointer)
      return
    }
    const parentNode = await this.getNode(node.parent!) as BPTreeInternalNode<K, V>

    const nodeIndex = parentNode.keys.indexOf(node.id)
    if (nodeIndex === -1) {
      throw new Error(`Node ${node.id} not found in parent ${parentNode.id}`)
    }
    const insertIndex = nodeIndex

    parentNode.values.splice(insertIndex, 0, value)
    parentNode.keys.splice(insertIndex + 1, 0, pointer.id)
    pointer.parent = parentNode.id

    if (pointer.leaf) {
      const leftSibling = node as BPTreeLeafNode<K, V>
      const oldNextId = leftSibling.next

      pointer.prev = leftSibling.id
      pointer.next = oldNextId
      leftSibling.next = pointer.id

      await this.bufferForNodeUpdate(leftSibling)

      if (oldNextId) {
        const oldNext = await this.getNode(oldNextId) as BPTreeLeafNode<K, V>
        oldNext.prev = pointer.id
        await this.bufferForNodeUpdate(oldNext)
      }
    }

    await this.bufferForNodeUpdate(parentNode)
    await this.bufferForNodeUpdate(pointer)

    if (parentNode.keys.length > this.order) {
      const parentPointer = await this._createNode(false, [], []) as BPTreeInternalNode<K, V>
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
        await this.bufferForNodeUpdate(n)
      }
      for (const k of parentPointer.keys) {
        const n = await this.getNode(k)
        n.parent = parentPointer.id
        await this.bufferForNodeUpdate(n)
      }

      await this._insertInParent(parentNode, midValue, parentPointer)
      await this.bufferForNodeUpdate(parentNode)
    }
  }

  async init(): Promise<void> {
    this.clear()
    const head = await this.strategy.readHead()
    if (head === null) {
      this.order = this.strategy.order
      const root = await this._createNode(true, [], [], true)
      this.rootId = root.id
      this.strategy.head.root = this.rootId
      await this.commitHeadBuffer()
      await this.commitNodeCreateBuffer()
    }
    else {
      const { root, order } = head
      this.strategy.head = head
      this.order = order
      this.rootId = root!
    }
    if (this.order < 3) {
      throw new Error(`The 'order' parameter must be greater than 2. but got a '${this.order}'.`)
    }
  }

  protected async getNode(id: string): Promise<BPTreeUnknownNode<K, V>> {
    if (this._nodeUpdateBuffer.has(id)) {
      return this._nodeUpdateBuffer.get(id)!
    }
    if (this._nodeCreateBuffer.has(id)) {
      return this._nodeCreateBuffer.get(id)!
    }
    const cache = await this.nodes.cache(id)
    return cache.raw
  }

  protected async insertableNode(value: V): Promise<BPTreeLeafNode<K, V>> {
    let node = await this.getNode(this.rootId)
    if (node.parent !== null) {
      node.parent = null
      await this.bufferForNodeUpdate(node)
    }
    while (!node.leaf) {
      const parentId = node.id
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const k = node.keys
        if (this.comparator.isSame(value, nValue)) {
          node = await this.getNode((node.keys as string[])[i + 1])
          if (node.parent !== parentId) {
            node.parent = parentId
            await this.bufferForNodeUpdate(node)
          }
          break
        }
        else if (this.comparator.isLower(value, nValue)) {
          node = await this.getNode((node.keys as string[])[i])
          if (node.parent !== parentId) {
            node.parent = parentId
            await this.bufferForNodeUpdate(node)
          }
          break
        }
        else if (i + 1 === node.values.length) {
          node = await this.getNode((node.keys as string[])[i + 1])
          if (node.parent !== parentId) {
            node.parent = parentId
            await this.bufferForNodeUpdate(node)
          }
          break
        }
      }
    }
    return node as BPTreeLeafNode<K, V>
  }

  protected async insertableNodeByPrimary(value: V): Promise<BPTreeLeafNode<K, V>> {
    let node = await this.getNode(this.rootId)
    if (node.parent !== null) {
      node.parent = null
      await this.bufferForNodeUpdate(node)
    }
    while (!node.leaf) {
      const parentId = node.id
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const k = node.keys
        if (this.comparator.isPrimarySame(value, nValue)) {
          node = await this.getNode((node.keys as string[])[i])
          if (node.parent !== parentId) {
            node.parent = parentId
            await this.bufferForNodeUpdate(node)
          }
          break
        }
        else if (this.comparator.isPrimaryLower(value, nValue)) {
          node = await this.getNode((node.keys as string[])[i])
          if (node.parent !== parentId) {
            node.parent = parentId
            await this.bufferForNodeUpdate(node)
          }
          break
        }
        else if (i + 1 === node.values.length) {
          node = await this.getNode((node.keys as string[])[i + 1])
          if (node.parent !== parentId) {
            node.parent = parentId
            await this.bufferForNodeUpdate(node)
          }
          break
        }
      }
    }
    return node as BPTreeLeafNode<K, V>
  }

  protected async insertableRightestNodeByPrimary(value: V): Promise<BPTreeLeafNode<K, V>> {
    let node = await this.getNode(this.rootId)
    if (node.parent !== null) {
      node.parent = null
      await this.bufferForNodeUpdate(node)
    }
    while (!node.leaf) {
      const parentId = node.id
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const k = node.keys
        if (this.comparator.isPrimaryLower(value, nValue)) {
          node = await this.getNode((node.keys as string[])[i])
          if (node.parent !== parentId) {
            node.parent = parentId
            await this.bufferForNodeUpdate(node)
          }
          break
        }
        if (i + 1 === node.values.length) {
          node = await this.getNode((node.keys as string[])[i + 1])
          if (node.parent !== parentId) {
            node.parent = parentId
            await this.bufferForNodeUpdate(node)
          }
          break
        }
      }
    }
    return node as BPTreeLeafNode<K, V>
  }

  protected async insertableRightestEndNodeByPrimary(value: V): Promise<BPTreeLeafNode<K, V> | null> {
    const node = await this.insertableRightestNodeByPrimary(value)
    if (!node.next) {
      return null
    }
    return await this.getNode(node.next) as BPTreeLeafNode<K, V>
  }

  protected async insertableEndNode(value: V, direction: 1 | -1): Promise<BPTreeLeafNode<K, V> | null> {
    const insertableNode = await this.insertableNode(value)
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
    return await this.getNode(guessNode) as BPTreeLeafNode<K, V>
  }

  protected async leftestNode(): Promise<BPTreeLeafNode<K, V>> {
    let node = await this.getNode(this.rootId)
    if (node.parent !== null) {
      node.parent = null
      await this.bufferForNodeUpdate(node)
    }
    while (!node.leaf) {
      const parentId = node.id
      const keys = node.keys
      node = await this.getNode((node.keys as string[])[0])
      if (node.parent !== parentId) {
        node.parent = parentId
        await this.bufferForNodeUpdate(node)
      }
    }
    return node as BPTreeLeafNode<K, V>
  }

  protected async rightestNode(): Promise<BPTreeLeafNode<K, V>> {
    let node = await this.getNode(this.rootId)
    if (node.parent !== null) {
      node.parent = null
      await this.bufferForNodeUpdate(node)
    }
    while (!node.leaf) {
      const parentId = node.id
      const keys = node.keys
      node = await this.getNode((node.keys as string[])[node.keys.length - 1])
      if (node.parent !== parentId) {
        node.parent = parentId
        await this.bufferForNodeUpdate(node)
      }
    }
    return node as BPTreeLeafNode<K, V>
  }

  public async exists(key: K, value: V): Promise<boolean> {
    const node = await this.insertableNode(value)
    for (let i = 0, len = node.values.length; i < len; i++) {
      if (this.comparator.isSame(value, node.values[i])) {
        const keys = node.keys[i]
        if (keys.includes(key)) {
          return true
        }
      }
    }
    return false
  }

  public async forceUpdate(id?: string): Promise<number> {
    if (id) {
      this.nodes.delete(id)
      await this.getNode(id)
      return 1
    }
    const keys = Array.from(this.nodes.keys())
    for (const key of keys) {
      this.nodes.delete(key)
    }
    for (const key of keys) {
      await this.getNode(key)
    }
    return keys.length
  }

  protected async commitHeadBuffer(): Promise<void> {
    if (!this._strategyDirty) {
      return
    }
    this._strategyDirty = false
    await this.strategy.writeHead(this.strategy.head)
    if (this.strategy.head.root) {
      this.nodes.delete(this.strategy.head.root)
    }
  }

  protected async commitNodeCreateBuffer(): Promise<void> {
    for (const node of this._nodeCreateBuffer.values()) {
      await this.strategy.write(node.id, node)
    }
    this._nodeCreateBuffer.clear()
  }

  protected async commitNodeUpdateBuffer(): Promise<void> {
    for (const node of this._nodeUpdateBuffer.values()) {
      await this.strategy.write(node.id, node)
      this.nodes.delete(node.id)
    }
    this._nodeUpdateBuffer.clear()
  }

  protected async commitNodeDeleteBuffer(): Promise<void> {
    for (const node of this._nodeDeleteBuffer.values()) {
      // Save to shared delete cache before deletion (for active transactions' snapshot isolation)
      this.strategy.sharedDeleteCache.set(node.id, node)
      await this.strategy.delete(node.id)
      this.nodes.delete(node.id)
    }
    this._nodeDeleteBuffer.clear()
  }

  public async get(key: K): Promise<V | undefined> {
    return await this.readLock(async () => {
      let node = await this.leftestNode() as BPTreeLeafNode<K, V>
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
        node = await this.getNode(node.next) as BPTreeLeafNode<K, V>
      }
      return undefined
    })
  }

  public async *keysStream(
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

  public async *whereStream(
    condition: BPTreeCondition<V>,
    limit?: number
  ): AsyncGenerator<[K, V]> {
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
    const startNode = await this.verifierStartNode[driverKey](value) as BPTreeLeafNode<K, V>
    const endNode = await this.verifierEndNode[driverKey](value) as BPTreeLeafNode<K, V> | null
    const direction = this.verifierDirection[driverKey]
    const comparator = this.verifierMap[driverKey]
    const earlyTerminate = this.verifierEarlyTerminate[driverKey]

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
        const verify = this.verifierMap[key as keyof BPTreeCondition<V>]
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

  public async keys(condition: BPTreeCondition<V>, filterValues?: Set<K>): Promise<Set<K>> {
    const set = new Set<K>()
    for await (const key of this.keysStream(condition, filterValues)) {
      set.add(key)
    }
    return set
  }

  public async where(condition: BPTreeCondition<V>): Promise<BPTreePair<K, V>> {
    const map = new Map<K, V>()
    for await (const [key, value] of this.whereStream(condition)) {
      map.set(key, value)
    }
    return map
  }

  public async insert(key: K, value: V): Promise<void> {
    await this.writeLock(async () => {
      const before = await this.insertableNode(value)
      await this._insertAtLeaf(before, key, value)

      if (before.values.length === this.order) {
        const after = await this._createNode(
          true,
          [],
          [],
          true,
          before.parent,
          null,
          null,
        ) as BPTreeLeafNode<K, V>
        const mid = Math.ceil(this.order / 2) - 1
        after.values = before.values.slice(mid + 1)
        after.keys = before.keys.slice(mid + 1)
        before.values = before.values.slice(0, mid + 1)
        before.keys = before.keys.slice(0, mid + 1)
        await this._insertInParent(before, after.values[0], after)
        await this.bufferForNodeUpdate(before)
      }

      await this.commitHeadBuffer()
      await this.commitNodeCreateBuffer()
      await this.commitNodeUpdateBuffer()
    })
  }

  public async delete(key: K, value: V): Promise<void> {
    await this.writeLock(async () => {
      const node = await this.insertableNode(value)
      let i = node.values.length
      while (i--) {
        const nValue = node.values[i]
        if (this.comparator.isSame(value, nValue)) {
          const keys = node.keys[i]
          const keyIndex = keys.indexOf(key)
          if (keyIndex !== -1) {
            keys.splice(keyIndex, 1)
            if (keys.length === 0) {
              node.keys.splice(i, 1)
              node.values.splice(i, 1)
            }
            await this._deleteEntry(node, key as any, value)
            await this.bufferForNodeUpdate(node)
            break
          }
        }
      }

      await this.commitHeadBuffer()
      await this.commitNodeCreateBuffer()
      await this.commitNodeUpdateBuffer()
      await this.commitNodeDeleteBuffer()
    })
  }

  public getHeadData(): SerializableData {
    return this.strategy.head.data
  }

  public async setHeadData(data: SerializableData): Promise<void> {
    this.strategy.head.data = data
    await this.strategy.writeHead(this.strategy.head)
  }

  protected abstract readLock<T>(fn: () => Promise<T>): Promise<T>
  protected abstract writeLock<T>(fn: () => Promise<T>): Promise<T>
}
