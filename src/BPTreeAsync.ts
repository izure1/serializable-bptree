import { Ryoiki } from 'ryoiki'
import { CacheEntanglementAsync } from 'cache-entanglement'
import {
  BPTree,
  BPTreeCondition,
  BPTreeLeafNode,
  BPTreePair,
  BPTreeNodeKey,
  BPTreeUnknownNode,
  BPTreeInternalNode,
  BPTreeConstructorOption,
} from './base/BPTree'
import { SerializeStrategyAsync } from './SerializeStrategyAsync'
import { ValueComparator } from './base/ValueComparator'
import { SerializableData } from './base/SerializeStrategy'

export class BPTreeAsync<K, V> extends BPTree<K, V> {
  declare protected readonly strategy: SerializeStrategyAsync<K, V>
  declare protected readonly nodes: ReturnType<typeof this._createCachedNode>
  private readonly lock: Ryoiki

  constructor(
    strategy: SerializeStrategyAsync<K, V>,
    comparator: ValueComparator<V>,
    option?: BPTreeConstructorOption
  ) {
    super(strategy, comparator, option)
    this.nodes = this._createCachedNode()
    this.lock = new Ryoiki()
  }

  private _createCachedNode() {
    return new CacheEntanglementAsync(async (key) => {
      return await this.strategy.read(key) as BPTreeUnknownNode<K, V>
    }, {
      capacity: this.option.capacity ?? 1000
    })
  }

  protected async readLock<T>(callback: () => Promise<T>): Promise<T> {
    let lockId: string
    return await this.lock.readLock(async (_lockId) => {
      lockId = _lockId
      return await callback()
    }).finally(() => {
      this.lock.readUnlock(lockId)
    })
  }

  protected async writeLock<T>(callback: () => Promise<T>): Promise<T> {
    let lockId: string
    return await this.lock.writeLock(async (_lockId) => {
      lockId = _lockId
      return await callback()
    }).finally(() => {
      this.lock.writeUnlock(lockId)
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
    leaf = false,
    parent: string | null = null,
    next: string | null = null,
    prev: string | null = null
  ): Promise<BPTreeUnknownNode<K, V>> {
    const id = await this._createNodeId(isLeaf)
    const node = {
      id,
      keys,
      values,
      leaf,
      parent,
      next,
      prev,
    } as BPTreeUnknownNode<K, V>
    this._nodeCreateBuffer.set(id, node)
    return node
  }

  protected async _deleteEntry(
    node: BPTreeUnknownNode<K, V>,
    key: BPTreeNodeKey<K>,
    value: V
  ): Promise<void> {
    if (!node.leaf) {
      for (let i = 0, len = node.keys.length; i < len; i++) {
        const nKey = node.keys[i]
        if (nKey === key) {
          node.keys.splice(i, 1)
          this.bufferForNodeUpdate(node)
          break
        }
      }
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        if (this.comparator.isSame(value, nValue)) {
          node.values.splice(i, 1)
          this.bufferForNodeUpdate(node)
          break
        }
      }
    }

    if (this.rootId === node.id && node.keys.length === 1) {
      const keys = node.keys as string[]
      this.bufferForNodeDelete(node)
      const newRoot = await this.getNode(keys[0])
      this.rootId = newRoot.id
      newRoot.parent = null
      this.strategy.head.root = this.rootId
      this.bufferForNodeUpdate(newRoot)
      return
    }
    else if (this.rootId === node.id) {
      const root = await this.getNode(this.rootId)
      this.bufferForNodeUpdate(root)
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
          pointer.prev = node.id
          if (pointer.next) {
            const n = await this.getNode(node.next!)
            n.prev = pointer.id
            this.bufferForNodeUpdate(n)
          }
          if (pointer.prev) {
            const n = await this.getNode(node.id)
            n.next = pointer.id
            this.bufferForNodeUpdate(n)
          }
          if (isPredecessor) {
            pointer.prev = null
          }
        }
        pointer.values.push(...node.values)

        if (!pointer.leaf) {
          const keys = pointer.keys
          for (const key of keys) {
            const node = await this.getNode(key)
            node.parent = pointer.id
            this.bufferForNodeUpdate(node)
          }
        }

        await this._deleteEntry(await this.getNode(node.parent!), node.id, guess!)
        this.bufferForNodeUpdate(pointer)
        this.bufferForNodeDelete(node)
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
            for (let i = 0, len = parentNode.values.length; i < len; i++) {
              const nValue = parentNode.values[i]
              if (this.comparator.isSame(guess!, nValue)) {
                parentNode.values[i] = pointerKm
                this.bufferForNodeUpdate(parentNode)
                break
              }
            }
          }
          else {
            pointerPm = pointer.keys.splice(-1)[0] as unknown as K[]
            pointerKm = pointer.values.splice(-1)[0]
            node.keys = [pointerPm, ...node.keys]
            node.values = [pointerKm, ...node.values]
            parentNode = await this.getNode(node.parent!) as BPTreeInternalNode<K, V>
            for (let i = 0, len = parentNode.values.length; i < len; i++) {
              const nValue = parentNode.values[i]
              if (this.comparator.isSame(guess!, nValue)) {
                parentNode.values[i] = pointerKm
                this.bufferForNodeUpdate(parentNode)
                break
              }
            }
          }
          this.bufferForNodeUpdate(node)
          this.bufferForNodeUpdate(pointer)
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
            for (let i = 0, len = parentNode.values.length; i < len; i++) {
              const nValue = parentNode.values[i]
              if (this.comparator.isSame(guess!, nValue)) {
                parentNode.values[i] = pointerK0
                this.bufferForNodeUpdate(parentNode)
                break
              }
            }
          }
          else {
            pointerP0 = pointer.keys.splice(0, 1)[0] as unknown as K[]
            pointerK0 = pointer.values.splice(0, 1)[0]
            node.keys = [...node.keys, pointerP0]
            node.values = [...node.values, pointerK0]
            parentNode = await this.getNode(node.parent!) as BPTreeInternalNode<K, V>
            for (let i = 0, len = parentNode.values.length; i < len; i++) {
              const nValue = parentNode.values[i]
              if (this.comparator.isSame(guess!, nValue)) {
                parentNode.values[i] = pointer.values[0]
                this.bufferForNodeUpdate(parentNode)
                break
              }
            }
          }
          this.bufferForNodeUpdate(node)
          this.bufferForNodeUpdate(pointer)
        }
        if (!pointer.leaf) {
          for (const key of pointer.keys) {
            const n = await this.getNode(key)
            n.parent = pointer.id
            this.bufferForNodeUpdate(n)
          }
        }
        if (!node.leaf) {
          for (const key of node.keys) {
            const n = await this.getNode(key)
            n.parent = node.id
            this.bufferForNodeUpdate(n)
          }
        }
        if (!parentNode.leaf) {
          for (const key of parentNode.keys) {
            const n = await this.getNode(key)
            n.parent = parentNode.id
            this.bufferForNodeUpdate(n)
          }
        }
      }
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

      // 리프 노드인 경우 next/prev 링크 설정
      if (pointer.leaf) {
        node.next = pointer.id
        pointer.prev = node.id
      }

      this.bufferForNodeUpdate(node)
      this.bufferForNodeUpdate(pointer)
      return
    }
    const parentNode = await this.getNode(node.parent!) as BPTreeInternalNode<K, V>

    // 값의 크기 기준으로 올바른 삽입 위치 찾기
    let insertIndex = 0
    for (let i = 0; i < parentNode.values.length; i++) {
      if (this.comparator.asc(value, parentNode.values[i]) > 0) {
        insertIndex = i + 1
      } else {
        break
      }
    }

    // 값과 포인터 삽입
    parentNode.values.splice(insertIndex, 0, value)
    parentNode.keys.splice(insertIndex + 1, 0, pointer.id)
    pointer.parent = parentNode.id

    // 리프 노드인 경우 next/prev 링크 설정
    if (pointer.leaf) {
      const leftSiblingId = parentNode.keys[insertIndex] as string
      const rightSiblingId = parentNode.keys[insertIndex + 2] as string | undefined

      // 좌측 형제 노드 연결
      if (leftSiblingId) {
        const leftSibling = await this.getNode(leftSiblingId)
        pointer.prev = leftSibling.id
        pointer.next = leftSibling.next
        leftSibling.next = pointer.id
        this.bufferForNodeUpdate(leftSibling)
      }

      // 우측 형제 노드 연결
      if (rightSiblingId) {
        const rightSibling = await this.getNode(rightSiblingId)
        rightSibling.prev = pointer.id
        this.bufferForNodeUpdate(rightSibling)
      }
    }

    this.bufferForNodeUpdate(parentNode)
    this.bufferForNodeUpdate(pointer)

    // 오버플로우 처리
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
        const node = await this.getNode(k)
        node.parent = parentNode.id
        this.bufferForNodeUpdate(node)
      }
      for (const k of parentPointer.keys) {
        const node = await this.getNode(k)
        node.parent = parentPointer.id
        this.bufferForNodeUpdate(node)
      }

      await this._insertInParent(parentNode, midValue, parentPointer)
      this.bufferForNodeUpdate(parentNode)
    }
  }

  async init(): Promise<void> {
    const head = await this.strategy.readHead()
    // first created
    if (head === null) {
      this.order = this.strategy.order
      const root = await this._createNode(true, [], [], true)
      this.rootId = root.id
      this.strategy.head.root = this.rootId
      await this.commitHeadBuffer()
      await this.commitNodeCreateBuffer()
    }
    // loaded
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
    while (!node.leaf) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const k = node.keys
        if (this.comparator.isSame(value, nValue)) {
          node = await this.getNode(k[i + 1])
          break
        }
        else if (this.comparator.isLower(value, nValue)) {
          node = await this.getNode(k[i])
          break
        }
        else if (i + 1 === node.values.length) {
          node = await this.getNode(k[i + 1])
          break
        }
      }
    }
    return node
  }

  /**
   * Find the insertable node using primaryAsc comparison.
   * This allows finding nodes by primary value only, ignoring unique identifiers.
   */
  protected async insertableNodeByPrimary(value: V): Promise<BPTreeLeafNode<K, V>> {
    let node = await this.getNode(this.rootId)
    while (!node.leaf) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const k = node.keys
        if (this.comparator.isPrimarySame(value, nValue)) {
          node = await this.getNode(k[i])
          break
        }
        else if (this.comparator.isPrimaryLower(value, nValue)) {
          node = await this.getNode(k[i])
          break
        }
        else if (i + 1 === node.values.length) {
          node = await this.getNode(k[i + 1])
          break
        }
      }
    }
    return node
  }

  protected async insertableRightestNodeByPrimary(value: V): Promise<BPTreeLeafNode<K, V>> {
    let node = await this.getNode(this.rootId)
    while (!node.leaf) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const k = node.keys

        // If the search value is smaller than the node value, go to the left child
        if (this.comparator.isPrimaryLower(value, nValue)) {
          node = await this.getNode(k[i])
          break
        }
        // If it is the same or larger, continue to the next value
        // If it is the last value, go to the rightmost child
        if (i + 1 === node.values.length) {
          node = await this.getNode(k[i + 1])
          break
        }
      }
    }
    return node
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
    while (!node.leaf) {
      const keys = node.keys
      node = await this.getNode(keys[0])
    }
    return node
  }

  protected async rightestNode(): Promise<BPTreeLeafNode<K, V>> {
    let node = await this.getNode(this.rootId)
    while (!node.leaf) {
      const keys = node.keys
      node = await this.getNode(keys[keys.length - 1])
    }
    return node
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
      await this.strategy.delete(node.id)
      this.nodes.delete(node.id)
    }
    this._nodeDeleteBuffer.clear()
  }

  /**
   * Retrieves the value associated with the given key (PK).
   * Note: This method performs a full scan of leaf nodes as the tree is ordered by Value, not Key.
   * 
   * @param key The key to search for.
   * @returns The value associated with the key, or undefined if not found.
   */
  public async get(key: K): Promise<V | undefined> {
    return this.readLock(async () => {
      let node = await this.leftestNode() as BPTreeLeafNode<K, V>

      while (true) {
        if (node.values) {
          const len = node.values.length
          for (let i = 0; i < len; i++) {
            const keys = node.keys[i]
            for (let j = 0; j < keys.length; j++) {
              if (keys[j] === key) {
                return node.values[i]
              }
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

    // Optimizer: Select the driver condition (priority: equal > range > like)
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

      // Filter: Check other conditions
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
    return this.readLock(async () => {
      const set = new Set<K>()
      for await (const key of this.keysStream(condition, filterValues)) {
        set.add(key)
      }
      return set
    })
  }

  public async where(condition: BPTreeCondition<V>): Promise<BPTreePair<K, V>> {
    return this.readLock(async () => {
      const map = new Map<K, V>()
      for await (const [key, value] of this.whereStream(condition)) {
        map.set(key, value)
      }
      return map
    })
  }

  public async insert(key: K, value: V): Promise<void> {
    return this.writeLock(async () => {
      const before = await this.insertableNode(value)
      this._insertAtLeaf(before, key, value)

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
        this.bufferForNodeUpdate(before)
      }

      await this.commitHeadBuffer()
      await this.commitNodeCreateBuffer()
      await this.commitNodeUpdateBuffer()
    })
  }

  public async delete(key: K, value: V): Promise<void> {
    return this.writeLock(async () => {
      const node = await this.insertableNode(value)
      let i = node.values.length
      while (i--) {
        const nValue = node.values[i]
        if (this.comparator.isSame(value, nValue)) {
          const keys = node.keys[i]
          if (keys.includes(key)) {
            if (keys.length > 1) {
              keys.splice(keys.indexOf(key), 1)
              this.bufferForNodeUpdate(node)
            }
            else if (node.id === this.rootId) {
              node.values.splice(i, 1)
              node.keys.splice(i, 1)
              this.bufferForNodeUpdate(node)
            }
            else {
              keys.splice(keys.indexOf(key), 1)
              node.keys.splice(i, 1)
              node.values.splice(node.values.indexOf(value), 1)
              await this._deleteEntry(node, key, value)
              this.bufferForNodeUpdate(node)
            }
          }
        }
      }
      await this.commitHeadBuffer()
      await this.commitNodeCreateBuffer()
      await this.commitNodeUpdateBuffer()
      await this.commitNodeDeleteBuffer()
    })
  }

  public async exists(key: K, value: V): Promise<boolean> {
    return this.readLock(async () => {
      const node = await this.insertableNode(value)
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        if (this.comparator.isSame(value, nValue)) {
          const keys = node.keys[i]
          return keys.includes(key)
        }
      }
      return false
    })
  }

  public async setHeadData(data: SerializableData): Promise<void> {
    return this.writeLock(async () => {
      this.strategy.head.data = data
      this._strategyDirty = true
      await this.commitHeadBuffer()
    })
  }

  public async forceUpdate(): Promise<number> {
    return this.readLock(async () => {
      const keys = [...this.nodes.keys()]
      this.nodes.clear()
      await this.init()
      for (const key of keys) {
        await this.getNode(key)
      }
      return keys.length
    })
  }
}
