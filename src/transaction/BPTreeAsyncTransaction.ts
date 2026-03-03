import type { TransactionResult } from 'mvcc-api'
import type {
  AsyncBPTreeMVCC,
  BPTreeCondition,
  BPTreeConstructorOption,
  BPTreeInternalNode,
  BPTreeLeafNode,
  BPTreeNode,
  BPTreeNodeKey,
  BPTreePair,
  BPTreeUnknownNode,
  SerializableData,
  SerializeStrategyHead,
  BPTreeSearchOption,
} from '../types'
import { Ryoiki } from 'ryoiki'
import { BPTreeTransaction } from '../base/BPTreeTransaction'
import { SerializeStrategyAsync } from '../SerializeStrategyAsync'
import { ValueComparator } from '../base/ValueComparator'

export class BPTreeAsyncTransaction<K, V> extends BPTreeTransaction<K, V> {
  declare protected readonly rootTx: BPTreeAsyncTransaction<K, V>
  declare protected readonly mvccRoot: AsyncBPTreeMVCC<K, V>
  declare protected readonly mvcc: AsyncBPTreeMVCC<K, V>
  declare protected readonly strategy: SerializeStrategyAsync<K, V>
  declare protected readonly comparator: ValueComparator<V>
  declare protected readonly option: BPTreeConstructorOption
  protected readonly lock: Ryoiki

  constructor(
    rootTx: BPTreeAsyncTransaction<K, V> | null,
    mvccRoot: AsyncBPTreeMVCC<K, V>,
    mvcc: AsyncBPTreeMVCC<K, V>,
    strategy: SerializeStrategyAsync<K, V>,
    comparator: ValueComparator<V>,
    option?: BPTreeConstructorOption
  ) {
    super(
      rootTx,
      mvccRoot,
      mvcc,
      strategy,
      comparator,
      option,
    )
    this.lock = new Ryoiki()
  }

  protected async writeLock<T>(id: number, fn: () => Promise<T>): Promise<T> {
    let lockId: string
    return this.lock.writeLock([id, id + 0.1], async (_lockId) => {
      lockId = _lockId
      return fn()
    }).finally(() => {
      this.lock.writeUnlock(lockId)
    })
  }

  protected async getNode(id: string): Promise<BPTreeUnknownNode<K, V>> {
    return await this.mvcc.read(id) as BPTreeUnknownNode<K, V>
  }

  /**
   * Create a new node with a unique ID.
   */
  protected async _createNode(
    leaf: boolean,
    keys: string[] | K[][],
    values: V[],
    parent: string | null = null,
    next: string | null = null,
    prev: string | null = null
  ): Promise<BPTreeUnknownNode<K, V>> {
    const id = await this.strategy.id(leaf)
    const node = {
      id,
      keys,
      values,
      leaf,
      parent,
      next,
      prev
    } as BPTreeUnknownNode<K, V>
    await this.mvcc.create(id, node)
    return node
  }

  protected async _updateNode(node: BPTreeUnknownNode<K, V>): Promise<void> {
    if (this.mvcc.isDeleted(node.id)) {
      return
    }
    await this.mvcc.write(node.id, node)
  }

  protected async _deleteNode(node: BPTreeUnknownNode<K, V>): Promise<void> {
    if (this.mvcc.isDeleted(node.id)) {
      return
    }
    await this.mvcc.delete(node.id)
  }

  protected async _readHead(): Promise<SerializeStrategyHead | null> {
    return await this.mvcc.read('__HEAD__') as unknown as SerializeStrategyHead | null
  }

  protected async _writeHead(head: SerializeStrategyHead): Promise<void> {
    if (!(await this.mvcc.exists('__HEAD__'))) {
      await this.mvcc.create('__HEAD__', head as unknown as BPTreeUnknownNode<K, V>)
    }
    else {
      await this.mvcc.write('__HEAD__', head as unknown as BPTreeUnknownNode<K, V>)
    }
    this.rootId = head.root!
  }

  protected async _insertAtLeaf(node: BPTreeUnknownNode<K, V>, key: BPTreeNodeKey<K>, value: V): Promise<BPTreeUnknownNode<K, V>> {
    let leaf = node as BPTreeLeafNode<K, V>
    if (leaf.values.length) {
      for (let i = 0, len = leaf.values.length; i < len; i++) {
        const nValue = leaf.values[i]
        if (this.comparator.isSame(value, nValue)) {
          const keys = leaf.keys[i]
          if (keys.includes(key as K)) {
            break
          }
          leaf = this._cloneNode(leaf)
          leaf.keys[i].push(key as K)
          await this._updateNode(leaf)
          return leaf
        }
        else if (this.comparator.isLower(value, nValue)) {
          leaf = this._cloneNode(leaf)
          leaf.values.splice(i, 0, value)
          leaf.keys.splice(i, 0, [key as K])
          await this._updateNode(leaf)
          return leaf
        }
        else if (i + 1 === leaf.values.length) {
          leaf = this._cloneNode(leaf)
          leaf.values.push(value)
          leaf.keys.push([key as K])
          await this._updateNode(leaf)
          return leaf
        }
      }
    }
    else {
      leaf = this._cloneNode(leaf)
      leaf.values = [value]
      leaf.keys = [[key as K]]
      await this._updateNode(leaf)
      return leaf
    }
    return leaf
  }

  protected async _insertInParent(node: BPTreeUnknownNode<K, V>, value: V, newSiblingNode: BPTreeUnknownNode<K, V>): Promise<void> {
    if (this.rootId === node.id) {
      node = this._cloneNode(node)
      newSiblingNode = this._cloneNode(newSiblingNode)
      const root = await this._createNode(false, [node.id, newSiblingNode.id], [value])
      this.rootId = root.id
      node.parent = root.id
      newSiblingNode.parent = root.id

      if (newSiblingNode.leaf) {
        (node as any).next = newSiblingNode.id;
        (newSiblingNode as any).prev = node.id;
      }

      await this._writeHead({
        root: root.id,
        order: this.order,
        data: this.strategy.head.data
      })

      await this._updateNode(node)
      await this._updateNode(newSiblingNode)
      return
    }

    const parentNode = this._cloneNode(await this.getNode(node.parent!)) as BPTreeInternalNode<K, V>
    const nodeIndex = parentNode.keys.indexOf(node.id)

    if (nodeIndex === -1) {
      throw new Error(`Node ${node.id} not found in parent ${parentNode.id}`)
    }

    parentNode.values.splice(nodeIndex, 0, value)
    parentNode.keys.splice(nodeIndex + 1, 0, newSiblingNode.id)

    newSiblingNode = this._cloneNode(newSiblingNode)
    newSiblingNode.parent = parentNode.id

    if (newSiblingNode.leaf) {
      const leftSibling = this._cloneNode(node) as unknown as BPTreeLeafNode<K, V>
      const oldNextId = leftSibling.next

      newSiblingNode.prev = leftSibling.id
      newSiblingNode.next = oldNextId
      leftSibling.next = newSiblingNode.id

      await this._updateNode(leftSibling)

      if (oldNextId) {
        const oldNext = this._cloneNode(await this.getNode(oldNextId)) as BPTreeLeafNode<K, V>
        oldNext.prev = newSiblingNode.id
        await this._updateNode(oldNext)
      }
    }

    await this._updateNode(parentNode)
    await this._updateNode(newSiblingNode)

    if (parentNode.keys.length > this.order) {
      const newSiblingNodeRecursive = await this._createNode(false, [], []) as BPTreeInternalNode<K, V>
      newSiblingNodeRecursive.parent = parentNode.parent
      const mid = Math.ceil(this.order / 2) - 1
      newSiblingNodeRecursive.values = parentNode.values.slice(mid + 1)
      newSiblingNodeRecursive.keys = parentNode.keys.slice(mid + 1)
      const midValue = parentNode.values[mid]
      parentNode.values = parentNode.values.slice(0, mid)
      parentNode.keys = parentNode.keys.slice(0, mid + 1)

      for (const k of parentNode.keys) {
        const n = this._cloneNode(await this.getNode(k))
        n.parent = parentNode.id
        await this._updateNode(n)
      }
      for (const k of newSiblingNodeRecursive.keys) {
        const n = this._cloneNode(await this.getNode(k))
        n.parent = newSiblingNodeRecursive.id
        await this._updateNode(n)
      }

      await this._updateNode(parentNode)
      await this._insertInParent(parentNode, midValue, newSiblingNodeRecursive)
    }
  }

  protected async insertableNode(value: V): Promise<BPTreeLeafNode<K, V>> {
    let node = await this.getNode(this.rootId)
    while (!node.leaf) {
      const { index } = this._binarySearchValues(node.values, value, false, true)
      node = await this.getNode(node.keys[index])
    }
    return node as BPTreeLeafNode<K, V>
  }

  protected async insertableNodeByPrimary(value: V): Promise<BPTreeLeafNode<K, V>> {
    let node = await this.getNode(this.rootId)
    while (!node.leaf) {
      const { index } = this._binarySearchValues(node.values, value, true, false)
      node = await this.getNode(node.keys[index])
    }
    return node as BPTreeLeafNode<K, V>
  }

  protected async insertableRightestNodeByPrimary(value: V): Promise<BPTreeLeafNode<K, V>> {
    let node = await this.getNode(this.rootId)
    while (!node.leaf) {
      const { index } = this._binarySearchValues(node.values, value, true, true)
      node = await this.getNode(node.keys[index])
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
    const insertableNode = direction === -1
      ? await this.insertableNodeByPrimary(value)
      : await this.insertableRightestNodeByPrimary(value)
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
    if (node === null) {
      debugger
    }
    while (!node.leaf) {
      const keys = node.keys
      node = await this.getNode(keys[0])
    }
    return node as BPTreeLeafNode<K, V>
  }

  protected async rightestNode(): Promise<BPTreeLeafNode<K, V>> {
    let node = await this.getNode(this.rootId)
    while (!node.leaf) {
      const keys = node.keys
      node = await this.getNode(keys[keys.length - 1])
    }
    return node as BPTreeLeafNode<K, V>
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
    let nextNodePromise: Promise<BPTreeUnknownNode<K, V>> | null = null

    while (!done) {
      if (endNode && node.id === endNode.id) {
        done = true
        break
      }

      // Read-ahead: Start loading the next node in the background
      if (direction === 1) {
        if (node.next && !done) {
          nextNodePromise = this.getNode(node.next)
        }
      }
      else {
        if (node.prev && !done) {
          nextNodePromise = this.getNode(node.prev)
        }
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
          }
          else if (earlyTerminate && hasMatched) {
            done = true
            break
          }
        }
      }
      else {
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
          }
          else if (earlyTerminate && hasMatched) {
            done = true
            break
          }
        }
      }

      if (done) {
        if (nextNodePromise) await nextNodePromise
        break
      }

      if (nextNodePromise) {
        node = await nextNodePromise as BPTreeLeafNode<K, V>
        nextNodePromise = null
      }
      else {
        done = true
      }
    }
  }

  public async init(): Promise<void> {
    if (this.rootTx !== this) {
      throw new Error('Cannot call init on a nested transaction')
    }
    return await this._initInternal()
  }

  protected async _initInternal(): Promise<void> {
    if (this.isInitialized) {
      throw new Error('Transaction already initialized')
    }
    if (this.isDestroyed) {
      throw new Error('Transaction already destroyed')
    }
    this.isInitialized = true
    try {
      this._clearCache()
      const head = await this._readHead()
      if (head === null) {
        this.order = this.strategy.order
        const root = await this._createNode(true, [], [])
        await this._writeHead({
          root: root.id,
          order: this.order,
          data: this.strategy.head.data
        })
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
    } catch (e) {
      this.isInitialized = false
      throw e
    }
  }

  public async exists(key: K, value: V): Promise<boolean> {
    const node = await this.insertableNode(value)
    const { index, found } = this._binarySearchValues(node.values, value)
    if (found) {
      const keys = node.keys[index]
      if (keys.includes(key)) {
        return true
      }
    }
    return false
  }

  public async get(key: K): Promise<V | undefined> {
    let node = await this.leftestNode()
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
  }

  public async *keysStream(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>
  ): AsyncGenerator<K> {
    const { filterValues, limit, order = 'asc' } = options ?? {}
    const stream = this.whereStream(condition, options)
    const intersection = filterValues && filterValues.size > 0 ? filterValues : null
    let count = 0
    for await (const [key] of stream) {
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

  public async *whereStream(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>
  ): AsyncGenerator<[K, V]> {
    const { filterValues, limit, order = 'asc' } = options ?? {}
    const driverKey = this.getDriverKey(condition)
    if (!driverKey) return

    const value = condition[driverKey] as V
    const v = this.ensureValues(value)
    const config = this.searchConfigs[driverKey][order]

    let startNode = await config.start(this, v)
    let endNode = await config.end(this, v)
    const direction = config.direction
    const earlyTerminate = config.earlyTerminate

    if (order === 'desc' && !startNode) {
      startNode = await this.rightestNode()
    }
    if (order === 'asc' && !startNode) {
      startNode = await this.leftestNode()
    }
    if (!startNode) return

    const comparator = this.verifierMap[driverKey]

    const generator = this.getPairsGenerator(
      value,
      startNode as BPTreeLeafNode<K, V>,
      endNode as BPTreeLeafNode<K, V> | null,
      comparator,
      direction as 1 | -1,
      earlyTerminate
    )

    let count = 0
    const intersection = filterValues && filterValues.size > 0 ? filterValues : null
    for await (const pair of generator) {
      const [k, v] = pair
      if (intersection && !intersection.has(k)) {
        continue
      }
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

  public async keys(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>
  ): Promise<Set<K>> {
    const set = new Set<K>()
    for await (const key of this.keysStream(condition, options)) {
      set.add(key)
    }
    return set
  }

  public async where(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>
  ): Promise<BPTreePair<K, V>> {
    const map = new Map<K, V>()
    for await (const [key, value] of this.whereStream(condition, options)) {
      map.set(key, value)
    }
    return map
  }

  public async insert(key: K, value: V): Promise<void> {
    return this.writeLock(0, async () => {
      let before = await this.insertableNode(value)
      before = await this._insertAtLeaf(before, key, value) as BPTreeLeafNode<K, V>

      if (before.values.length === this.order) {
        let after = await this._createNode(
          true,
          [],
          [],
          before.parent,
          null,
          null,
        ) as BPTreeLeafNode<K, V>
        const mid = Math.ceil(this.order / 2) - 1
        after = this._cloneNode(after)
        after.values = before.values.slice(mid + 1)
        after.keys = before.keys.slice(mid + 1)
        before.values = before.values.slice(0, mid + 1)
        before.keys = before.keys.slice(0, mid + 1)
        await this._updateNode(before)
        await this._updateNode(after)
        await this._insertInParent(before, after.values[0], after)
      }
    })
  }

  public async batchInsert(entries: [K, V][]): Promise<void> {
    if (entries.length === 0) return
    return this.writeLock(0, async () => {
      const sorted = [...entries].sort((a, b) => this.comparator.asc(a[1], b[1]))
      let currentLeaf: BPTreeLeafNode<K, V> | null = null
      let modified = false

      for (const [key, value] of sorted) {
        const targetLeaf = await this.insertableNode(value)

        if (currentLeaf !== null && currentLeaf.id === targetLeaf.id) {
          // 같은 리프 — clone/update 없이 직접 삽입
        }
        else {
          // 다른 리프 — 이전 배치 flush 후 새 배치 시작
          if (currentLeaf !== null && modified) {
            await this._updateNode(currentLeaf)
          }
          currentLeaf = this._cloneNode(targetLeaf)
          modified = false
        }

        const changed = this._insertValueIntoLeaf(currentLeaf, key as K, value)
        modified = modified || changed

        if (currentLeaf.values.length === this.order) {
          // overflow — flush 후 split
          await this._updateNode(currentLeaf)
          let after = await this._createNode(
            true,
            [],
            [],
            currentLeaf.parent,
            null,
            null,
          ) as BPTreeLeafNode<K, V>
          const mid = Math.ceil(this.order / 2) - 1
          after = this._cloneNode(after)
          after.values = currentLeaf.values.slice(mid + 1)
          after.keys = currentLeaf.keys.slice(mid + 1)
          currentLeaf.values = currentLeaf.values.slice(0, mid + 1)
          currentLeaf.keys = currentLeaf.keys.slice(0, mid + 1)
          await this._updateNode(currentLeaf)
          await this._updateNode(after)
          await this._insertInParent(currentLeaf, after.values[0], after)
          currentLeaf = null
          modified = false
        }
      }

      // 마지막 배치 flush
      if (currentLeaf !== null && modified) {
        await this._updateNode(currentLeaf)
      }
    })
  }

  protected async _deleteEntry(
    node: BPTreeUnknownNode<K, V>,
    key: BPTreeNodeKey<K>
  ): Promise<BPTreeUnknownNode<K, V>> {
    if (!node.leaf) {
      let keyIndex = -1
      for (let i = 0, len = node.keys.length; i < len; i++) {
        if (node.keys[i] === key) {
          keyIndex = i
          break
        }
      }

      if (keyIndex !== -1) {
        node = this._cloneNode(node)
        node.keys.splice(keyIndex, 1)
        const valueIndex = keyIndex > 0 ? keyIndex - 1 : 0
        node.values.splice(valueIndex, 1)
        await this._updateNode(node)
      }
    }

    if (this.rootId === node.id && node.keys.length === 1 && !node.leaf) {
      const keys = node.keys as string[]
      this._deleteNode(node)
      const newRoot = this._cloneNode(await this.getNode(keys[0]))
      newRoot.parent = null
      await this._updateNode(newRoot)
      await this._writeHead({
        root: newRoot.id,
        order: this.order,
        data: this.strategy.head.data
      })
      return node
    }
    else if (this.rootId === node.id) {
      await this._writeHead({
        root: node.id,
        order: this.order,
        data: this.strategy.head.data
      })
      return node
    }
    else if (
      (node.keys.length < Math.ceil(this.order / 2) && !node.leaf) ||
      (node.values.length < Math.ceil((this.order - 1) / 2) && node.leaf)
    ) {
      if (node.parent === null) {
        return node
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
        if (node.values.length + nextNode.values.length < this.order) {
          siblingNode = nextNode
          guess = postValue
        }
        else {
          isPredecessor = true
          siblingNode = prevNode
          guess = prevValue
        }
      }
      if (!siblingNode) {
        return node
      }

      node = this._cloneNode(node)
      siblingNode = this._cloneNode(siblingNode)

      if (node.values.length + siblingNode.values.length < this.order) {
        if (!isPredecessor) {
          const pTemp = siblingNode
          siblingNode = node as BPTreeInternalNode<K, V>
          node = pTemp
        }
        siblingNode.keys.push(...node.keys as any)
        if (!node.leaf) {
          siblingNode.values.push(guess!)
        }
        else {
          siblingNode.next = node.next
          if (siblingNode.next) {
            const n = this._cloneNode(await this.getNode(siblingNode.next))
            n.prev = siblingNode.id
            await this._updateNode(n)
          }
        }
        siblingNode.values.push(...node.values)

        if (!siblingNode.leaf) {
          const keys = siblingNode.keys
          for (const key of keys) {
            const node = this._cloneNode(await this.getNode(key))
            node.parent = siblingNode.id
            await this._updateNode(node)
          }
        }

        this._deleteNode(node)
        await this._updateNode(siblingNode)
        await this._deleteEntry(await this.getNode(node.parent!), node.id)
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
            parentNode = this._cloneNode(await this.getNode(node.parent!)) as BPTreeInternalNode<K, V>
            const nodeIndex = parentNode.keys.indexOf(node.id)
            if (nodeIndex > 0) {
              parentNode.values[nodeIndex - 1] = pointerKm
              await this._updateNode(parentNode)
            }
          }
          else {
            pointerPm = siblingNode.keys.splice(-1)[0] as unknown as K[]
            pointerKm = siblingNode.values.splice(-1)[0]
            node.keys = [pointerPm, ...node.keys]
            node.values = [pointerKm, ...node.values]
            parentNode = this._cloneNode(await this.getNode(node.parent!)) as BPTreeInternalNode<K, V>
            const nodeIndex = parentNode.keys.indexOf(node.id)
            if (nodeIndex > 0) {
              parentNode.values[nodeIndex - 1] = pointerKm
              await this._updateNode(parentNode)
            }
          }
          await this._updateNode(node)
          await this._updateNode(siblingNode)
        }
        else {
          let pointerP0
          let pointerK0
          if (!node.leaf) {
            pointerP0 = siblingNode.keys.splice(0, 1)[0]
            pointerK0 = siblingNode.values.splice(0, 1)[0]
            node.keys = [...node.keys, pointerP0]
            node.values = [...node.values, guess!]
            parentNode = this._cloneNode(await this.getNode(node.parent!)) as BPTreeInternalNode<K, V>
            const pointerIndex = parentNode.keys.indexOf(siblingNode.id)
            if (pointerIndex > 0) {
              parentNode.values[pointerIndex - 1] = pointerK0
              await this._updateNode(parentNode)
            }
          }
          else {
            pointerP0 = siblingNode.keys.splice(0, 1)[0] as unknown as K[]
            pointerK0 = siblingNode.values.splice(0, 1)[0]
            node.keys = [...node.keys, pointerP0]
            node.values = [...node.values, pointerK0]
            parentNode = this._cloneNode(await this.getNode(node.parent!)) as BPTreeInternalNode<K, V>
            const pointerIndex = parentNode.keys.indexOf(siblingNode.id)
            if (pointerIndex > 0) {
              parentNode.values[pointerIndex - 1] = siblingNode.values[0]
              await this._updateNode(parentNode)
            }
          }
          await this._updateNode(node)
          await this._updateNode(siblingNode)
        }
        if (!siblingNode.leaf) {
          for (const key of siblingNode.keys) {
            const n = this._cloneNode(await this.getNode(key))
            n.parent = siblingNode.id
            await this._updateNode(n)
          }
        }
        if (!node.leaf) {
          for (const key of node.keys) {
            const n = this._cloneNode(await this.getNode(key))
            n.parent = node.id
            await this._updateNode(n)
          }
        }
        if (!parentNode.leaf) {
          for (const key of parentNode.keys) {
            const n = this._cloneNode(await this.getNode(key))
            n.parent = parentNode.id
            await this._updateNode(n)
          }
        }
      }
    } else {
      await this._updateNode(this._cloneNode(node))
    }
    return node
  }

  public async delete(key: K, value?: V): Promise<void> {
    return this.writeLock(0, async () => {
      if (value === undefined) {
        value = await this.get(key)
      }

      if (value === undefined) {
        return
      }

      let node = await this.insertableNodeByPrimary(value)
      let found = false
      while (true) {
        let i = node.values.length
        while (i--) {
          const nValue = node.values[i]
          if (this.comparator.isSame(value, nValue)) {
            const keys = node.keys[i]
            const keyIndex = keys.indexOf(key)
            if (keyIndex !== -1) {
              node = this._cloneNode(node)
              const freshKeys = node.keys[i]
              freshKeys.splice(keyIndex, 1)
              if (freshKeys.length === 0) {
                node.keys.splice(i, 1)
                node.values.splice(i, 1)
              }
              await this._updateNode(node)
              node = await this._deleteEntry(node, key) as BPTreeLeafNode<K, V>
              found = true
              break
            }
          }
        }
        if (found) break
        if (node.next) {
          node = await this.getNode(node.next) as BPTreeLeafNode<K, V>
          continue
        }
        break
      }
    })
  }

  public async getHeadData(): Promise<SerializableData> {
    const head = await this._readHead()
    if (head === null) {
      throw new Error('Head not found')
    }
    return head.data
  }

  public async setHeadData(data: SerializableData): Promise<void> {
    const head = await this._readHead()
    if (head === null) {
      throw new Error('Head not found')
    }
    await this._writeHead({
      root: head.root,
      order: head.order,
      data,
    })
  }

  public async commit(label?: string): Promise<TransactionResult<string, BPTreeNode<K, V>>> {
    let result = await this.mvcc.commit(label)
    if (result.success) {
      const isRootTx = this.rootTx === this
      if (!isRootTx) {
        result = await this.rootTx.commit(label)
        if (result.success) {
          this.rootTx.rootId = this.rootId
        }
      }
    }
    return result
  }

  public async rollback(): Promise<TransactionResult<string, BPTreeNode<K, V>>> {
    return this.mvcc.rollback()
  }
}
