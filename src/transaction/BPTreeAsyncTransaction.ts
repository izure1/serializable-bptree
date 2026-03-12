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

      for (let i = 0, len = newSiblingNodeRecursive.keys.length; i < len; i++) {
        const k = newSiblingNodeRecursive.keys[i]
        const n = this._cloneNode(await this.getNode(k))
        n.parent = newSiblingNodeRecursive.id
        await this._updateNode(n)
      }

      await this._updateNode(parentNode)
      await this._insertInParent(parentNode, midValue, newSiblingNodeRecursive)
    }
  }

  protected async locateLeaf(value: V): Promise<BPTreeLeafNode<K, V>> {
    let node = await this.getNode(this.rootId)
    while (!node.leaf) {
      const { index } = this._binarySearchValues(node.values, value, false, true)
      node = await this.getNode(node.keys[index])
    }
    return node as BPTreeLeafNode<K, V>
  }

  protected async findLowerBoundLeaf(value: V): Promise<BPTreeLeafNode<K, V>> {
    let node = await this.getNode(this.rootId)
    while (!node.leaf) {
      const { index } = this._binarySearchValues(node.values, value, true, false)
      node = await this.getNode(node.keys[index])
    }
    return node as BPTreeLeafNode<K, V>
  }

  protected async findUpperBoundLeaf(value: V): Promise<BPTreeLeafNode<K, V>> {
    let node = await this.getNode(this.rootId)
    while (!node.leaf) {
      const { index } = this._binarySearchValues(node.values, value, true, true)
      node = await this.getNode(node.keys[index])
    }
    return node as BPTreeLeafNode<K, V>
  }

  protected async findOuterBoundaryLeaf(value: V, direction: 1 | -1): Promise<BPTreeLeafNode<K, V> | null> {
    const insertableNode = direction === -1
      ? await this.findLowerBoundLeaf(value)
      : await this.findUpperBoundLeaf(value)
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
    startNode: BPTreeLeafNode<K, V>,
    endNode: BPTreeLeafNode<K, V> | null,
    direction: 1 | -1,
  ): AsyncGenerator<[K, V]> {
    let node = startNode
    let nextNodePromise: Promise<BPTreeUnknownNode<K, V>> | null = null

    while (true) {
      if (endNode && node.id === endNode.id) {
        break
      }

      // Read-ahead: Start loading the next node in the background
      if (direction === 1) {
        if (node.next) {
          nextNodePromise = this.getNode(node.next)
        }
      }
      else {
        if (node.prev) {
          nextNodePromise = this.getNode(node.prev)
        }
      }

      const len = node.values.length
      if (direction === 1) {
        for (let i = 0; i < len; i++) {
          const nValue = node.values[i]
          const keys = node.keys[i]
          for (let j = 0, len = keys.length; j < len; j++) {
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

      if (nextNodePromise) {
        node = await nextNodePromise as BPTreeLeafNode<K, V>
        nextNodePromise = null
      }
      else {
        break
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
    const node = await this.locateLeaf(value)
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
        for (let j = 0, len = keys.length; j < len; j++) {
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
    const conditionKeys = Object.keys(condition)
    if (conditionKeys.length === 0) return

    const resolved = this.resolveStartEndConfigs(condition, order)
    const direction = resolved.direction

    let startNode: BPTreeLeafNode<K, V> | null
    if (resolved.startKey) {
      const startConfig = this.searchConfigs[resolved.startKey][order]
      startNode = await startConfig.start(this, resolved.startValues) as BPTreeLeafNode<K, V> | null
    }
    else {
      startNode = order === 'asc'
        ? await this.leftestNode()
        : await this.rightestNode()
    }

    let endNode: BPTreeLeafNode<K, V> | null = null
    if (resolved.endKey) {
      const endConfig = this.searchConfigs[resolved.endKey][order]
      endNode = await endConfig.end(this, resolved.endValues) as BPTreeLeafNode<K, V> | null
    }

    if (!startNode) return

    const generator = this.getPairsGenerator(
      startNode,
      endNode,
      direction,
    )

    let count = 0
    const intersection = filterValues && filterValues.size > 0 ? filterValues : null
    for await (const pair of generator) {
      const [k, v] = pair
      if (intersection && !intersection.has(k)) {
        continue
      }
      if (this.verify(v, condition)) {
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
      let before = await this.locateLeaf(value)
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
      let cachedLeafId: string | null = null
      let cachedLeafMaxValue: V | null = null

      for (let i = 0, len = sorted.length; i < len; i++) {
        const [key, value] = sorted[i]
        let targetLeaf: BPTreeLeafNode<K, V>
        // 정렬된 데이터이므로 현재 리프의 최대값 이하이면 locateLeaf 스킵
        if (
          cachedLeafId !== null &&
          cachedLeafMaxValue !== null &&
          currentLeaf !== null &&
          (this.comparator.isLower(value, cachedLeafMaxValue) || this.comparator.isSame(value, cachedLeafMaxValue))
        ) {
          targetLeaf = currentLeaf
        }
        else {
          targetLeaf = await this.locateLeaf(value)
        }

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

        cachedLeafId = currentLeaf.id
        const changed = this._insertValueIntoLeaf(currentLeaf, key as K, value)
        modified = modified || changed
        // 리프의 마지막 값을 캐시 (정렬 데이터이므로 항상 최대값)
        cachedLeafMaxValue = currentLeaf.values[currentLeaf.values.length - 1]

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
          cachedLeafId = null
          cachedLeafMaxValue = null
          modified = false
        }
      }

      // 마지막 배치 flush
      if (currentLeaf !== null && modified) {
        await this._updateNode(currentLeaf)
      }
    })
  }

  public async bulkLoad(entries: [K, V][]): Promise<void> {
    if (entries.length === 0) return
    return this.writeLock(0, async () => {
      // 빈 트리 검증: 루트가 리프이고 값이 없어야 함
      const root = await this.getNode(this.rootId)
      if (!root.leaf || root.values.length > 0) {
        throw new Error('bulkLoad can only be called on an empty tree. Use batchInsert for non-empty trees.')
      }

      // 1. value 기준 정렬
      const sorted = [...entries].sort((a, b) => this.comparator.asc(a[1], b[1]))

      // 2. 동일 value 그룹핑 (keys 병합)
      const grouped: { keys: K[], value: V }[] = []
      for (let i = 0, len = sorted.length; i < len; i++) {
        const [key, value] = sorted[i]
        const last = grouped[grouped.length - 1]
        if (last && this.comparator.isSame(last.value, value)) {
          if (!last.keys.includes(key)) {
            last.keys.push(key)
          }
        }
        else {
          grouped.push({ keys: [key], value })
        }
      }

      // 기존 빈 루트 삭제
      await this._deleteNode(root)

      // 3. 리프 노드 생성 (order-1 크기 단위)
      const maxLeafSize = this.order - 1
      const leaves: BPTreeLeafNode<K, V>[] = []

      for (let i = 0, len = grouped.length; i < len; i += maxLeafSize) {
        const chunk = grouped.slice(i, i + maxLeafSize)
        const leafKeys = chunk.map(g => g.keys)
        const leafValues = chunk.map(g => g.value)
        const leaf = await this._createNode(
          true,
          leafKeys,
          leafValues,
          null,
          null,
          null
        ) as BPTreeLeafNode<K, V>
        leaves.push(leaf)
      }

      // 4. 리프 간 linked list 연결
      for (let i = 0, len = leaves.length; i < len; i++) {
        if (i > 0) {
          leaves[i].prev = leaves[i - 1].id
        }
        if (i < len - 1) {
          leaves[i].next = leaves[i + 1].id
        }
        await this._updateNode(leaves[i])
      }

      // 5. Bottom-up 내부 노드 구축
      let currentLevel: BPTreeUnknownNode<K, V>[] = leaves

      while (currentLevel.length > 1) {
        const nextLevel: BPTreeUnknownNode<K, V>[] = []

        for (let i = 0, len = currentLevel.length; i < len; i += this.order) {
          const children = currentLevel.slice(i, i + this.order)
          const childIds = children.map(c => c.id)

          // separator values: 두 번째 자식부터의 첫 번째 값
          const separators: V[] = []
          for (let j = 1, len = children.length; j < len; j++) {
            separators.push(children[j].values[0])
          }

          const internalNode = await this._createNode(
            false,
            childIds,
            separators,
            null,
            null,
            null
          ) as BPTreeInternalNode<K, V>

          // 자식 노드들의 parent 갱신
          for (let j = 0, len = children.length; j < len; j++) {
            const child = children[j]
            child.parent = internalNode.id
            await this._updateNode(child)
          }

          nextLevel.push(internalNode)
        }

        currentLevel = nextLevel
      }

      // 6. 루트 설정
      const newRoot = currentLevel[0]
      await this._writeHead({
        root: newRoot.id,
        order: this.order,
        data: this.strategy.head.data
      })
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
          for (let i = 0, len = keys.length; i < len; i++) {
            const key = keys[i]
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
          for (let i = 0, len = siblingNode.keys.length; i < len; i++) {
            const key = siblingNode.keys[i]
            const n = this._cloneNode(await this.getNode(key))
            n.parent = siblingNode.id
            await this._updateNode(n)
          }
        }
        if (!node.leaf) {
          for (let i = 0, len = node.keys.length; i < len; i++) {
            const key = node.keys[i]
            const n = this._cloneNode(await this.getNode(key))
            n.parent = node.id
            await this._updateNode(n)
          }
        }
        if (!parentNode.leaf) {
          for (let i = 0, len = parentNode.keys.length; i < len; i++) {
            const key = parentNode.keys[i]
            const n = this._cloneNode(await this.getNode(key))
            n.parent = parentNode.id
            await this._updateNode(n)
          }
        }
      }
    }
    else {
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

      let node = await this.findLowerBoundLeaf(value)
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
        else {
          this.mvcc.rollback()
        }
      }
    }
    else {
      this.mvcc.rollback()
    }
    return result
  }

  public async rollback(): Promise<TransactionResult<string, BPTreeNode<K, V>>> {
    return this.mvcc.rollback()
  }
}
