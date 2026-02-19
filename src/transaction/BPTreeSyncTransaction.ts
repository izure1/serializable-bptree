import type { TransactionResult } from 'mvcc-api'
import type {
  BPTreeCondition,
  BPTreeConstructorOption,
  BPTreeInternalNode,
  BPTreeLeafNode,
  BPTreeNode,
  BPTreeNodeKey,
  BPTreeOrder,
  BPTreePair,
  BPTreeUnknownNode,
  SerializableData,
  SerializeStrategyHead,
  SyncBPTreeMVCC
} from '../types'
import { BPTreeTransaction } from '../base/BPTreeTransaction'
import { SerializeStrategySync } from '../SerializeStrategySync'
import { ValueComparator } from '../base/ValueComparator'

export class BPTreeSyncTransaction<K, V> extends BPTreeTransaction<K, V> {
  declare protected readonly rootTx: BPTreeSyncTransaction<K, V>
  declare protected readonly mvccRoot: SyncBPTreeMVCC<K, V>
  declare protected readonly mvcc: SyncBPTreeMVCC<K, V>
  declare protected readonly strategy: SerializeStrategySync<K, V>
  declare protected readonly comparator: ValueComparator<V>
  declare protected readonly option: BPTreeConstructorOption

  constructor(
    rootTx: BPTreeSyncTransaction<K, V>,
    mvccRoot: SyncBPTreeMVCC<K, V>,
    mvcc: SyncBPTreeMVCC<K, V>,
    strategy: SerializeStrategySync<K, V>,
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
  }

  protected getNode(id: string): BPTreeUnknownNode<K, V> {
    return this.mvcc.read(id) as BPTreeUnknownNode<K, V>
  }

  /**
   * Create a new node with a unique ID.
   */
  protected _createNode(
    leaf: boolean,
    keys: string[] | K[][],
    values: V[],
    parent: string | null = null,
    next: string | null = null,
    prev: string | null = null
  ): BPTreeUnknownNode<K, V> {
    const id = this.strategy.id(leaf)
    const node = {
      id,
      keys,
      values,
      leaf,
      parent,
      next,
      prev
    } as BPTreeUnknownNode<K, V>
    this.mvcc.create(id, node)
    return node
  }

  protected _updateNode(node: BPTreeUnknownNode<K, V>): void {
    if (this.mvcc.isDeleted(node.id)) {
      return
    }
    this.mvcc.write(node.id, node)
  }

  protected _deleteNode(node: BPTreeUnknownNode<K, V>): void {
    if (this.mvcc.isDeleted(node.id)) {
      return
    }
    this.mvcc.delete(node.id)
  }

  protected _readHead(): SerializeStrategyHead | null {
    return this.mvcc.read('__HEAD__') as unknown as SerializeStrategyHead | null
  }

  protected _writeHead(head: SerializeStrategyHead): void {
    if (!this.mvcc.exists('__HEAD__')) {
      this.mvcc.create('__HEAD__', head as any)
    }
    else {
      this.mvcc.write('__HEAD__', head as any)
    }
    this.rootId = head.root!
  }

  protected _insertAtLeaf(node: BPTreeUnknownNode<K, V>, key: BPTreeNodeKey<K>, value: V): BPTreeUnknownNode<K, V> {
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
          this._updateNode(leaf)
          return leaf
        }
        else if (this.comparator.isLower(value, nValue)) {
          leaf = this._cloneNode(leaf)
          leaf.values.splice(i, 0, value)
          leaf.keys.splice(i, 0, [key as K])
          this._updateNode(leaf)
          return leaf
        }
        else if (i + 1 === leaf.values.length) {
          leaf = this._cloneNode(leaf)
          leaf.values.push(value)
          leaf.keys.push([key as K])
          this._updateNode(leaf)
          return leaf
        }
      }
    }
    else {
      leaf = this._cloneNode(leaf)
      leaf.values = [value]
      leaf.keys = [[key as K]]
      this._updateNode(leaf)
      return leaf
    }
    return leaf
  }

  protected _insertInParent(node: BPTreeUnknownNode<K, V>, value: V, newSiblingNode: BPTreeUnknownNode<K, V>): void {
    if (this.rootId === node.id) {
      node = this._cloneNode(node)
      newSiblingNode = this._cloneNode(newSiblingNode)
      const root = this._createNode(false, [node.id, newSiblingNode.id], [value])
      this.rootId = root.id
      node.parent = root.id
      newSiblingNode.parent = root.id

      if (newSiblingNode.leaf) {
        (node as any).next = newSiblingNode.id;
        (newSiblingNode as any).prev = node.id;
      }

      this._writeHead({
        root: root.id,
        order: this.order,
        data: this.strategy.head.data
      })

      this._updateNode(node)
      this._updateNode(newSiblingNode)
      return
    }

    const parentNode = this._cloneNode(this.getNode(node.parent!)) as BPTreeInternalNode<K, V>
    const nodeIndex = parentNode.keys.indexOf(node.id)

    if (nodeIndex === -1) {
      throw new Error(`Node ${node.id} not found in parent ${parentNode.id}`)
    }

    parentNode.values.splice(nodeIndex, 0, value)
    parentNode.keys.splice(nodeIndex + 1, 0, newSiblingNode.id)

    // newSiblingNode must be cloned to update its parent
    newSiblingNode = this._cloneNode(newSiblingNode)
    newSiblingNode.parent = parentNode.id

    if (newSiblingNode.leaf) {
      // leftSibling (node) must be cloned to update its next
      const leftSibling = this._cloneNode(node) as unknown as BPTreeLeafNode<K, V>
      const oldNextId = leftSibling.next

      newSiblingNode.prev = leftSibling.id
      newSiblingNode.next = oldNextId
      leftSibling.next = newSiblingNode.id

      this._updateNode(leftSibling)

      if (oldNextId) {
        const oldNext = this._cloneNode(this.getNode(oldNextId)) as BPTreeLeafNode<K, V>
        oldNext.prev = newSiblingNode.id
        this._updateNode(oldNext)
      }
    }

    this._updateNode(parentNode)
    this._updateNode(newSiblingNode)

    if (parentNode.keys.length > this.order) {
      const newSiblingNodeRecursive = this._createNode(false, [], []) as BPTreeInternalNode<K, V>
      newSiblingNodeRecursive.parent = parentNode.parent
      const mid = Math.ceil(this.order / 2) - 1
      newSiblingNodeRecursive.values = parentNode.values.slice(mid + 1)
      newSiblingNodeRecursive.keys = parentNode.keys.slice(mid + 1)
      const midValue = parentNode.values[mid]
      parentNode.values = parentNode.values.slice(0, mid)
      parentNode.keys = parentNode.keys.slice(0, mid + 1)

      for (const k of parentNode.keys) {
        const n = this._cloneNode(this.getNode(k))
        n.parent = parentNode.id
        this._updateNode(n)
      }
      for (const k of newSiblingNodeRecursive.keys) {
        const n = this._cloneNode(this.getNode(k))
        n.parent = newSiblingNodeRecursive.id
        this._updateNode(n)
      }

      this._updateNode(parentNode)
      this._insertInParent(parentNode, midValue, newSiblingNodeRecursive)
    }
  }

  protected insertableNode(value: V): BPTreeLeafNode<K, V> {
    let node = this.getNode(this.rootId)
    while (!node.leaf) {
      const { index } = this._binarySearchValues(node.values, value, false, true)
      node = this.getNode(node.keys[index])
    }
    return node as BPTreeLeafNode<K, V>
  }

  protected insertableNodeByPrimary(value: V): BPTreeLeafNode<K, V> {
    let node = this.getNode(this.rootId)
    while (!node.leaf) {
      const { index } = this._binarySearchValues(node.values, value, true, false)
      node = this.getNode(node.keys[index])
    }
    return node as BPTreeLeafNode<K, V>
  }

  protected insertableRightestNodeByPrimary(value: V): BPTreeLeafNode<K, V> {
    let node = this.getNode(this.rootId)
    while (!node.leaf) {
      const { index } = this._binarySearchValues(node.values, value, true, true)
      node = this.getNode(node.keys[index])
    }
    return node as BPTreeLeafNode<K, V>
  }

  protected insertableRightestEndNodeByPrimary(value: V): BPTreeLeafNode<K, V> | null {
    const node = this.insertableRightestNodeByPrimary(value)
    if (!node.next) {
      return null
    }
    return this.getNode(node.next) as BPTreeLeafNode<K, V>
  }

  protected insertableEndNode(value: V, direction: 1 | -1): BPTreeLeafNode<K, V> | null {
    const insertableNode = this.insertableNode(value)
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
    return this.getNode(guessNode) as BPTreeLeafNode<K, V>
  }

  protected leftestNode(): BPTreeLeafNode<K, V> {
    let node = this.getNode(this.rootId)
    if (node === null) {
      debugger
    }
    while (!node.leaf) {
      const keys = node.keys
      node = this.getNode(keys[0])
    }
    return node as BPTreeLeafNode<K, V>
  }

  protected rightestNode(): BPTreeLeafNode<K, V> {
    let node = this.getNode(this.rootId)
    while (!node.leaf) {
      const keys = node.keys
      node = this.getNode(keys[keys.length - 1])
    }
    return node as BPTreeLeafNode<K, V>
  }

  protected *getPairsGenerator(
    value: V,
    startNode: BPTreeLeafNode<K, V>,
    endNode: BPTreeLeafNode<K, V> | null,
    comparator: (nodeValue: V, value: V) => boolean,
    direction: 1 | -1,
    earlyTerminate: boolean
  ): Generator<[K, V]> {
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

      if (done) break
      if (direction === 1) {
        if (!node.next) {
          done = true
          break
        }
        node = this.getNode(node.next) as BPTreeLeafNode<K, V>
      }
      else {
        if (!node.prev) {
          done = true
          break
        }
        node = this.getNode(node.prev) as BPTreeLeafNode<K, V>
      }
    }
  }

  public init(): void {
    if (this.rootTx !== this) {
      throw new Error('Cannot call init on a nested transaction')
    }
    this._initInternal()
  }

  protected _initInternal(): void {
    if (this.isInitialized) {
      throw new Error('Transaction already initialized')
    }
    if (this.isDestroyed) {
      throw new Error('Transaction already destroyed')
    }
    this.isInitialized = true
    try {
      this._clearCache()
      const head = this._readHead()
      if (head === null) {
        this.order = this.strategy.order
        const root = this._createNode(true, [], [])
        this._writeHead({
          root: root.id,
          order: this.order,
          data: this.strategy.head.data
        })
      }
      else {
        const { root, order } = head
        this.strategy.head = head
        this.order = order
        this._writeHead({
          root: root,
          order: this.order,
          data: this.strategy.head.data
        })
      }
      if (this.order < 3) {
        throw new Error(`The 'order' parameter must be greater than 2. but got a '${this.order}'.`)
      }
    } catch (e) {
      this.isInitialized = false
      throw e
    }
  }

  public exists(key: K, value: V): boolean {
    const node = this.insertableNode(value)
    const { index, found } = this._binarySearchValues(node.values, value)
    if (found) {
      const keys = node.keys[index]
      if (keys.includes(key)) {
        return true
      }
    }
    return false
  }


  public get(key: K): V | undefined {
    let node = this.leftestNode()
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
      node = this.getNode(node.next) as BPTreeLeafNode<K, V>
    }
    return undefined
  }

  public *keysStream(
    condition: BPTreeCondition<V>,
    filterValues?: Set<K>,
    limit?: number,
    order: BPTreeOrder = 'asc'
  ): Generator<K> {
    const stream = this.whereStream(condition, limit, order)
    const intersection = filterValues && filterValues.size > 0 ? filterValues : null
    for (const [key] of stream) {
      if (intersection && !intersection.has(key)) {
        continue
      }
      yield key
    }
  }

  public *whereStream(
    condition: BPTreeCondition<V>,
    limit?: number,
    order: BPTreeOrder = 'asc'
  ): Generator<[K, V]> {
    const driverKey = this.getDriverKey(condition)
    if (!driverKey) return

    const value = condition[driverKey] as V
    let startNode = this.verifierStartNode[driverKey](value) as BPTreeLeafNode<K, V>
    let endNode = this.verifierEndNode[driverKey](value) as BPTreeLeafNode<K, V> | null
    let direction = this.verifierDirection[driverKey]
    const comparator = this.verifierMap[driverKey]
    const earlyTerminate = this.verifierEarlyTerminate[driverKey]

    if (order === 'desc') {
      startNode = endNode ?? this.rightestNode()
      endNode = null
      direction *= -1
    }

    const generator = this.getPairsGenerator(
      value,
      startNode,
      endNode,
      comparator,
      direction as 1 | -1,
      earlyTerminate
    )

    let count = 0
    for (const pair of generator) {
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

  public keys(condition: BPTreeCondition<V>, filterValues?: Set<K>, order: BPTreeOrder = 'asc'): Set<K> {
    const set = new Set<K>()
    for (const key of this.keysStream(condition, filterValues, undefined, order)) {
      set.add(key)
    }
    return set
  }

  public where(condition: BPTreeCondition<V>, order: BPTreeOrder = 'asc'): BPTreePair<K, V> {
    const map = new Map<K, V>()
    for (const [key, value] of this.whereStream(condition, undefined, order)) {
      map.set(key, value)
    }
    return map
  }

  public insert(key: K, value: V): void {
    let before = this.insertableNode(value)
    before = this._insertAtLeaf(before, key, value) as BPTreeLeafNode<K, V>

    if (before.values.length === this.order) {
      let after = this._createNode(
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
      this._updateNode(before)
      this._updateNode(after)
      this._insertInParent(before, after.values[0], after)
    }
  }

  protected _deleteEntry(
    node: BPTreeUnknownNode<K, V>,
    key: BPTreeNodeKey<K>
  ): BPTreeUnknownNode<K, V> {
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
        this._updateNode(node)
      }
    }

    if (this.rootId === node.id && node.keys.length === 1 && !node.leaf) {
      const keys = node.keys as string[]
      this._deleteNode(node)
      const newRoot = this._cloneNode(this.getNode(keys[0]))
      newRoot.parent = null
      this._updateNode(newRoot)
      this._writeHead({
        root: newRoot.id,
        order: this.order,
        data: this.strategy.head.data
      })
      return node
    }
    else if (this.rootId === node.id) {
      this._writeHead({
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
      let parentNode = this.getNode(node.parent) as BPTreeInternalNode<K, V>
      let prevNode: BPTreeInternalNode<K, V> | null = null
      let nextNode: BPTreeInternalNode<K, V> | null = null
      let prevValue: V | null = null
      let postValue: V | null = null

      for (let i = 0, len = parentNode.keys.length; i < len; i++) {
        const nKey = parentNode.keys[i]
        if (nKey === node.id) {
          if (i > 0) {
            prevNode = this.getNode(parentNode.keys[i - 1]) as BPTreeInternalNode<K, V>
            prevValue = parentNode.values[i - 1]
          }
          if (i < parentNode.keys.length - 1) {
            nextNode = this.getNode(parentNode.keys[i + 1]) as BPTreeInternalNode<K, V>
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

      // Now we know we're going to modify something
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
            const n = this._cloneNode(this.getNode(siblingNode.next))
            n.prev = siblingNode.id
            this._updateNode(n)
          }
        }
        siblingNode.values.push(...node.values)

        if (!siblingNode.leaf) {
          const keys = siblingNode.keys
          for (const key of keys) {
            const node = this._cloneNode(this.getNode(key))
            node.parent = siblingNode.id
            this._updateNode(node)
          }
        }

        this._deleteNode(node)
        this._updateNode(siblingNode)
        this._deleteEntry(this.getNode(node.parent!), node.id)
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
            parentNode = this._cloneNode(this.getNode(node.parent!)) as BPTreeInternalNode<K, V>
            const nodeIndex = parentNode.keys.indexOf(node.id)
            if (nodeIndex > 0) {
              parentNode.values[nodeIndex - 1] = pointerKm
              this._updateNode(parentNode)
            }
          }
          else {
            pointerPm = siblingNode.keys.splice(-1)[0] as unknown as K[]
            pointerKm = siblingNode.values.splice(-1)[0]
            node.keys = [pointerPm, ...node.keys]
            node.values = [pointerKm, ...node.values]
            parentNode = this._cloneNode(this.getNode(node.parent!)) as BPTreeInternalNode<K, V>
            const nodeIndex = parentNode.keys.indexOf(node.id)
            if (nodeIndex > 0) {
              parentNode.values[nodeIndex - 1] = pointerKm
              this._updateNode(parentNode)
            }
          }
          this._updateNode(node)
          this._updateNode(siblingNode)
        }
        else {
          let pointerP0
          let pointerK0
          if (!node.leaf) {
            pointerP0 = siblingNode.keys.splice(0, 1)[0]
            pointerK0 = siblingNode.values.splice(0, 1)[0]
            node.keys = [...node.keys, pointerP0]
            node.values = [...node.values, guess!]
            parentNode = this._cloneNode(this.getNode(node.parent!)) as BPTreeInternalNode<K, V>
            const pointerIndex = parentNode.keys.indexOf(siblingNode.id)
            if (pointerIndex > 0) {
              parentNode.values[pointerIndex - 1] = pointerK0
              this._updateNode(parentNode)
            }
          }
          else {
            pointerP0 = siblingNode.keys.splice(0, 1)[0] as unknown as K[]
            pointerK0 = siblingNode.values.splice(0, 1)[0]
            node.keys = [...node.keys, pointerP0]
            node.values = [...node.values, pointerK0]
            parentNode = this._cloneNode(this.getNode(node.parent!)) as BPTreeInternalNode<K, V>
            const pointerIndex = parentNode.keys.indexOf(siblingNode.id)
            if (pointerIndex > 0) {
              parentNode.values[pointerIndex - 1] = siblingNode.values[0]
              this._updateNode(parentNode)
            }
          }
          this._updateNode(node)
          this._updateNode(siblingNode)
        }
        if (!siblingNode.leaf) {
          for (const key of siblingNode.keys) {
            const n = this._cloneNode(this.getNode(key))
            n.parent = siblingNode.id
            this._updateNode(n)
          }
        }
        if (!node.leaf) {
          for (const key of node.keys) {
            const n = this._cloneNode(this.getNode(key))
            n.parent = node.id
            this._updateNode(n)
          }
        }
        if (!parentNode.leaf) {
          for (const key of parentNode.keys) {
            const n = this._cloneNode(this.getNode(key))
            n.parent = parentNode.id
            this._updateNode(n)
          }
        }
      }
    } else {
      this._updateNode(this._cloneNode(node))
    }
    return node
  }

  public delete(key: K, value?: V): void {
    if (value === undefined) {
      value = this.get(key)
    }

    if (value === undefined) {
      return
    }

    let node = this.insertableNodeByPrimary(value)
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
            this._updateNode(node)
            node = this._deleteEntry(node, key) as BPTreeLeafNode<K, V>
            found = true
            break
          }
        }
      }
      if (found) break
      if (node.next) {
        node = this.getNode(node.next) as BPTreeLeafNode<K, V>
        continue
      }
      break
    }
  }

  public getHeadData(): SerializableData {
    const head = this._readHead()
    if (head === null) {
      throw new Error('Head not found')
    }
    return head.data
  }

  public setHeadData(data: SerializableData): void {
    const head = this._readHead()
    if (head === null) {
      throw new Error('Head not found')
    }
    this._writeHead({
      root: head.root,
      order: head.order,
      data,
    })
  }

  public commit(label?: string): TransactionResult<string, BPTreeNode<K, V>> {
    let result = this.mvcc.commit(label)
    if (result.success) {
      const isRootTx = this.rootTx === this
      if (!isRootTx) {
        result = this.rootTx.commit(label)
        if (result.success) {
          this.rootTx.rootId = this.rootId
        }
      }
    }
    return result
  }

  public rollback(): TransactionResult<string, BPTreeNode<K, V>> {
    return this.mvcc.rollback()
  }
}
