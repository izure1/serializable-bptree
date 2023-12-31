import type { Json } from './utils/types'
import { BinarySearch } from './utils/BinarySearch'
import { ValueComparator } from './ValueComparator'
import { SerializeStrategy, SerializeStrategyHead } from './SerializeStrategy'

type BPTreeNodeKey<K> = number|K
type BPTreeCondition<V> = { gt?: V, lt?: V }|{ equal: V }|{ notEqual: V}
type BPTreePair<K, V> = { key: K, value: V }

export type BPTreeUnknownNode<K, V> = BPTreeInternalNode<K, V>|BPTreeLeafNode<K, V>

export interface BPTreeNode<K, V> {
  id: number
  keys: number[]|K[][],
  values: V[],
  leaf: boolean
  parent: number
  next: number
}

export interface BPTreeInternalNode<K, V> extends BPTreeNode<K, V> {
  leaf: false
  keys: number[]
}

export interface BPTreeLeafNode<K, V> extends BPTreeNode<K, V> {
  leaf: true
  keys: K[][]
}

export class BPTree<K, V> {
  protected readonly strategy: SerializeStrategy<K, V>
  protected readonly comparator: ValueComparator<V>
  protected readonly search: BinarySearch<V>
  protected readonly order: number
  protected readonly nodes: Map<number, BPTreeUnknownNode<K, V>>
  protected data: Record<string, Json>
  protected root: BPTreeUnknownNode<K, V>
  private readonly _creates: Map<number, BPTreeUnknownNode<K, V>>
  private readonly _updates: Map<number, BPTreeUnknownNode<K, V>>
  private _updatedHead: SerializeStrategyHead|null

  private _createNodeId(): number {
    const id = this.strategy.id()
    if (id === 0) {
      throw new Error(`The node's id should never be 0.`)
    }
    return id
  }

  private _createNode(keys: number[]|K[][], values: V[], leaf = false, parent = 0, next = 0): BPTreeUnknownNode<K, V> {
    const id = this._createNodeId()
    const node = {
      id,
      keys,
      values,
      leaf,
      parent,
      next
    } as BPTreeUnknownNode<K, V>
    this.nodes.set(id, node)
    return node
  }

  /**
   * @param strategy An instance of a strategy that manages the read/write state of a node.
   * @param comparator An instance of a comparator that compares the size of values.
   */
  constructor(strategy: SerializeStrategy<K, V>, comparator: ValueComparator<V>) {
    const head = strategy.readHead()
    this._creates = new Map()
    this._updates = new Map()
    this._updatedHead = null
    this.nodes = new Map()
    this.search = new BinarySearch(comparator)
    this.strategy = strategy
    this.comparator = comparator
    // first created
    if (head === null) {
      this.order = strategy.order
      this.data = {}
      this.root = this._createNode([], [], true)
      this._setHeadUpdate(this._headState)
      this._setCreates(this.root)
      this._emitHeadUpdates()
      this._emitCreates()
    }
    // loaded
    else {
      const { root, order, data } = head
      this.order = order
      this.data = data ?? {}
      this.root = this.getNode(root)
    }
    if (this.order < 3) {
      throw new Error(`The 'order' parameter must be greater than 2. but got a '${this.order}'.`)
    }
  }

  private get _headState(): SerializeStrategyHead {
    const root = this.root.id
    const order = this.order
    const data = this.data
    return {
      root,
      order,
      data,
    }
  }

  private _setHeadUpdate(head: SerializeStrategyHead): void {
    this._updatedHead = head
  }

  private _setCreates(node: BPTreeUnknownNode<K, V>): void {
    this._creates.set(node.id, node)
  }

  private _setUpdates(node: BPTreeUnknownNode<K, V>): void {
    this._updates.set(node.id, node)
  }

  private _emitHeadUpdates(): void {
    if (this._updatedHead !== null) {
      this.strategy.writeHead(this._updatedHead)
    }
    this._updatedHead = null
  }

  private _emitCreates(): void {
    for (const node of this._creates.values()) {
      this.strategy.write(node.id, node)
    }
    this._creates.clear()
  }

  private _emitUpdates(): void {
    for (const node of this._updates.values()) {
      this.strategy.write(node.id, node)
    }
    this._updates.clear()
  }

  protected getNode(id: number): BPTreeUnknownNode<K, V> {
    if (!this.nodes.has(id)) {
      this.nodes.set(id, this.strategy.read(id) as BPTreeUnknownNode<K, V>)
    }
    return this.nodes.get(id)!
  }

  protected leftestNode(): BPTreeLeafNode<K, V> {
    let node = this.root
    while (!node.leaf) {
      const keys = node.keys
      node = this.getNode(keys[0])
    }
    return node
  }

  private _insertableNode(value: V): BPTreeLeafNode<K, V> {
    let node = this.root
    while (!node.leaf) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const k = node.keys
        if (this.comparator.isSame(value, nValue)) {
          node = this.getNode(k[i+1])
          break
        }
        else if (this.comparator.isLower(value, nValue)) {
          node = this.getNode(k[i])
          break
        }
        else if (i+1 === node.values.length) {
          node = this.getNode(k[i+1])
          break
        }
      }
    }
    return node
  }

  /**
   * It returns whether there is a value in the tree.
   * @param key The key value to search for.
   * @param value The value to search for.
   */
  exists(key: K, value: V): boolean {
    const node = this._insertableNode(value)
    for (let i = 0, len = node.values.length; i < len; i++) {
      const nValue = node.values[i]
      if (this.comparator.isSame(value, nValue)) {
        const keys = node.keys[i]
        return keys.includes(key)
      }
    }
    return false
  }

  private _insertAtLeaf(node: BPTreeLeafNode<K, V>, key: K, value: V): void {
    if (node.values.length) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        if (this.comparator.isSame(value, nValue)) {
          const keys = node.keys[i]
          keys.push(key)
          this._setUpdates(node)
          break
        }
        else if (this.comparator.isLower(value, nValue)) {
          node.values.splice(i, 0, value)
          node.keys.splice(i, 0, [key])
          this._setUpdates(node)
          break
        }
        else if (i+1 === node.values.length) {
          node.values.push(value)
          node.keys.push([key])
          this._setUpdates(node)
          break
        }
      }
    }
    else {
      node.values = [value]
      node.keys = [[key]]
      this._setUpdates(node)
    }
  }

  private _insertInParent(node: BPTreeUnknownNode<K, V>, value: V, pointer: BPTreeUnknownNode<K, V>): void {
    if (this.root === node) {
      const root = this._createNode([node.id, pointer.id], [value])
      this.root = root
      node.parent = root.id
      pointer.parent = root.id
      this._setHeadUpdate(this._headState)
      this._setCreates(root)
      this._setUpdates(node)
      this._setUpdates(pointer)
      return
    }
    const parentNode = this.getNode(node.parent) as BPTreeInternalNode<K, V>
    for (let i = 0, len = parentNode.keys.length; i < len; i++) {
      const nKeys = parentNode.keys[i]
      if (nKeys === node.id) {
        parentNode.values.splice(i, 0, value)
        parentNode.keys.splice(i+1, 0, pointer.id)
        this._setUpdates(parentNode)

        if (parentNode.keys.length > this.order) {
          const parentPointer = this._createNode([], []) as BPTreeInternalNode<K, V>
          parentPointer.parent = parentNode.parent
          const mid = Math.ceil(this.order/2)-1
          parentPointer.values = parentNode.values.slice(mid+1)
          parentPointer.keys = parentNode.keys.slice(mid+1)
          const midValue = parentNode.values[mid]
          if (mid === 0) {
            parentNode.values = parentNode.values.slice(0, mid+1)
          }
          else {
            parentNode.values = parentNode.values.slice(0, mid)
          }
          parentNode.keys = parentNode.keys.slice(0, mid+1)
          for (const k of parentNode.keys) {
            const node = this.getNode(k)
            node.parent = parentNode.id
            this._setUpdates(node)
          }
          for (const k of parentPointer.keys) {
            const node = this.getNode(k)
            node.parent = parentPointer.id
            this._setUpdates(node)
          }

          this._insertInParent(parentNode, midValue, parentPointer)
          this._setCreates(parentPointer)
          this._setUpdates(parentNode)
        }
      }
    }
  }

  private _equalCondition(condition: unknown): condition is { equal: V } {
    return Object.prototype.hasOwnProperty.call(condition, 'equal')
  }

  private _notEqualCondition(condition: unknown): condition is { notEqual: V } {
    return Object.prototype.hasOwnProperty.call(condition, 'notEqual')
  }

  private _onlyGtCondition(condition: unknown): condition is { gt: V, notEqual?: V } {
    return (
      Object.prototype.hasOwnProperty.call(condition, 'gt') &&
      !Object.prototype.hasOwnProperty.call(condition, 'lt')
    )
  }

  private _onlyLtCondition(condition: unknown): condition is { lt: V, notEqual?: V } {
    return (
      Object.prototype.hasOwnProperty.call(condition, 'lt') &&
      !Object.prototype.hasOwnProperty.call(condition, 'gt')
    )
  }

  private _rangeCondition(condition: unknown): condition is { gt: V, lt: V, notEqual?: V } {
    return (
      Object.prototype.hasOwnProperty.call(condition, 'gt') &&
      Object.prototype.hasOwnProperty.call(condition, 'lt')
    )
  }

  private _getKeysFromValue(value: V): Set<K> {
    const keys = new Set<K>()
    const node = this._insertableNode(value)
    const [start, end] = this.search.range(node.values, value)
    if (start === -1) {
      return keys
    }
    for (let i = start; i < end; i++) {
      const pairKeys = node.keys[i]
      for (const key of pairKeys) {
        keys.add(key)
      }
    }
    return keys
  }

  private _getKeysFromNEValue(value: V): Set<K> {
    const keys = new Set<K>()
    let node = this.leftestNode()
    let done = false
    while (!done) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const pairKeys = node.keys[i]
        if (this.comparator.isSame(nValue, value) === false) {
          for (const key of pairKeys) {
            keys.add(key)
          }
        }
      }
      if (!node.next) {
        done = true
        break
      }
      node = this.getNode(node.next) as BPTreeLeafNode<K, V>
    }
    return keys
  }

  private _getKeysFromRange(gt: V, lt: V): Set<K> {
    const keys = new Set<K>()
    let node = this._insertableNode(gt)
    let done = false
    let found = false
    while (!done) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const localKeys = node.keys[i]
        if (
          this.comparator.isHigher(nValue, gt) &&
          this.comparator.isLower(nValue, lt)
        ) {
          found = true
          for (const key of localKeys) {
            keys.add(key)
          }
        }
        else if (found) {
          done = true
          break
        }
      }
      if (!node.next) {
        done = true
        break
      }
      node = this.getNode(node.next) as BPTreeLeafNode<K, V>
    }
    return keys
  }

  private _getKeysFromGt(gt: V): Set<K> {
    const keys = new Set<K>()
    let node = this._insertableNode(gt)
    let done = false
    let found = false
    while (!done) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const localKeys = node.keys[i]
        if (this.comparator.isHigher(nValue, gt)) {
          found = true
          for (const key of localKeys) {
            keys.add(key)
          }
        }
        else if (found) {
          done = true
          break
        }
      }
      if (!node.next) {
        done = true
        break
      }
      node = this.getNode(node.next) as BPTreeLeafNode<K, V>
    }
    return keys
  }

  private _getKeysFromLt(lt: V): Set<K> {
    const keys = new Set<K>()
    let node = this.leftestNode()
    let done = false
    let found = false
    while (!done) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const localKeys = node.keys[i]
        if (this.comparator.isLower(nValue, lt)) {
          found = true
          for (const key of localKeys) {
            keys.add(key)
          }
        }
        else if (found) {
          done = true
          break
        }
      }
      if (!node.next) {
        done = true
        break
      }
      node = this.getNode(node.next) as BPTreeLeafNode<K, V>
    }
    return keys
  }

  private _getPairsFromValue(value: V): BPTreePair<K, V>[] {
    const node = this._insertableNode(value)
    const [start, end] = this.search.range(node.values, value)
    if (start === -1) {
      return []
    }
    const pairs = []
    for (let i = start; i < end; i++) {
      const keys = node.keys[i]
      for (const key of keys) {
        pairs.push({ key, value })
      }
    }
    return pairs
  }

  private _getPairsFromNEValue(value: V): BPTreePair<K, V>[] {
    const pairs = []
    let node = this.leftestNode()
    let done = false
    while (!done) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const keys = node.keys[i]
        if (this.comparator.isSame(nValue, value) === false) {
          for (const key of keys) {
            pairs.push({ key, value: nValue })
          }
        }
      }
      if (!node.next) {
        done = true
        break
      }
      node = this.getNode(node.next) as BPTreeLeafNode<K, V>
    }
    return pairs
  }

  private _getPairsFromRange(gt: V, lt: V): BPTreePair<K, V>[] {
    const pairs = []
    let node = this._insertableNode(gt)
    let done = false
    let found = false
    while (!done) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const keys = node.keys[i]
        if (
          this.comparator.isHigher(nValue, gt) &&
          this.comparator.isLower(nValue, lt)
        ) {
          found = true
          for (const key of keys) {
            pairs.push({ key, value: nValue })
          }
        }
        else if (found) {
          done = true
          break
        }
      }
      if (!node.next) {
        done = true
        break
      }
      node = this.getNode(node.next) as BPTreeLeafNode<K, V>
    }
    return pairs
  }

  private _getPairsFromGt(gt: V): BPTreePair<K, V>[] {
    const pairs = []
    let node = this._insertableNode(gt)
    let done = false
    let found = false
    while (!done) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const keys = node.keys[i]
        if (this.comparator.isHigher(nValue, gt)) {
          found = true
          for (const key of keys) {
            pairs.push({ key, value: nValue })
          }
        }
        else if (found) {
          done = true
          break
        }
      }
      if (!node.next) {
        done = true
        break
      }
      node = this.getNode(node.next) as BPTreeLeafNode<K, V>
    }
    return pairs
  }

  private _getPairsFromLt(lt: V): BPTreePair<K, V>[] {
    const pairs = []
    let node = this.leftestNode()
    let done = false
    let found = false
    while (!done) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const keys = node.keys[i]
        if (this.comparator.isLower(nValue, lt)) {
          found = true
          for (const key of keys) {
            pairs.push({ key, value: nValue })
          }
        }
        else if (found) {
          done = true
          break
        }
      }
      if (!node.next) {
        done = true
        break
      }
      node = this.getNode(node.next) as BPTreeLeafNode<K, V>
    }
    return pairs
  }

  /**
   * It searches for a key within the tree. The result is returned as an array sorted in ascending order based on the value.  
   * The result is key set instance, and you can use the `gt`, `lt`, `equal`, `notEqual` condition statements.
   * This method operates much faster than first searching with `where` and then retrieving only the key list.
   * @param condition You can use the `gt`, `lt`, `equal`, `notEqual` condition statements.
   */
  keys(condition: BPTreeCondition<V>): Set<K> {
    if (this._equalCondition(condition)) {
      return this._getKeysFromValue(condition.equal)
    }
    else if (this._notEqualCondition(condition)) {
      return this._getKeysFromNEValue(condition.notEqual)
    }
    else if (this._rangeCondition(condition)) {
      const { gt, lt } = condition
      return this._getKeysFromRange(gt, lt)
    }
    else if (this._onlyGtCondition(condition)) {
      return this._getKeysFromGt(condition.gt)
    }
    else if (this._onlyLtCondition(condition)) {
      return this._getKeysFromLt(condition.lt)
    }
    else {
      throw new Error(`The 'condition' parameter is invalid.`)
    }
  }

  /**
   * It searches for a value within the tree. The result is returned as an array sorted in ascending order based on the value.  
   * The result includes the key and value attributes, and you can use the `gt`, `lt`, `equal`, `notEqual` condition statements.
   * @param condition You can use the `gt`, `lt`, `equal`, `notEqual` condition statements.
   */
  where(condition: BPTreeCondition<V>): BPTreePair<K, V>[] {
    if (this._equalCondition(condition)) {
      return this._getPairsFromValue(condition.equal)
    }
    else if (this._notEqualCondition(condition)) {
      return this._getPairsFromNEValue(condition.notEqual)
    }
    else if (this._rangeCondition(condition)) {
      const { gt, lt } = condition
      return this._getPairsFromRange(gt, lt)
    }
    else if (this._onlyGtCondition(condition)) {
      return this._getPairsFromGt(condition.gt)
    }
    else if (this._onlyLtCondition(condition)) {
      return this._getPairsFromLt(condition.lt)
    }
    else {
      throw new Error(`The 'condition' parameter is invalid.`)
    }
  }

  /**
   * You enter the key and value as a pair. You can later search for the pair by value.
   * This data is stored in the tree, sorted in ascending order of value.
   * @param key The key of the pair.
   * @param value The value of the pair.
   */
  insert(key: K, value: V): void {
    const before = this._insertableNode(value)
    this._insertAtLeaf(before, key, value)

    if (before.values.length === this.order) {
      const after = this._createNode(
        [],
        [],
        true,
        before.parent,
        before.next
      ) as BPTreeLeafNode<K, V>
      const mid = Math.ceil(this.order/2)-1
      after.values = before.values.slice(mid+1)
      after.keys = before.keys.slice(mid+1)
      before.values = before.values.slice(0, mid+1)
      before.keys = before.keys.slice(0, mid+1)
      before.next = after.id
      this._insertInParent(before, after.values[0], after)
      this._setCreates(after)
      this._setUpdates(before)
    }

    this._emitHeadUpdates()
    this._emitCreates()
    this._emitUpdates()
  }

  /**
   * Deletes the pair that matches the key and value.
   * @param key The key of the pair.
   * @param value The value of the pair.
   */
  delete(key: K, value: V): void {
    const node = this._insertableNode(value)
    let i = node.values.length
    while (i--) {
      const nValue = node.values[i]
      if (this.comparator.isSame(value, nValue)) {
        const keys = node.keys[i]
        if (keys.includes(key)) {
          if (keys.length > 1) {
            keys.splice(keys.indexOf(key), 1)
            this._setUpdates(node)
          }
          else if (node === this.root) {
            node.values.splice(i, 1)
            node.keys.splice(i, 1)
            this._setUpdates(node)
          }
          else {
            keys.splice(keys.indexOf(key), 1)
            node.keys.splice(i, 1)
            node.values.splice(node.values.indexOf(value), 1)
            this._deleteEntry(node, key, value)
            this._setUpdates(node)
          }
        }
      }
    }
    this._emitHeadUpdates()
    this._emitCreates()
    this._emitUpdates()
  }

  private _deleteEntry(node: BPTreeUnknownNode<K, V>, key: BPTreeNodeKey<K>, value: V): void {
    if (!node.leaf) {
      for (let i = 0, len = node.keys.length; i < len; i++) {
        const nKey = node.keys[i]
        if (nKey === key) {
          node.keys.splice(i, 1)
          this._setUpdates(node)
          break
        }
      }
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        if (this.comparator.isSame(value, nValue)) {
          node.values.splice(i, 1)
          this._setUpdates(node)
          break
        }
      }
    }

    if (this.root === node && node.keys.length === 1) {
      const keys = node.keys as number[]
      this.root = this.getNode(keys[0])
      this.root.parent = 0
      this._setHeadUpdate(this._headState)
      this._setUpdates(this.root)
      return
    }
    else if (this.root === node) {
      return
    }
    else if (
      (node.keys.length < Math.ceil(this.order/2) && !node.leaf) ||
      (node.values.length < Math.ceil((this.order-1)/2) && node.leaf)
    ) {
      let isPredecessor = false
      let parentNode = this.getNode(node.parent) as BPTreeInternalNode<K, V>
      let prevNode: BPTreeInternalNode<K, V>|null = null
      let nextNode: BPTreeInternalNode<K, V>|null = null
      let prevK: V|null = null
      let postK: V|null = null

      for (let i = 0, len = parentNode.keys.length; i < len; i++) {
        const nKey = parentNode.keys[i]
        if (nKey === node.id) {
          if (i > 0) {
            prevNode = this.getNode(parentNode.keys[i-1]) as BPTreeInternalNode<K, V>
            prevK = parentNode.values[i-1]
          }
          if (i < parentNode.keys.length-1) {
            nextNode = this.getNode(parentNode.keys[i+1]) as BPTreeInternalNode<K, V>
            postK = parentNode.values[i]
          }
        }
      }

      let pointer: BPTreeUnknownNode<K, V>
      let guess: V|null
      if (prevNode === null) {
        pointer = nextNode!
        guess = postK
      }
      else if (nextNode === null) {
        isPredecessor = true
        pointer = prevNode
        guess = prevK
      }
      else {
        if (node.values.length + nextNode.values.length < this.order) {
          pointer = nextNode
          guess = postK
        }
        else {
          isPredecessor = true
          pointer = prevNode
          guess = prevK
        }
      }
      if (node.values.length + pointer!.values.length < this.order) {
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
        }
        pointer.values.push(...node.values)
        
        if (!pointer.leaf) {
          const keys = pointer.keys
          for (const key of keys) {
            const node = this.getNode(key)
            node.parent = pointer.id
            this._setUpdates(node)
          }
        }
        
        this._deleteEntry(this.getNode(node.parent), node.id, guess!)
        this._setUpdates(pointer)
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
            parentNode = this.getNode(node.parent) as BPTreeInternalNode<K, V>
            for (let i = 0, len = parentNode.values.length; i < len; i++) {
              const nValue = parentNode.values[i]
              if (this.comparator.isSame(guess!, nValue)) {
                parentNode.values[i] = pointerKm
                this._setUpdates(parentNode)
                break
              }
            }
          }
          else {
            pointerPm = pointer.keys.splice(-1)[0] as unknown as K[]
            pointerKm = pointer.values.splice(-1)[0]
            node.keys = [pointerPm, ...node.keys]
            node.values = [pointerKm, ...node.values]
            parentNode = this.getNode(node.parent) as BPTreeInternalNode<K, V>
            for (let i = 0, len = parentNode.values.length; i < len; i++) {
              const nValue = parentNode.values[i]
              if (this.comparator.isSame(guess!, nValue)) {
                parentNode.values[i] = pointerKm
                this._setUpdates(parentNode)
                break
              }
            }
          }
          this._setUpdates(node)
          this._setUpdates(pointer)
        }
        else {
          let pointerP0
          let pointerK0
          if (!node.leaf) {
            pointerP0 = pointer.keys.splice(0, 1)[0]
            pointerK0 = pointer.values.splice(0, 1)[0]
            node.keys = [...node.keys, pointerP0]
            node.values = [...node.values, guess!]
            parentNode = this.getNode(node.parent) as BPTreeInternalNode<K, V>
            for (let i = 0, len = parentNode.values.length; i < len; i++) {
              const nValue = parentNode.values[i]
              if (this.comparator.isSame(guess!, nValue)) {
                parentNode.values[i] = pointerK0
                this._setUpdates(parentNode)
                break
              }
            }
          }
          else {
            pointerP0 = pointer.keys.splice(0, 1)[0] as unknown as K[]
            pointerK0 = pointer.values.splice(0, 1)[0]
            node.keys = [...node.keys, pointerP0]
            node.values = [...node.values, pointerK0]
            parentNode = this.getNode(node.parent) as BPTreeInternalNode<K, V>
            for (let i = 0, len = parentNode.values.length; i < len; i++) {
              const nValue = parentNode.values[i]
              if (this.comparator.isSame(guess!, nValue)) {
                parentNode.values[i] = pointer.values[0]
                this._setUpdates(parentNode)
                break
              }
            }
          }
          this._setUpdates(node)
          this._setUpdates(pointer)
        }
        if (!pointer.leaf) {
          for (const key of pointer.keys) {
            const n = this.getNode(key)
            n.parent = pointer.id
            this._setUpdates(n)
          }
        }
        if (!node.leaf) {
          for (const key of node.keys) {
            const n = this.getNode(key)
            n.parent = node.id
            this._setUpdates(n)
          }
        }
        if (!parentNode.leaf) {
          for (const key of parentNode.keys) {
            const n = this.getNode(key)
            n.parent = parentNode.id
            this._setUpdates(n)
          }
        }
      }
    }
  }

  /**
   * Returns the user-defined data stored in the head of the tree.
   * This value can be set using the `setHeadData` method. If no data has been previously inserted, the default value is returned, and the default value is `{}`.
   * @returns User-defined data stored in the head of the tree.
   */
  getHeadData(): Record<string, Json> {
    return this.data
  }

  /**
   * Inserts user-defined data into the head of the tree.
   * This feature is useful when you need to store separate, non-volatile information in the tree.
   * For example, you can store information such as the last update time and the number of insertions.
   * @param data User-defined data to be stored in the head of the tree.
   */
  setHeadData(data: Record<string, Json>): void {
    this.data = data
    this._updatedHead = this._headState
    this._emitHeadUpdates()
  }
}
