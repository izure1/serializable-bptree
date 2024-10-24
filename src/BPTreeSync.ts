import {
  BPTree,
  BPTreeCondition,
  BPTreeLeafNode,
  BPTreePair,
  BPTreeNodeKey,
  BPTreeUnknownNode,
  BPTreeInternalNode,
} from './base/BPTree'
import { SerializeStrategySync } from './SerializeStrategySync'
import { ValueComparator } from './base/ValueComparator'
import { SerializableData } from './base/SerializeStrategy'

export class BPTreeSync<K, V> extends BPTree<K, V> {
  declare protected readonly strategy: SerializeStrategySync<K, V>

  constructor(strategy: SerializeStrategySync<K, V>, comparator: ValueComparator<V>) {
    super(strategy, comparator)
  }

  protected _getPairsRightToLeft(
    value: V,
    startNode: BPTreeLeafNode<K, V>,
    fullScan: boolean,
    comparator: (nodeValue: V, value: V) => boolean
  ): BPTreePair<K, V>[] {
    const pairs = []
    let node = startNode
    let done = false
    let found = false
    while (!done) {
      let i = node.values.length
      while (i--) {
        const nValue = node.values[i]
        const keys = node.keys[i]
        if (comparator(nValue, value)) {
          found = true
          let j = keys.length
          while (j--) {
            pairs.push({ key: keys[j], value: nValue })
          }
        }
        else if (found && !fullScan) {
          done = true
          break
        }
      }
      if (!node.prev) {
        done = true
        break
      }
      node = this.getNode(node.prev) as BPTreeLeafNode<K, V>
    }
    return pairs.reverse()
  }

  protected _getPairsLeftToRight(
    value: V,
    startNode: BPTreeLeafNode<K, V>,
    fullScan: boolean,
    comparator: (nodeValue: V, value: V) => boolean
  ): BPTreePair<K, V>[] {
    const pairs = []
    let node = startNode
    let done = false
    let found = false
    while (!done) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const keys = node.keys[i]
        if (comparator(nValue, value)) {
          found = true
          for (const key of keys) {
            pairs.push({ key, value: nValue })
          }
        }
        else if (found && !fullScan) {
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

  protected getPairs(
    value: V,
    startNode: BPTreeLeafNode<K, V>,
    fullScan: boolean,
    comparator: (nodeValue: V, value: V) => boolean,
    direction: 1|-1
  ): BPTreePair<K, V>[] {
    switch (direction) {
      case -1:  return this._getPairsRightToLeft(value, startNode, fullScan, comparator)
      case +1:  return this._getPairsLeftToRight(value, startNode, fullScan, comparator)
      default:  throw new Error(`Direction must be -1 or 1. but got a ${direction}`)
    }
  }

  protected _createNodeId(isLeaf: boolean): string {
    const id = this.strategy.id(isLeaf)
    if (id === null) {
      throw new Error(`The node's id should never be null.`)
    }
    return id
  }

  protected _createNode(
    isLeaf: boolean,
    keys: string[]|K[][],
    values: V[],
    leaf = false,
    parent: string|null = null,
    next: string|null = null,
    prev: string|null = null
  ): BPTreeUnknownNode<K, V> {
    const id = this._createNodeId(isLeaf)
    const node = {
      id,
      keys,
      values,
      leaf,
      parent,
      next,
      prev,
    } as BPTreeUnknownNode<K, V>
    this.nodes.set(id, node)
    return node
  }

  protected _deleteEntry(
    node: BPTreeUnknownNode<K, V>,
    key: BPTreeNodeKey<K>,
    value: V
  ): void {
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

    if (this.root === node && node.keys.length === 1) {
      const keys = node.keys as string[]
      this.bufferForNodeDelete(this.root)
      this.root = this.getNode(keys[0])
      this.root.parent = null
      this.strategy.head.root = this.root.id
      this.bufferForNodeUpdate(this.root)
      return
    }
    else if (this.root === node) {
      return
    }
    else if (
      (node.keys.length < Math.ceil(this.order/2) && !node.leaf) ||
      (node.values.length < Math.ceil((this.order-1)/2) && node.leaf)
    ) {
      if (node.parent === null) {
        return
      }
      let isPredecessor = false
      let parentNode = this.getNode(node.parent) as BPTreeInternalNode<K, V>
      let prevNode: BPTreeInternalNode<K, V>|null = null
      let nextNode: BPTreeInternalNode<K, V>|null = null
      let prevValue: V|null = null
      let postValue: V|null = null

      for (let i = 0, len = parentNode.keys.length; i < len; i++) {
        const nKey = parentNode.keys[i]
        if (nKey === node.id) {
          if (i > 0) {
            prevNode = this.getNode(parentNode.keys[i-1]) as BPTreeInternalNode<K, V>
            prevValue = parentNode.values[i-1]
          }
          if (i < parentNode.keys.length-1) {
            nextNode = this.getNode(parentNode.keys[i+1]) as BPTreeInternalNode<K, V>
            postValue = parentNode.values[i]
          }
        }
      }

      let pointer: BPTreeUnknownNode<K, V>
      let guess: V|null
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
          pointer.prev = node.id
          if (pointer.next) {
            const n = this.getNode(node.next!)
            n.prev = pointer.id
            this.bufferForNodeUpdate(n)
          }
          if (pointer.prev) {
            const n = this.getNode(node.id)
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
            const node = this.getNode(key)
            node.parent = pointer.id
            this.bufferForNodeUpdate(node)
          }
        }
        
        this._deleteEntry(this.getNode(node.parent!), node.id, guess!)
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
            parentNode = this.getNode(node.parent!) as BPTreeInternalNode<K, V>
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
            parentNode = this.getNode(node.parent!) as BPTreeInternalNode<K, V>
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
            parentNode = this.getNode(node.parent!) as BPTreeInternalNode<K, V>
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
            parentNode = this.getNode(node.parent!) as BPTreeInternalNode<K, V>
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
            const n = this.getNode(key)
            n.parent = pointer.id
            this.bufferForNodeUpdate(n)
          }
        }
        if (!node.leaf) {
          for (const key of node.keys) {
            const n = this.getNode(key)
            n.parent = node.id
            this.bufferForNodeUpdate(n)
          }
        }
        if (!parentNode.leaf) {
          for (const key of parentNode.keys) {
            const n = this.getNode(key)
            n.parent = parentNode.id
            this.bufferForNodeUpdate(n)
          }
        }
      }
    }
  }

  protected _insertInParent(
    node: BPTreeUnknownNode<K, V>,
    value: V,
    pointer: BPTreeUnknownNode<K, V>
  ): void {
    if (this.root === node) {
      const root = this._createNode(false, [node.id, pointer.id], [value])
      this.root = root
      this.strategy.head.root = root.id
      node.parent = root.id
      pointer.parent = root.id
      this.bufferForNodeCreate(root)
      this.bufferForNodeUpdate(node)
      this.bufferForNodeUpdate(pointer)
      return
    }
    const parentNode = this.getNode(node.parent!) as BPTreeInternalNode<K, V>
    for (let i = 0, len = parentNode.keys.length; i < len; i++) {
      const nKeys = parentNode.keys[i]
      if (nKeys === node.id) {
        parentNode.values.splice(i, 0, value)
        parentNode.keys.splice(i+1, 0, pointer.id)
        this.bufferForNodeUpdate(parentNode)

        if (parentNode.keys.length > this.order) {
          const parentPointer = this._createNode(false, [], []) as BPTreeInternalNode<K, V>
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
            this.bufferForNodeUpdate(node)
          }
          for (const k of parentPointer.keys) {
            const node = this.getNode(k)
            node.parent = parentPointer.id
            this.bufferForNodeUpdate(node)
          }

          this._insertInParent(parentNode, midValue, parentPointer)
          this.bufferForNodeCreate(parentPointer)
          this.bufferForNodeUpdate(parentNode)
        }
      }
    }
  }

  init(): void {
    const head = this.strategy.readHead()
    // first created
    if (head === null) {
      this.order = this.strategy.order
      this.root = this._createNode(true, [], [], true)
      this.strategy.head.root = this.root.id
      this.bufferForNodeCreate(this.root)
      this.commitHeadBuffer()
      this.commitNodeCreateBuffer()
    }
    // loaded
    else {
      const { root, order } = head
      this.strategy.head = head
      this.order = order
      this.root = this.getNode(root!)
    }
    if (this.order < 3) {
      throw new Error(`The 'order' parameter must be greater than 2. but got a '${this.order}'.`)
    }
  }

  protected getNode(id: string): BPTreeUnknownNode<K, V> {
    if (!this.nodes.has(id)) {
      this.nodes.set(id, this.strategy.read(id) as BPTreeUnknownNode<K, V>)
    }
    return this.nodes.get(id)!
  }

  protected insertableNode(value: V): BPTreeLeafNode<K, V> {
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

  protected leftestNode(): BPTreeLeafNode<K, V> {
    let node = this.root
    while (!node.leaf) {
      const keys = node.keys
      node = this.getNode(keys[0])
    }
    return node
  }

  protected commitHeadBuffer(): void {
    this.strategy.writeHead(this.strategy.head)
  }

  protected commitNodeCreateBuffer(): void {
    for (const node of this._nodeCreateBuffer.values()) {
      this.strategy.write(node.id, node)
    }
    this._nodeCreateBuffer.clear()
  }

  protected commitNodeUpdateBuffer(): void {
    for (const node of this._nodeUpdateBuffer.values()) {
      this.strategy.write(node.id, node)
    }
    this._nodeUpdateBuffer.clear()
  }

  protected commitNodeDeleteBuffer(): void {
    for (const node of this._nodeDeleteBuffer.values()) {
      this.strategy.delete(node.id)
    }
    this._nodeDeleteBuffer.clear()
  }

  public keys(condition: BPTreeCondition<V>, filterValues?: Set<K>): Set<K> {
    for (const k in condition) {
      const key = k as keyof BPTreeCondition<V>
      const value = condition[key] as V
      const startNode   = this.verifierStartNode[key](value) as BPTreeLeafNode<K, V>
      const direction   = this.verifierDirection[key]
      const fullScan    = this.verifierFullScan[key]
      const comparator  = this.verifierMap[key]
      const pairs       = this.getPairs(value, startNode, fullScan, comparator, direction)
      if (!filterValues) {
        filterValues = new Set(pairs.map((pair) => pair.key))
      }
      else {
        const intersections = new Set<K>()
        for (const key of filterValues) {
          const has = pairs.some((pair) => pair.key === key)
          if (has) {
            intersections.add(key)
          }
        }
        filterValues = intersections
      }
    }
    return filterValues ?? new Set([])
  }

  public where(condition: BPTreeCondition<V>): BPTreePair<K, V>[] {
    let result: BPTreePair<K, V>[]|null = null
    for (const k in condition) {
      const key = k as keyof BPTreeCondition<V>
      const value = condition[key] as V
      const startNode   = this.verifierStartNode[key](value) as BPTreeLeafNode<K, V>
      const direction   = this.verifierDirection[key]
      const fullScan    = this.verifierFullScan[key]
      const comparator  = this.verifierMap[key]
      const pairs       = this.getPairs(value, startNode, fullScan, comparator, direction)
      if (result === null) {
        result = pairs
      }
      else {
        const intersection = []
        for (const pair of pairs) {
          if (result.find((p) => p.key === pair.key)) {
            intersection.push(pair)
          }
        }
        result = intersection
      }
    }
    return result ?? []
  }

  public insert(key: K, value: V): void {
    const before = this.insertableNode(value)
    this._insertAtLeaf(before, key, value)

    if (before.values.length === this.order) {
      const after = this._createNode(
        true,
        [],
        [],
        true,
        before.parent,
        before.next,
        before.id,
      ) as BPTreeLeafNode<K, V>
      const mid = Math.ceil(this.order/2)-1
      const beforeNext = before.next
      after.values = before.values.slice(mid+1)
      after.keys = before.keys.slice(mid+1)
      before.values = before.values.slice(0, mid+1)
      before.keys = before.keys.slice(0, mid+1)
      before.next = after.id
      if (beforeNext) {
        const node = this.getNode(beforeNext)
        node.prev = after.id
        this.bufferForNodeUpdate(node)
      }
      this._insertInParent(before, after.values[0], after)
      this.bufferForNodeCreate(after)
      this.bufferForNodeUpdate(before)
    }

    this.commitHeadBuffer()
    this.commitNodeCreateBuffer()
    this.commitNodeUpdateBuffer()
  }

  public delete(key: K, value: V): void {
    const node = this.insertableNode(value)
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
          else if (node === this.root) {
            node.values.splice(i, 1)
            node.keys.splice(i, 1)
            this.bufferForNodeUpdate(node)
          }
          else {
            keys.splice(keys.indexOf(key), 1)
            node.keys.splice(i, 1)
            node.values.splice(node.values.indexOf(value), 1)
            this._deleteEntry(node, key, value)
            this.bufferForNodeUpdate(node)
          }
        }
      }
    }
    this.commitHeadBuffer()
    this.commitNodeCreateBuffer()
    this.commitNodeUpdateBuffer()
    this.commitNodeDeleteBuffer()
  }

  public exists(key: K, value: V): boolean {
    const node = this.insertableNode(value)
    for (let i = 0, len = node.values.length; i < len; i++) {
      const nValue = node.values[i]
      if (this.comparator.isSame(value, nValue)) {
        const keys = node.keys[i]
        return keys.includes(key)
      }
    }
    return false
  }

  public setHeadData(data: SerializableData): void {
    this.strategy.head.data = data
    this.commitHeadBuffer()
  }

  public forceUpdate(): number {
    const keys = [...this.nodes.keys()]
    this.nodes.clear()
    this.init()
    for (const key of keys) {
      this.getNode(key)
    }
    return keys.length
  }
}
