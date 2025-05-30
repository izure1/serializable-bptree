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
  declare protected readonly nodes: ReturnType<BPTreeAsync<K, V>['_createCachedNode']>

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
      lifespan: this.option.lifespan ?? '3m'
    })
  }

  protected async getPairsRightToLeft(
    value: V,
    startNode: BPTreeLeafNode<K, V>,
    endNode: BPTreeLeafNode<K, V>|null,
    comparator: (nodeValue: V, value: V) => boolean
  ): Promise<BPTreePair<K, V>> {
    const pairs: [K, V][] = []
    let node = startNode
    let done = false
    while (!done) {
      if (endNode && node.id === endNode.id) {
        done = true
        break
      }
      let i = node.values.length
      while (i--) {
        const nValue = node.values[i]
        const keys = node.keys[i]
        if (comparator(nValue, value)) {
          let j = keys.length
          while (j--) {
            pairs.push([keys[j], nValue])
          }
        }
      }
      if (!node.prev) {
        done = true
        break
      }
      node = await this.getNode(node.prev) as BPTreeLeafNode<K, V>
    }
    return new Map(pairs.reverse())
  }

  protected async getPairsLeftToRight(
    value: V,
    startNode: BPTreeLeafNode<K, V>,
    endNode: BPTreeLeafNode<K, V>|null,
    comparator: (nodeValue: V, value: V) => boolean
  ): Promise<BPTreePair<K, V>> {
    const pairs: [K, V][] = []
    let node = startNode
    let done = false
    while (!done) {
      if (endNode && node.id === endNode.id) {
        done = true
        break
      }
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const keys = node.keys[i]
        if (comparator(nValue, value)) {
          for (let j = 0, len = keys.length; j < len; j++) {
            const key = keys[j]
            pairs.push([key, nValue])
          }
        }
      }
      if (!node.next) {
        done = true
        break
      }
      node = await this.getNode(node.next) as BPTreeLeafNode<K, V>
    }
    return new Map(pairs)
  }

  protected async getPairs(
    value: V,
    startNode: BPTreeLeafNode<K, V>,
    endNode: BPTreeLeafNode<K, V>|null,
    comparator: (nodeValue: V, value: V) => boolean,
    direction: 1|-1
  ): Promise<BPTreePair<K, V>> {
    switch (direction) {
      case -1:  return await this.getPairsRightToLeft(value, startNode, endNode, comparator)
      case +1:  return await this.getPairsLeftToRight(value, startNode, endNode, comparator)
      default:  throw new Error(`Direction must be -1 or 1. but got a ${direction}`)
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
    keys: string[]|K[][],
    values: V[],
    leaf = false,
    parent: string|null = null,
    next: string|null = null,
    prev: string|null = null
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

    if (this.root.id === node.id && node.keys.length === 1) {
      const keys = node.keys as string[]
      this.bufferForNodeDelete(this.root)
      this.root = await this.getNode(keys[0])
      this.root.parent = null
      this.strategy.head.root = this.root.id
      this.bufferForNodeUpdate(this.root)
      return
    }
    else if (this.root.id === node.id) {
      this.bufferForNodeUpdate(this.root)
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
      let parentNode = await this.getNode(node.parent) as BPTreeInternalNode<K, V>
      let prevNode: BPTreeInternalNode<K, V>|null = null
      let nextNode: BPTreeInternalNode<K, V>|null = null
      let prevValue: V|null = null
      let postValue: V|null = null

      for (let i = 0, len = parentNode.keys.length; i < len; i++) {
        const nKey = parentNode.keys[i]
        if (nKey === node.id) {
          if (i > 0) {
            prevNode = await this.getNode(parentNode.keys[i-1]) as BPTreeInternalNode<K, V>
            prevValue = parentNode.values[i-1]
          }
          if (i < parentNode.keys.length-1) {
            nextNode = await this.getNode(parentNode.keys[i+1]) as BPTreeInternalNode<K, V>
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
    if (this.root.id === node.id) {
      const root = await this._createNode(false, [node.id, pointer.id], [value])
      this.root = root
      this.strategy.head.root = root.id
      node.parent = root.id
      pointer.parent = root.id
      this.bufferForNodeUpdate(node)
      this.bufferForNodeUpdate(pointer)
      return
    }
    const parentNode = await this.getNode(node.parent!) as BPTreeInternalNode<K, V>
    for (let i = 0, len = parentNode.keys.length; i < len; i++) {
      const nKeys = parentNode.keys[i]
      if (nKeys === node.id) {
        parentNode.values.splice(i, 0, value)
        parentNode.keys.splice(i+1, 0, pointer.id)
        this.bufferForNodeUpdate(parentNode)

        if (parentNode.keys.length > this.order) {
          const parentPointer = await this._createNode(false, [], []) as BPTreeInternalNode<K, V>
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
    }
  }

  async init(): Promise<void> {
    const head = await this.strategy.readHead()
    // first created
    if (head === null) {
      this.order = this.strategy.order
      this.root = await this._createNode(true, [], [], true)
      this.strategy.head.root = this.root.id
      await this.commitHeadBuffer()
      await this.commitNodeCreateBuffer()
    }
    // loaded
    else {
      const { root, order } = head
      this.strategy.head = head
      this.order = order
      this.root = await this.getNode(root!)
    }
    if (this.order < 3) {
      throw new Error(`The 'order' parameter must be greater than 2. but got a '${this.order}'.`)
    }
  }

  protected async getNode(id: string): Promise<BPTreeUnknownNode<K, V>> {
    if (this._nodeCreateBuffer.has(id)) {
      return this._nodeCreateBuffer.get(id)!
    }
    const cache = await this.nodes.cache(id)
    return cache.raw
  }

  protected async insertableNode(value: V): Promise<BPTreeLeafNode<K, V>> {
    let node = this.root
    while (!node.leaf) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        const k = node.keys
        if (this.comparator.isSame(value, nValue)) {
          node = await this.getNode(k[i+1])
          break
        }
        else if (this.comparator.isLower(value, nValue)) {
          node = await this.getNode(k[i])
          break
        }
        else if (i+1 === node.values.length) {
          node = await this.getNode(k[i+1])
          break
        }
      }
    }
    return node
  }

  protected async insertableEndNode(value: V, direction: 1|-1): Promise<BPTreeLeafNode<K, V>|null> {
    const insertableNode = await this.insertableNode(value)
    let key: 'next'|'prev'
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
    let node = this.root
    while (!node.leaf) {
      const keys = node.keys
      node = await this.getNode(keys[0])
    }
    return node
  }

  protected async rightestNode(): Promise<BPTreeLeafNode<K, V>> {
    let node = this.root
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
    }
    this._nodeUpdateBuffer.clear()
  }

  protected async commitNodeDeleteBuffer(): Promise<void> {
    for (const node of this._nodeDeleteBuffer.values()) {
      await this.strategy.delete(node.id)
    }
    this._nodeDeleteBuffer.clear()
  }

  public async keys(condition: BPTreeCondition<V>, filterValues?: Set<K>): Promise<Set<K>> {
    for (const k in condition) {
      const key = k as keyof BPTreeCondition<V>
      const value = condition[key] as V
      const startNode   = await this.verifierStartNode[key](value) as BPTreeLeafNode<K, V>
      const endNode     = await this.verifierEndNode[key](value) as BPTreeLeafNode<K, V>|null
      const direction   = this.verifierDirection[key]
      const comparator  = this.verifierMap[key]
      const pairs       = await this.getPairs(value, startNode, endNode, comparator, direction)
      if (!filterValues) {
        filterValues = new Set(pairs.keys())
      }
      else {
        const intersections = new Set<K>()
        for (const key of filterValues) {
          const has = pairs.has(key)
          if (has) {
            intersections.add(key)
          }
        }
        filterValues = intersections
      }
    }
    return filterValues ?? new Set([])
  }

  public async where(condition: BPTreeCondition<V>): Promise<BPTreePair<K, V>> {
    let result: BPTreePair<K, V>|null = null
    for (const k in condition) {
      const key = k as keyof BPTreeCondition<V>
      const value = condition[key] as V
      const startNode   = await this.verifierStartNode[key](value) as BPTreeLeafNode<K, V>
      const endNode     = await this.verifierEndNode[key](value) as BPTreeLeafNode<K, V>|null
      const direction   = this.verifierDirection[key]
      const comparator  = this.verifierMap[key]
      const pairs = await this.getPairs(value, startNode, endNode, comparator, direction)
      if (result === null) {
        result = pairs
      }
      else {
        const intersection = new Map<K, V>()
        for (const [k, v] of pairs) {
          if (result.has(k)) {
            intersection.set(k, v)
          }
        }
        result = intersection
      }
    }
    return result ?? new Map()
  }

  public async insert(key: K, value: V): Promise<void> {
    const before = await this.insertableNode(value)
    this._insertAtLeaf(before, key, value)

    if (before.values.length === this.order) {
      const after = await this._createNode(
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
        const node = await this.getNode(beforeNext)
        node.prev = after.id
        this.bufferForNodeUpdate(node)
      }
      await this._insertInParent(before, after.values[0], after)
      this.bufferForNodeUpdate(before)
    }

    await this.commitHeadBuffer()
    await this.commitNodeCreateBuffer()
    await this.commitNodeUpdateBuffer()
  }

  public async delete(key: K, value: V): Promise<void> {
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
          else if (node.id === this.root.id) {
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
  }

  public async exists(key: K, value: V): Promise<boolean> {
    const node = await this.insertableNode(value)
    for (let i = 0, len = node.values.length; i < len; i++) {
      const nValue = node.values[i]
      if (this.comparator.isSame(value, nValue)) {
        const keys = node.keys[i]
        return keys.includes(key)
      }
    }
    return false
  }

  public async setHeadData(data: SerializableData): Promise<void> {
    this.strategy.head.data = data
    this._strategyDirty = true
    await this.commitHeadBuffer()
  }

  public async forceUpdate(): Promise<number> {
    const keys = [...this.nodes.keys()]
    this.nodes.clear()
    await this.init()
    for (const key of keys) {
      await this.getNode(key)
    }
    return keys.length
  }
}
