import type { TransactionResult } from 'mvcc-api'
import type {
  AsyncBPTreeMVCC,
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
  SerializeStrategyHead
} from '../types'
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
  }

  protected async getNode(id: string): Promise<BPTreeUnknownNode<K, V>> {
    if (this.nodes.has(id)) {
      return this.nodes.get(id)!
    }
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
    this.nodes.set(id, node)
    return node
  }

  protected async _updateNode(node: BPTreeUnknownNode<K, V>): Promise<void> {
    await this.mvcc.write(node.id, node)
    this.nodes.set(node.id, node)
  }

  protected async _deleteNode(node: BPTreeUnknownNode<K, V>): Promise<void> {
    await this.mvcc.delete(node.id)
    this.nodes.delete(node.id)
  }

  protected async _readHead(): Promise<SerializeStrategyHead | null> {
    if (this.nodes.has('__HEAD__')) {
      return this.nodes.get('__HEAD__') as unknown as SerializeStrategyHead ?? null
    }
    const head = await this.mvcc.read('__HEAD__')
    return head as unknown as SerializeStrategyHead ?? null
  }

  protected async _writeHead(head: SerializeStrategyHead): Promise<void> {
    if (!(await this.mvcc.exists('__HEAD__'))) {
      await this.mvcc.create('__HEAD__', head as any)
    }
    else {
      await this.mvcc.write('__HEAD__', head as any)
    }
    this.nodes.set('__HEAD__', head as unknown as BPTreeUnknownNode<K, V>)
    this.rootId = head.root!
  }

  protected async _insertAtLeaf(node: BPTreeLeafNode<K, V>, key: K, value: V): Promise<void> {
    if (node.values.length) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        if (this.comparator.isSame(value, nValue)) {
          const keys = node.keys[i]
          if (keys.includes(key)) {
            break
          }
          keys.push(key)
          await this._updateNode(node)
          return
        }
        else if (this.comparator.isLower(value, nValue)) {
          node.values.splice(i, 0, value)
          node.keys.splice(i, 0, [key])
          await this._updateNode(node)
          return
        }
        else if (i + 1 === node.values.length) {
          node.values.push(value)
          node.keys.push([key])
          await this._updateNode(node)
          return
        }
      }
    }
    else {
      node.values = [value]
      node.keys = [[key]]
      await this._updateNode(node)
      return
    }
  }

  protected async _insertInParent(node: BPTreeUnknownNode<K, V>, value: V, pointer: BPTreeUnknownNode<K, V>): Promise<void> {
    if (this.rootId === node.id) {
      const root = await this._createNode(false, [node.id, pointer.id], [value])
      this.rootId = root.id
      node.parent = root.id
      pointer.parent = root.id

      if (pointer.leaf) {
        (node as any).next = pointer.id;
        (pointer as any).prev = node.id;
      }

      await this._writeHead({
        root: root.id,
        order: this.order,
        data: this.strategy.head.data
      })

      await this._updateNode(node)
      await this._updateNode(pointer)
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
      const leftSibling = node as unknown as BPTreeLeafNode<K, V>
      const oldNextId = leftSibling.next

      pointer.prev = leftSibling.id
      pointer.next = oldNextId
      leftSibling.next = pointer.id

      await this._updateNode(leftSibling)

      if (oldNextId) {
        const oldNext = await this.getNode(oldNextId) as BPTreeLeafNode<K, V>
        oldNext.prev = pointer.id
        await this._updateNode(oldNext)
      }
    }

    await this._updateNode(parentNode)
    await this._updateNode(pointer)

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
        await this._updateNode(n)
      }
      for (const k of parentPointer.keys) {
        const n = await this.getNode(k)
        n.parent = parentPointer.id
        await this._updateNode(n)
      }

      await this._updateNode(parentNode)
      await this._insertInParent(parentNode, midValue, parentPointer)
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
      } else {
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

      if (done) {
        if (nextNodePromise) await nextNodePromise
        break
      }

      if (nextNodePromise) {
        node = await nextNodePromise as BPTreeLeafNode<K, V>
        nextNodePromise = null
      } else {
        done = true
      }
    }
  }

  async init(): Promise<void> {
    this.clear()
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
      await this._writeHead({
        root: root,
        order: this.order,
        data: this.strategy.head.data
      })
    }
    if (this.order < 3) {
      throw new Error(`The 'order' parameter must be greater than 2. but got a '${this.order}'.`)
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
    filterValues?: Set<K>,
    limit?: number,
    order: BPTreeOrder = 'asc'
  ): AsyncGenerator<K> {
    const stream = this.whereStream(condition, limit, order)
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
    limit?: number,
    order: BPTreeOrder = 'asc'
  ): AsyncGenerator<[K, V]> {
    const driverKey = this.getDriverKey(condition)
    if (!driverKey) return

    const value = condition[driverKey] as V
    let startNode = await this.verifierStartNode[driverKey](value) as BPTreeLeafNode<K, V>
    let endNode = await this.verifierEndNode[driverKey](value) as BPTreeLeafNode<K, V> | null
    let direction = this.verifierDirection[driverKey]
    const comparator = this.verifierMap[driverKey]
    const earlyTerminate = this.verifierEarlyTerminate[driverKey]

    if (order === 'desc') {
      startNode = endNode ?? await this.rightestNode()
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

  public async keys(condition: BPTreeCondition<V>, filterValues?: Set<K>, order: BPTreeOrder = 'asc'): Promise<Set<K>> {
    const set = new Set<K>()
    for await (const key of this.keysStream(condition, filterValues, undefined, order)) {
      set.add(key)
    }
    return set
  }

  public async where(condition: BPTreeCondition<V>, order: BPTreeOrder = 'asc'): Promise<BPTreePair<K, V>> {
    const map = new Map<K, V>()
    for await (const [key, value] of this.whereStream(condition, undefined, order)) {
      map.set(key, value)
    }
    return map
  }

  public async insert(key: K, value: V): Promise<void> {
    const before = await this.insertableNode(value)
    await this._insertAtLeaf(before, key, value)

    if (before.values.length === this.order) {
      const after = await this._createNode(
        true,
        [],
        [],
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
    }
  }

  protected async _deleteEntry(
    node: BPTreeUnknownNode<K, V>,
    key: BPTreeNodeKey<K>
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
        // If it's not the first key, the separator is at keyIndex - 1.
        // If it's the first key, the separator is at 0.
        const valueIndex = keyIndex > 0 ? keyIndex - 1 : 0
        node.values.splice(valueIndex, 1)
        await this._updateNode(node)
      }
    }

    if (this.rootId === node.id && node.keys.length === 1 && !node.leaf) {
      const keys = node.keys as string[]
      this._deleteNode(node)
      const newRoot = await this.getNode(keys[0])
      newRoot.parent = null
      await this._updateNode(newRoot)
      await this._writeHead({
        root: newRoot.id,
        order: this.order,
        data: this.strategy.head.data
      })
      return
    }
    else if (this.rootId === node.id) {
      const root = await this.getNode(this.rootId)
      await this._updateNode(root)
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
            await this._updateNode(n)
          }
        }
        pointer.values.push(...node.values)

        if (!pointer.leaf) {
          const keys = pointer.keys
          for (const key of keys) {
            const node = await this.getNode(key)
            node.parent = pointer.id
            await this._updateNode(node)
          }
        }

        this._deleteNode(node)
        await this._updateNode(pointer)
        await this._deleteEntry(await this.getNode(node.parent!), node.id)
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
              await this._updateNode(parentNode)
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
              await this._updateNode(parentNode)
            }
          }
          await this._updateNode(node)
          await this._updateNode(pointer)
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
              await this._updateNode(parentNode)
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
              await this._updateNode(parentNode)
            }
          }
          await this._updateNode(node)
          await this._updateNode(pointer)
        }
        if (!pointer.leaf) {
          for (const key of pointer.keys) {
            const n = await this.getNode(key)
            n.parent = pointer.id
            await this._updateNode(n)
          }
        }
        if (!node.leaf) {
          for (const key of node.keys) {
            const n = await this.getNode(key)
            n.parent = node.id
            await this._updateNode(n)
          }
        }
        if (!parentNode.leaf) {
          for (const key of parentNode.keys) {
            const n = await this.getNode(key)
            n.parent = parentNode.id
            await this._updateNode(n)
          }
        }
      }
    } else {
      await this._updateNode(node)
    }
  }

  public async delete(key: K, value: V): Promise<void> {
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
            keys.splice(keyIndex, 1)
            if (keys.length === 0) {
              node.keys.splice(i, 1)
              node.values.splice(i, 1)
            }
            await this._updateNode(node)
            await this._deleteEntry(node, key)
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
      result = await this.mvccRoot.commit(label)
      if (result.success && this.rootTx !== this) {
        this.rootTx.rootId = this.rootId
      }
      if (result.success) {
        for (const r of result.created) {
          this.nodes.set(r.key, r.data as BPTreeUnknownNode<K, V>)
        }
        for (const r of result.updated) {
          this.nodes.set(r.key, r.data as BPTreeUnknownNode<K, V>)
        }
        for (const r of result.deleted) {
          this.nodes.delete(r.key)
        }
      }
    }
    return result
  }

  public rollback(): TransactionResult<string, BPTreeNode<K, V>> {
    return this.mvcc.rollback()
  }
}
