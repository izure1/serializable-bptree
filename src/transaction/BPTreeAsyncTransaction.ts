import type {
  BPTreeLeafNode,
  BPTreeUnknownNode,
  BPTreeInternalNode,
  BPTreeTransactionResult
} from '../types'
import { SerializeStrategyAsync } from '../SerializeStrategyAsync'
import { BPTreeAsyncBase } from '../base/BPTreeAsyncBase'
import { BPTreeAsyncSnapshotStrategy } from './BPTreeAsyncSnapshotStrategy'

/**
 * Represents an asynchronous transaction for a B+ Tree.
 * Provides Snapshot Isolation using MVCC and Copy-on-Write techniques.
 */
export class BPTreeAsyncTransaction<K, V> extends BPTreeAsyncBase<K, V> {
  private readonly realBaseTree: BPTreeAsyncBase<K, V>
  private readonly realBaseStrategy: SerializeStrategyAsync<K, V>

  private txNodes: Map<string, BPTreeUnknownNode<K, V>> = new Map()
  protected readonly dirtyIds: Set<string>
  protected readonly createdInTx: Set<string>
  protected readonly deletedIds: Set<string>

  private initialRootId: string
  private transactionRootId: string

  constructor(baseTree: BPTreeAsyncBase<K, V>) {
    super((baseTree as any).strategy, (baseTree as any).comparator, (baseTree as any).option)
    this.realBaseTree = baseTree
    this.realBaseStrategy = (baseTree as any).strategy
    this.order = baseTree.getOrder()
    this.initialRootId = ''
    this.transactionRootId = ''
    this.dirtyIds = new Set()
    this.createdInTx = new Set()
    this.deletedIds = new Set()
  }

  /**
   * Initializes the transaction by capturing the current state of the tree.
   */
  public async initTransaction(): Promise<void> {
    const head = await this.realBaseStrategy.readHead()
    if (head) {
      this.order = head.order
      this.initialRootId = head.root!
    } else {
      this.initialRootId = this.realBaseTree.getRootId()
    }

    if (!this.initialRootId) {
      const root = await this._createNode(true, [], [], true)
      this.initialRootId = root.id
    }

    this.transactionRootId = this.initialRootId
    this.rootId = this.transactionRootId

    const snapshotStrategy = new BPTreeAsyncSnapshotStrategy(this.realBaseStrategy, this.initialRootId);
    (this as any).strategy = snapshotStrategy

    this.txNodes.clear()
    this.dirtyIds.clear()
    this.createdInTx.clear()
    this.deletedIds.clear()
  }

  protected async getNode(id: string): Promise<BPTreeUnknownNode<K, V>> {
    if (this.txNodes.has(id)) {
      return this.txNodes.get(id)!
    }

    if (this.deletedIds.has(id)) {
      throw new Error(`The tree attempted to reference deleted node '${id}'`)
    }

    const baseNode = await this.realBaseStrategy.read(id)
    const clone = JSON.parse(JSON.stringify(baseNode))

    this.txNodes.set(id, clone)
    return clone
  }

  protected async bufferForNodeUpdate(node: BPTreeUnknownNode<K, V>): Promise<void> {
    if (this.dirtyIds.has(node.id) && this.txNodes.has(node.id) && (node as any)._p) {
      this.txNodes.set(node.id, node)
      return
    }
    (node as any)._p = true
    this.txNodes.set(node.id, node)
    this.dirtyIds.add(node.id)
    if (node.leaf) {
      if (node.next && !this.dirtyIds.has(node.next) && !this.deletedIds.has(node.next)) {
        try {
          await this.bufferForNodeUpdate(await this.getNode(node.next))
        } catch (e) { }
      }
      if (node.prev && !this.dirtyIds.has(node.prev) && !this.deletedIds.has(node.prev)) {
        try {
          await this.bufferForNodeUpdate(await this.getNode(node.prev))
        } catch (e) { }
      }
    }
    await this.markPathDirty(node)
    delete (node as any)._p
  }

  protected async bufferForNodeCreate(node: BPTreeUnknownNode<K, V>): Promise<void> {
    this.txNodes.set(node.id, node)
    this.dirtyIds.add(node.id)
    this.createdInTx.add(node.id)
    if (node.leaf) {
      if (node.next && !this.dirtyIds.has(node.next) && !this.deletedIds.has(node.next)) {
        try {
          await this.bufferForNodeUpdate(await this.getNode(node.next))
        } catch (e) { }
      }
      if (node.prev && !this.dirtyIds.has(node.prev) && !this.deletedIds.has(node.prev)) {
        try {
          await this.bufferForNodeUpdate(await this.getNode(node.prev))
        } catch (e) { }
      }
    }
    await this.markPathDirty(node)
  }

  protected async bufferForNodeDelete(node: BPTreeUnknownNode<K, V>): Promise<void> {
    this.txNodes.delete(node.id)
    this.dirtyIds.add(node.id)
    this.deletedIds.add(node.id)
  }

  private async markPathDirty(node: BPTreeUnknownNode<K, V>): Promise<void> {
    let curr = node
    while (curr.parent) {
      if (this.deletedIds.has(curr.parent)) {
        break
      }
      if (this.dirtyIds.has(curr.parent) && this.txNodes.has(curr.parent)) {
        break
      }
      const parent = await this.getNode(curr.parent)
      this.dirtyIds.add(parent.id)
      curr = parent
    }
    if (!curr.parent) {
      this.transactionRootId = curr.id
    }
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
    const id = (await this.strategy.id(isLeaf))!
    const node: BPTreeUnknownNode<K, V> = {
      id,
      keys,
      values,
      leaf: leaf as any,
      parent,
      next,
      prev,
    } as any

    await this.bufferForNodeCreate(node)
    return node
  }

  /**
   * Attempts to commit the transaction.
   * Uses Optimistic Locking (Compare-And-Swap) on the root node ID to detect conflicts.
   * 
   * @returns A promise that resolves to the transaction result.
   */
  public async commit(): Promise<BPTreeTransactionResult> {
    const idMapping: Map<string, string> = new Map()
    const finalNodes: BPTreeUnknownNode<K, V>[] = []

    for (const oldId of this.dirtyIds) {
      if (this.createdInTx.has(oldId)) {
        idMapping.set(oldId, oldId)
      } else {
        const node = this.txNodes.get(oldId)
        if (node) {
          const newId = (await this.realBaseStrategy.id(node.leaf as any))!
          idMapping.set(oldId, newId)
        }
      }
    }

    const newCreatedIds: string[] = []
    for (const oldId of this.dirtyIds) {
      const node = this.txNodes.get(oldId)
      if (!node) continue

      const newId = idMapping.get(oldId)!
      node.id = newId
      if (node.parent && idMapping.has(node.parent)) {
        node.parent = idMapping.get(node.parent)!
      }
      if (!node.leaf) {
        const internal = node as BPTreeInternalNode<K, V>
        for (let i = 0; i < internal.keys.length; i++) {
          const childId = internal.keys[i]
          if (idMapping.has(childId)) {
            internal.keys[i] = idMapping.get(childId)!
          }
        }
      }
      if (node.leaf) {
        const leaf = node as BPTreeLeafNode<K, V>
        if (leaf.next && idMapping.has(leaf.next)) {
          leaf.next = idMapping.get(leaf.next)!
        }
        if (leaf.prev && idMapping.has(leaf.prev)) {
          leaf.prev = idMapping.get(leaf.prev)!
        }
      }
      finalNodes.push(node)
      newCreatedIds.push(newId)
    }

    let newRootId = this.rootId
    if (idMapping.has(this.rootId)) {
      newRootId = idMapping.get(this.rootId)!
    }

    for (const node of finalNodes) {
      await this.realBaseStrategy.write(node.id, node)
    }

    const success = await (this.realBaseStrategy as any).compareAndSwapHead(this.initialRootId, newRootId)

    if (success) {
      const distinctObsolete = new Set<string>()
      for (const oldId of this.dirtyIds) {
        if (!this.createdInTx.has(oldId) && this.txNodes.has(oldId)) {
          distinctObsolete.add(oldId)
        }
      }
      return {
        success: true,
        createdIds: newCreatedIds,
        obsoleteIds: Array.from(distinctObsolete)
      }
    } else {
      await this.rollback()
      return {
        success: false,
        createdIds: newCreatedIds,
        obsoleteIds: []
      }
    }
  }

  /**
   * Rolls back the transaction by clearing all buffered changes.
   * If cleanup is `true`, it also clears the transaction nodes.
   * @param cleanup Whether to clear the transaction nodes.
   * @returns The IDs of nodes that were created in this transaction.
   */
  async rollback(cleanup: boolean = true): Promise<string[]> {
    const createdIds = Array.from(this.createdInTx)
    this.txNodes.clear()
    this.dirtyIds.clear()
    this.createdInTx.clear()
    if (cleanup) {
      for (const id of createdIds) {
        await this.realBaseStrategy.delete(id)
      }
    }
    return createdIds
  }

  protected async readLock<T>(fn: () => Promise<T>): Promise<T> {
    return await fn()
  }

  protected async writeLock<T>(fn: () => Promise<T>): Promise<T> {
    return await fn()
  }

  protected async commitHeadBuffer(): Promise<void> { }
  protected async commitNodeCreateBuffer(): Promise<void> { }
  protected async commitNodeUpdateBuffer(): Promise<void> { }
  protected async commitNodeDeleteBuffer(): Promise<void> { }
}
