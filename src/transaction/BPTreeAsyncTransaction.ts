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
  private dirtyIds: Set<string> = new Set()
  private createdInTx: Set<string> = new Set()

  private initialRootId: string
  private transactionRootId: string

  constructor(baseTree: BPTreeAsyncBase<K, V>) {
    super((baseTree as any).strategy, (baseTree as any).comparator, (baseTree as any).option)
    this.realBaseTree = baseTree
    this.realBaseStrategy = (baseTree as any).strategy
    this.initialRootId = ''
    this.transactionRootId = ''
  }

  /**
   * Initializes the transaction by capturing the current state of the tree.
   */
  public async initTransaction(): Promise<void> {
    const head = await this.realBaseStrategy.readHead()
    this.initialRootId = head?.root ?? (this.realBaseTree as any).rootId
    this.transactionRootId = this.initialRootId
    this.rootId = this.transactionRootId

    const snapshotStrategy = new BPTreeAsyncSnapshotStrategy(this.realBaseStrategy, this.initialRootId);
    (this as any).strategy = snapshotStrategy

    this.txNodes.clear()
    this.dirtyIds.clear()
    this.createdInTx.clear()
  }

  protected async getNode(id: string): Promise<BPTreeUnknownNode<K, V>> {
    if (this.txNodes.has(id)) {
      return this.txNodes.get(id)!
    }

    const baseNode = await this.realBaseStrategy.read(id)
    const clone = JSON.parse(JSON.stringify(baseNode))

    this.txNodes.set(id, clone)
    return clone
  }

  protected async bufferForNodeUpdate(node: BPTreeUnknownNode<K, V>): Promise<void> {
    this.txNodes.set(node.id, node)
    this.dirtyIds.add(node.id)
    await this.markPathDirty(node)
  }

  protected async bufferForNodeCreate(node: BPTreeUnknownNode<K, V>): Promise<void> {
    this.txNodes.set(node.id, node)
    this.dirtyIds.add(node.id)
    this.createdInTx.add(node.id)
    await this.markPathDirty(node)
  }

  protected async bufferForNodeDelete(node: BPTreeUnknownNode<K, V>): Promise<void> {
    this.txNodes.delete(node.id)
    this.dirtyIds.add(node.id)
  }

  private async markPathDirty(node: BPTreeUnknownNode<K, V>): Promise<void> {
    let curr = node
    while (curr.parent) {
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
    leaf = false,
    parent: string | null = null,
    next: string | null = null,
    prev: string | null = null
  ): Promise<BPTreeUnknownNode<K, V>> {
    const id = await this.realBaseStrategy.id(isLeaf)!
    const node: BPTreeUnknownNode<K, V> = {
      id,
      keys,
      values,
      leaf: isLeaf as any,
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

    let newRootId = this.transactionRootId
    if (idMapping.has(this.transactionRootId)) {
      newRootId = idMapping.get(this.transactionRootId)!
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
   * Internal use only.
   */
  protected async rollback(): Promise<void> {
    this.txNodes.clear()
    this.dirtyIds.clear()
    this.createdInTx.clear()
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
