import type {
  BPTreeLeafNode,
  BPTreeUnknownNode,
  BPTreeInternalNode,
  BPTreeTransactionResult
} from '../types'
import { SerializeStrategySync } from '../SerializeStrategySync'
import { BPTreeSyncBase } from '../base/BPTreeSyncBase'
import { BPTreeSyncSnapshotStrategy } from './BPTreeSyncSnapshotStrategy'

/**
 * Represents a synchronous transaction for a B+ Tree.
 * Provides Snapshot Isolation using MVCC and Copy-on-Write techniques.
 */
export class BPTreeSyncTransaction<K, V> extends BPTreeSyncBase<K, V> {
  private readonly realBaseTree: BPTreeSyncBase<K, V>
  private readonly realBaseStrategy: SerializeStrategySync<K, V>

  private txNodes: Map<string, BPTreeUnknownNode<K, V>> = new Map()
  protected readonly dirtyIds: Set<string>
  protected readonly createdInTx: Set<string>
  protected readonly deletedIds: Set<string>

  private initialRootId: string
  private transactionRootId: string

  private transactionId: number
  private initialLastCommittedTransactionId: number = 0

  constructor(baseTree: BPTreeSyncBase<K, V>) {
    super((baseTree as any).strategy, (baseTree as any).comparator, (baseTree as any).option)
    this.realBaseTree = baseTree
    this.realBaseStrategy = (baseTree as any).strategy
    this.order = baseTree.getOrder()
    this.initialRootId = ''
    this.transactionRootId = ''
    this.dirtyIds = new Set()
    this.createdInTx = new Set()
    this.deletedIds = new Set()
    this.transactionId = Date.now() + Math.random()
  }

  /**
   * Initializes the transaction by capturing the current state of the tree.
   */
  public initTransaction(): void {
    const head = this.realBaseStrategy.readHead()
    if (head) {
      this.order = head.order
      this.initialRootId = head.root!
    } else {
      this.initialRootId = this.realBaseTree.getRootId()
    }

    if (!this.initialRootId) {
      const root = this._createNode(true, [], [], true)
      this.initialRootId = root.id
    }
    this.initialLastCommittedTransactionId = this.realBaseStrategy.getLastCommittedTransactionId()

    this.transactionRootId = this.initialRootId
    this.rootId = this.transactionRootId

    const snapshotStrategy = new BPTreeSyncSnapshotStrategy(this.realBaseStrategy, this.initialRootId)
      ; (this as any).strategy = snapshotStrategy

    this.txNodes.clear()
    this.dirtyIds.clear()
    this.createdInTx.clear()
    this.deletedIds.clear()
  }

  protected getNode(id: string): BPTreeUnknownNode<K, V> {
    if (this.txNodes.has(id)) {
      return this.txNodes.get(id)!
    }

    if (this.deletedIds.has(id)) {
      throw new Error(`The tree attempted to reference deleted node '${id}'`)
    }

    const baseNode = this.realBaseStrategy.read(id)
    const clone = JSON.parse(JSON.stringify(baseNode))

    this.txNodes.set(id, clone)
    return clone
  }

  protected bufferForNodeUpdate(node: BPTreeUnknownNode<K, V>): void {
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
          this.bufferForNodeUpdate(this.getNode(node.next))
        } catch (e) { }
      }
      if (node.prev && !this.dirtyIds.has(node.prev) && !this.deletedIds.has(node.prev)) {
        try {
          this.bufferForNodeUpdate(this.getNode(node.prev))
        } catch (e) { }
      }
    }
    this.markPathDirty(node)
    delete (node as any)._p
  }

  protected bufferForNodeCreate(node: BPTreeUnknownNode<K, V>): void {
    this.txNodes.set(node.id, node)
    this.dirtyIds.add(node.id)
    this.createdInTx.add(node.id)
    if (node.leaf) {
      if (node.next && !this.dirtyIds.has(node.next) && !this.deletedIds.has(node.next)) {
        try {
          this.bufferForNodeUpdate(this.getNode(node.next))
        } catch (e) { }
      }
      if (node.prev && !this.dirtyIds.has(node.prev) && !this.deletedIds.has(node.prev)) {
        try {
          this.bufferForNodeUpdate(this.getNode(node.prev))
        } catch (e) { }
      }
    }
    this.markPathDirty(node)
  }

  protected bufferForNodeDelete(node: BPTreeUnknownNode<K, V>): void {
    this.txNodes.delete(node.id)
    this.dirtyIds.add(node.id)
    this.deletedIds.add(node.id)
  }

  private markPathDirty(node: BPTreeUnknownNode<K, V>): void {
    let curr = node
    while (curr.parent) {
      if (this.deletedIds.has(curr.parent)) {
        break
      }
      if (this.dirtyIds.has(curr.parent) && this.txNodes.has(curr.parent)) {
        break
      }
      const parent = this.getNode(curr.parent)
      this.dirtyIds.add(parent.id)
      curr = parent
    }
    if (!curr.parent) {
      this.transactionRootId = curr.id
    }
  }

  protected _createNode(
    isLeaf: boolean,
    keys: string[] | K[][],
    values: V[],
    leaf = isLeaf,
    parent: string | null = null,
    next: string | null = null,
    prev: string | null = null
  ): BPTreeUnknownNode<K, V> {
    const id = this.strategy.id(isLeaf)!
    const node: BPTreeUnknownNode<K, V> = {
      id,
      keys,
      values,
      leaf: leaf as any,
      parent,
      next,
      prev,
    } as any

    this.bufferForNodeCreate(node)
    return node
  }

  /**
   * Attempts to commit the transaction.
   * Uses Optimistic Locking (Compare-And-Swap) on the root node ID to detect conflicts.
   * 
   * @param cleanup Whether to clean up obsolete nodes after commit. Defaults to true.
   * @returns The transaction result.
   */
  public commit(cleanup: boolean = true): BPTreeTransactionResult {
    const idMapping: Map<string, string> = new Map()
    const finalNodes: BPTreeUnknownNode<K, V>[] = []

    for (const oldId of this.dirtyIds) {
      if (this.createdInTx.has(oldId)) {
        idMapping.set(oldId, oldId)
      } else {
        const node = this.txNodes.get(oldId)
        if (node) {
          const newId = this.realBaseStrategy.id(node.leaf as any)!
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

    // OCC Check: Only commit if base strategy's lastCommittedTransactionId hasn't changed
    let success = false
    if (finalNodes.length === 0) {
      // No changes made in this transaction (Read-only or no-op)
      success = true
    } else if (this.realBaseStrategy.getLastCommittedTransactionId() === this.initialLastCommittedTransactionId) {
      // Perform writes only when OCC check passes
      for (const node of finalNodes) {
        this.realBaseStrategy.write(node.id, node)
      }
      this.realBaseStrategy.compareAndSwapHead(newRootId, this.transactionId)
      success = true
    }

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
      this.rollback(cleanup)
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
  rollback(cleanup: boolean = true): string[] {
    const createdIds = Array.from(this.createdInTx)
    this.txNodes.clear()
    this.dirtyIds.clear()
    this.createdInTx.clear()
    if (cleanup) {
      for (const id of createdIds) {
        this.realBaseStrategy.delete(id)
      }
    }
    return createdIds
  }

  // Override to do nothing, as transaction handles its own commits
  protected commitHeadBuffer(): void { }
  protected commitNodeCreateBuffer(): void { }
  protected commitNodeUpdateBuffer(): void { }
  protected commitNodeDeleteBuffer(): void { }
}
