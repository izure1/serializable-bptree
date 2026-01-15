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
  private dirtyIds: Set<string> = new Set()
  private createdInTx: Set<string> = new Set()

  private initialRootId: string
  private transactionRootId: string

  constructor(baseTree: BPTreeSyncBase<K, V>) {
    super((baseTree as any).strategy, (baseTree as any).comparator, (baseTree as any).option)
    this.realBaseTree = baseTree
    this.realBaseStrategy = (baseTree as any).strategy
    this.initialRootId = ''
    this.transactionRootId = ''
  }

  /**
   * Initializes the transaction by capturing the current state of the tree.
   */
  public initTransaction(): void {
    const head = this.realBaseStrategy.readHead()
    this.initialRootId = head?.root ?? (this.realBaseTree as any).rootId
    this.transactionRootId = this.initialRootId
    this.rootId = this.transactionRootId

    const snapshotStrategy = new BPTreeSyncSnapshotStrategy(this.realBaseStrategy, this.initialRootId);
    (this as any).strategy = snapshotStrategy

    this.txNodes.clear()
    this.dirtyIds.clear()
    this.createdInTx.clear()
  }

  protected getNode(id: string): BPTreeUnknownNode<K, V> {
    if (this.txNodes.has(id)) {
      return this.txNodes.get(id)!
    }

    const baseNode = this.realBaseStrategy.read(id)
    const clone = JSON.parse(JSON.stringify(baseNode))

    this.txNodes.set(id, clone)
    return clone
  }

  protected bufferForNodeUpdate(node: BPTreeUnknownNode<K, V>): void {
    this.txNodes.set(node.id, node)
    this.dirtyIds.add(node.id)
    this.markPathDirty(node)
  }

  protected bufferForNodeCreate(node: BPTreeUnknownNode<K, V>): void {
    this.txNodes.set(node.id, node)
    this.dirtyIds.add(node.id)
    this.createdInTx.add(node.id)
    this.markPathDirty(node)
  }

  protected bufferForNodeDelete(node: BPTreeUnknownNode<K, V>): void {
    this.txNodes.delete(node.id)
    this.dirtyIds.add(node.id)
  }

  private markPathDirty(node: BPTreeUnknownNode<K, V>): void {
    let curr = node
    while (curr.parent) {
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
    leaf = false,
    parent: string | null = null,
    next: string | null = null,
    prev: string | null = null
  ): BPTreeUnknownNode<K, V> {
    const id = this.realBaseStrategy.id(isLeaf)!
    const node: BPTreeUnknownNode<K, V> = {
      id,
      keys,
      values,
      leaf: isLeaf as any,
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
   * @returns The transaction result.
   */
  public commit(): BPTreeTransactionResult {
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

    let newRootId = this.transactionRootId
    if (idMapping.has(this.transactionRootId)) {
      newRootId = idMapping.get(this.transactionRootId)!
    }

    for (const node of finalNodes) {
      this.realBaseStrategy.write(node.id, node)
    }

    const success = (this.realBaseStrategy as any).compareAndSwapHead(this.initialRootId, newRootId)

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
      this.rollback()
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
  protected rollback(): void {
    this.txNodes.clear()
    this.dirtyIds.clear()
    this.createdInTx.clear()
  }

  // Override to do nothing, as transaction handles its own commits
  protected commitHeadBuffer(): void { }
  protected commitNodeCreateBuffer(): void { }
  protected commitNodeUpdateBuffer(): void { }
  protected commitNodeDeleteBuffer(): void { }
}
