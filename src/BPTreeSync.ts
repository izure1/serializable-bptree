import type { BPTreeConstructorOption, BPTreeUnknownNode } from './types'
import { SyncMVCCTransaction } from 'mvcc-api'
import { SerializeStrategySync } from './SerializeStrategySync'
import { ValueComparator } from './base/ValueComparator'
import { BPTreeSyncBase } from './base/BPTreeSyncBase'
import { BPTreeSyncTransaction } from './transaction/BPTreeSyncTransaction'
import { BPTreeMVCCStrategySync } from './transaction/BPTreeMVCCStrategySync'

export class BPTreeSync<K, V> extends BPTreeSyncBase<K, V> {
  public readonly mvccRoot: SyncMVCCTransaction<BPTreeMVCCStrategySync<K, V>, string, BPTreeUnknownNode<K, V> | null>

  constructor(
    strategy: SerializeStrategySync<K, V>,
    comparator: ValueComparator<V>,
    option?: BPTreeConstructorOption
  ) {
    super(strategy, comparator, option)
    this.mvccRoot = new SyncMVCCTransaction(new BPTreeMVCCStrategySync(strategy))
  }

  /**
   * Creates a new synchronous transaction.
   * @returns A new BPTreeSyncTransaction.
   */
  public createTransaction(): BPTreeSyncTransaction<K, V> {
    const nestedTx = this.mvccRoot.createNested()
    const tx = new BPTreeSyncTransaction(this, nestedTx)
    tx.initTransaction()
    return tx
  }

  public applyCommit(rootId: string, order: number, changes: { created: string[], deleted: string[], updated: string[] }): void {
    super.applyCommit(rootId, order, changes)
    this.strategy.writeHead(this.strategy.head)
  }

  public insert(key: K, value: V): void {
    const tx = this.createTransaction()
    tx.insert(key, value)
    const { success, error } = tx.commit()
    if (!success) {
      throw new Error(`Transaction failed: ${error || 'Commit failed due to conflict'}`)
    }
  }

  public delete(key: K, value: V): void {
    const tx = this.createTransaction()
    tx.delete(key, value)
    const { success, error } = tx.commit()
    if (!success) {
      throw new Error(`Transaction failed: ${error || 'Commit failed due to conflict'}`)
    }
  }
}
