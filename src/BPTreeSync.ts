import type { BPTreeConstructorOption } from './types'
import { SyncMVCCTransaction } from 'mvcc-api'
import { SerializeStrategySync } from './SerializeStrategySync'
import { ValueComparator } from './base/ValueComparator'
import { BPTreeSyncTransaction } from './transaction/BPTreeSyncTransaction'
import { BPTreeMVCCStrategySync } from './transaction/BPTreeMVCCStrategySync'

export class BPTreeSync<K, V> extends BPTreeSyncTransaction<K, V> {
  constructor(
    strategy: SerializeStrategySync<K, V>,
    comparator: ValueComparator<V>,
    option?: BPTreeConstructorOption
  ) {
    const mvccRoot = new SyncMVCCTransaction(new BPTreeMVCCStrategySync(strategy))
    super(
      null as any,
      mvccRoot as any,
      mvccRoot as any,
      strategy,
      comparator,
      option,
    )
  }

  /**
   * Creates a new synchronous transaction.
   * @returns A new BPTreeSyncTransaction.
   */
  public createTransaction(): BPTreeSyncTransaction<K, V> {
    const nestedTx = this.mvcc.createNested()
    const tx = new BPTreeSyncTransaction(
      this,
      this.mvcc,
      nestedTx,
      this.strategy,
      this.comparator,
      this.option
    );
    (tx as any)._initInternal()
    return tx
  }

  public insert(key: K, value: V): void {
    const tx = this.createTransaction()
    tx.insert(key, value)
    const result = tx.commit()
    if (!result.success) {
      throw new Error(`Transaction failed: ${result.error || 'Commit failed due to conflict'}`)
    }
  }

  public delete(key: K, value: V): void {
    const tx = this.createTransaction()
    tx.delete(key, value)
    const result = tx.commit()
    if (!result.success) {
      throw new Error(`Transaction failed: ${result.error || 'Commit failed due to conflict'}`)
    }
  }
}
