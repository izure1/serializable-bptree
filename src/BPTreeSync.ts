import type { BPTreeConstructorOption } from './types'
import { SerializeStrategySync } from './SerializeStrategySync'
import { ValueComparator } from './base/ValueComparator'
import { BPTreeSyncBase } from './base/BPTreeSyncBase'
import { BPTreeSyncTransaction } from './transaction/BPTreeSyncTransaction'

export class BPTreeSync<K, V> extends BPTreeSyncBase<K, V> {
  constructor(
    strategy: SerializeStrategySync<K, V>,
    comparator: ValueComparator<V>,
    option?: BPTreeConstructorOption
  ) {
    super(strategy, comparator, option)
  }

  /**
   * Creates a new synchronous transaction.
   * @returns A new BPTreeSyncTransaction.
   */
  public createTransaction(): BPTreeSyncTransaction<K, V> {
    const tx = new BPTreeSyncTransaction(this)
    tx.initTransaction()
    return tx
  }

  public insert(key: K, value: V): void {
    const tx = this.createTransaction()
    tx.insert(key, value)
    const { success } = tx.commit()
    if (!success) {
      throw new Error('Transaction failed: Commit failed due to conflict')
    }
  }

  public delete(key: K, value: V): void {
    const tx = this.createTransaction()
    tx.delete(key, value)
    const { success } = tx.commit()
    if (!success) {
      throw new Error('Transaction failed: Commit failed due to conflict')
    }
  }
}
