import type { BPTreeConstructorOption } from './types'
import { AsyncMVCCTransaction } from 'mvcc-api'
import { SerializeStrategyAsync } from './SerializeStrategyAsync'
import { ValueComparator } from './base/ValueComparator'
import { BPTreeAsyncTransaction } from './transaction/BPTreeAsyncTransaction'
import { BPTreeMVCCStrategyAsync } from './transaction/BPTreeMVCCStrategyAsync'

export class BPTreeAsync<K, V> extends BPTreeAsyncTransaction<K, V> {
  constructor(
    strategy: SerializeStrategyAsync<K, V>,
    comparator: ValueComparator<V>,
    option?: BPTreeConstructorOption
  ) {
    const mvccRoot = new AsyncMVCCTransaction(new BPTreeMVCCStrategyAsync(strategy))
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
   * Creates a new asynchronous transaction.
   * @returns A new BPTreeAsyncTransaction.
   */
  public async createTransaction(): Promise<BPTreeAsyncTransaction<K, V>> {
    const nestedTx = this.mvcc.createNested()
    const tx = new BPTreeAsyncTransaction(
      this,
      this.mvcc,
      nestedTx,
      this.strategy,
      this.comparator,
      this.option
    )
    await (tx as any)._initInternal()
    return tx
  }

  public async insert(key: K, value: V): Promise<void> {
    return this.writeLock(1, async () => {
      const tx = await this.createTransaction()
      await tx.insert(key, value)
      const result = await tx.commit()
      if (!result.success) {
        throw new Error(`Transaction failed: ${result.error || 'Commit failed due to conflict'}`)
      }
    })
  }

  public async delete(key: K, value: V): Promise<void> {
    return this.writeLock(1, async () => {
      const tx = await this.createTransaction()
      await tx.delete(key, value)
      const result = await tx.commit()
      if (!result.success) {
        throw new Error(`Transaction failed: ${result.error || 'Commit failed due to conflict'}`)
      }
    })
  }
}
