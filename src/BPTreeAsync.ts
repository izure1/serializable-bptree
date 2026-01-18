import type { BPTreeConstructorOption } from './types'
import { SerializeStrategyAsync } from './SerializeStrategyAsync'
import { ValueComparator } from './base/ValueComparator'
import { BPTreeAsyncBase } from './base/BPTreeAsyncBase'
import { BPTreeAsyncTransaction } from './transaction/BPTreeAsyncTransaction'

export class BPTreeAsync<K, V> extends BPTreeAsyncBase<K, V> {
  constructor(
    strategy: SerializeStrategyAsync<K, V>,
    comparator: ValueComparator<V>,
    option?: BPTreeConstructorOption
  ) {
    super(strategy, comparator, option)
  }

  /**
   * Creates a new asynchronous transaction.
   * @returns A promise that resolves to a new BPTreeAsyncTransaction.
   */
  public async createTransaction(): Promise<BPTreeAsyncTransaction<K, V>> {
    const tx = new BPTreeAsyncTransaction(this)
    await tx.initTransaction()
    return tx
  }

  public async insert(key: K, value: V): Promise<void> {
    const tx = await this.createTransaction()
    await tx.insert(key, value)
    const { success } = await tx.commit()
    if (!success) {
      throw new Error('Transaction failed: Commit failed due to conflict')
    }
  }

  public async delete(key: K, value: V): Promise<void> {
    const tx = await this.createTransaction()
    await tx.delete(key, value)
    const { success } = await tx.commit()
    if (!success) {
      throw new Error('Transaction failed: Commit failed due to conflict')
    }
  }

  protected async readLock<T>(fn: () => Promise<T>): Promise<T> {
    return await fn()
  }

  protected async writeLock<T>(fn: () => Promise<T>): Promise<T> {
    return await fn()
  }
}
