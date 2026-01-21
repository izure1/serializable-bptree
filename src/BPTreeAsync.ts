import type { BPTreeConstructorOption, BPTreeUnknownNode } from './types'
import { AsyncMVCCTransaction } from 'mvcc-api'
import { SerializeStrategyAsync } from './SerializeStrategyAsync'
import { ValueComparator } from './base/ValueComparator'
import { BPTreeAsyncBase } from './base/BPTreeAsyncBase'
import { BPTreeAsyncTransaction } from './transaction/BPTreeAsyncTransaction'
import { BPTreeMVCCStrategyAsync } from './transaction/BPTreeMVCCStrategyAsync'

export class BPTreeAsync<K, V> extends BPTreeAsyncBase<K, V> {
  public readonly mvccRoot: AsyncMVCCTransaction<BPTreeMVCCStrategyAsync<K, V>, string, BPTreeUnknownNode<K, V> | null>

  constructor(
    strategy: SerializeStrategyAsync<K, V>,
    comparator: ValueComparator<V>,
    option?: BPTreeConstructorOption
  ) {
    super(strategy, comparator, option)
    this.mvccRoot = new AsyncMVCCTransaction(new BPTreeMVCCStrategyAsync(strategy))
  }

  /**
   * Creates a new asynchronous transaction.
   * @returns A promise that resolves to a new BPTreeAsyncTransaction.
   */
  public async createTransaction(): Promise<BPTreeAsyncTransaction<K, V>> {
    const nestedTx = this.mvccRoot.createNested()
    const tx = new BPTreeAsyncTransaction(this, nestedTx)
    await tx.initTransaction()
    return tx
  }

  public async insert(key: K, value: V): Promise<void> {
    const tx = await this.createTransaction()
    await tx.insert(key, value)
    const { success, error } = await tx.commit()
    if (!success) {
      throw new Error(`Transaction failed: ${error || 'Commit failed due to conflict'}`)
    }
  }

  public async applyCommit(rootId: string, order: number, changes: { created: string[], deleted: string[], updated: string[] }): Promise<void> {
    super.applyCommit(rootId, order, changes)
    await this.strategy.writeHead(this.strategy.head)
  }

  public async delete(key: K, value: V): Promise<void> {
    const tx = await this.createTransaction()
    await tx.delete(key, value)
    const { success, error } = await tx.commit()
    if (!success) {
      throw new Error(`Transaction failed: ${error || 'Commit failed due to conflict'}`)
    }
  }

  protected async readLock<T>(fn: () => Promise<T>): Promise<T> {
    return await fn()
  }

  protected async writeLock<T>(fn: () => Promise<T>): Promise<T> {
    return await fn()
  }
}
