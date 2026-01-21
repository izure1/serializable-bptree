import { AsyncMVCCStrategy } from 'mvcc-api'
import type { SerializeStrategyAsync } from '../SerializeStrategyAsync'
import type { BPTreeUnknownNode } from '../types'

/**
 * MVCC Strategy for asynchronous B+Tree operations.
 * Uses node ID as key and node data as value.
 */
export class BPTreeMVCCStrategyAsync<K, V> extends AsyncMVCCStrategy<string, BPTreeUnknownNode<K, V> | null> {
  constructor(private readonly strategy: SerializeStrategyAsync<K, V>) {
    super()
  }

  async read(key: string): Promise<BPTreeUnknownNode<K, V> | null> {
    try {
      return await this.strategy.read(key) as BPTreeUnknownNode<K, V>
    } catch {
      return null
    }
  }

  async write(key: string, value: BPTreeUnknownNode<K, V> | null): Promise<void> {
    if (value === null) {
      await this.strategy.delete(key)
    } else {
      await this.strategy.write(key, value)
    }
  }

  async delete(key: string): Promise<void> {
    await this.strategy.delete(key)
  }

  async exists(key: string): Promise<boolean> {
    try {
      const node = await this.strategy.read(key)
      return node !== null && node !== undefined
    } catch {
      return false
    }
  }
}
