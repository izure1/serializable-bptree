import type { SerializeStrategyAsync } from '../SerializeStrategyAsync'
import type { BPTreeNode, SerializeStrategyHead } from '../types'
import { AsyncMVCCStrategy } from 'mvcc-api'

/**
 * MVCC Strategy for synchronous B+Tree operations.
 * Uses node ID as key and node data as value.
 */
export class BPTreeMVCCStrategyAsync<K, V, B extends BPTreeNode<K, V>> extends AsyncMVCCStrategy<string, B> {
  constructor(private readonly strategy: SerializeStrategyAsync<K, V>) {
    super()
  }

  async read(key: string): Promise<B> {
    if (key === '__HEAD__') {
      return await this.strategy.readHead() as unknown as Promise<B>
    }
    return await this.strategy.read(key) as unknown as Promise<B>
  }

  async write(key: string, value: B): Promise<void> {
    if (key === '__HEAD__') {
      await this.strategy.writeHead(value as unknown as SerializeStrategyHead)
    }
    else {
      await this.strategy.write(key, value)
    }
  }

  async delete(key: string): Promise<void> {
    await this.strategy.delete(key)
  }

  async exists(key: string): Promise<boolean> {
    if (key === '__HEAD__') {
      return await this.strategy.readHead() !== null
    }
    try {
      const node = await this.strategy.read(key)
      return node !== null && node !== undefined
    } catch {
      return false
    }
  }
}
