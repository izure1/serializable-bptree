import type { SerializeStrategySync } from '../SerializeStrategySync'
import type { BPTreeNode, SerializeStrategyHead } from '../types'
import { SyncMVCCStrategy } from 'mvcc-api'

/**
 * MVCC Strategy for synchronous B+Tree operations.
 * Uses node ID as key and node data as value.
 */
export class BPTreeMVCCStrategySync<K, V, B extends BPTreeNode<K, V>> extends SyncMVCCStrategy<string, B> {
  constructor(private readonly strategy: SerializeStrategySync<K, V>) {
    super()
  }

  read(key: string): B {
    if (key === '__HEAD__') {
      return this.strategy.readHead() as unknown as B
    }
    return this.strategy.read(key) as B
  }

  write(key: string, value: B): void {
    if (key === '__HEAD__') {
      this.strategy.writeHead(value as unknown as SerializeStrategyHead)
    }
    else {
      this.strategy.write(key, value)
    }
  }

  delete(key: string): void {
    this.strategy.delete(key)
  }

  exists(key: string): boolean {
    if (key === '__HEAD__') {
      return this.strategy.readHead() !== null
    }
    try {
      const node = this.strategy.read(key)
      return node !== null && node !== undefined
    } catch {
      return false
    }
  }
}
