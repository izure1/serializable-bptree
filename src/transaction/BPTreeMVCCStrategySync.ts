import { SyncMVCCStrategy } from 'mvcc-api'
import type { SerializeStrategySync } from '../SerializeStrategySync'
import type { BPTreeUnknownNode } from '../types'

/**
 * MVCC Strategy for synchronous B+Tree operations.
 * Uses node ID as key and node data as value.
 */
export class BPTreeMVCCStrategySync<K, V> extends SyncMVCCStrategy<string, BPTreeUnknownNode<K, V> | null> {
  constructor(private readonly strategy: SerializeStrategySync<K, V>) {
    super()
  }

  read(key: string): BPTreeUnknownNode<K, V> | null {
    try {
      return this.strategy.read(key) as BPTreeUnknownNode<K, V>
    } catch {
      return null
    }
  }

  write(key: string, value: BPTreeUnknownNode<K, V> | null): void {
    if (value === null) {
      this.strategy.delete(key)
    } else {
      this.strategy.write(key, value)
    }
  }

  delete(key: string): void {
    this.strategy.delete(key)
  }

  exists(key: string): boolean {
    try {
      const node = this.strategy.read(key)
      return node !== null && node !== undefined
    } catch {
      return false
    }
  }
}
