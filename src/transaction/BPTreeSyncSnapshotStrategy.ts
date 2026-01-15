import type {
  SerializeStrategyHead,
  BPTreeNode,
  Json
} from '../types'
import { SerializeStrategySync } from '../SerializeStrategySync'

export class BPTreeSyncSnapshotStrategy<K, V> extends SerializeStrategySync<K, V> {
  private readonly baseStrategy: SerializeStrategySync<K, V>
  private readonly snapshotHead: SerializeStrategyHead

  constructor(baseStrategy: SerializeStrategySync<K, V>, root: string) {
    super(baseStrategy.order)
    this.baseStrategy = baseStrategy
    this.snapshotHead = {
      ...baseStrategy.head,
      root: root,
      data: { ...baseStrategy.head.data }
    }
    // Directly override property from base class constructor
    this.head = this.snapshotHead
  }

  id(isLeaf: boolean): string {
    return this.baseStrategy.id(isLeaf)
  }

  read(id: string): BPTreeNode<K, V> {
    return this.baseStrategy.read(id)
  }

  write(id: string, node: BPTreeNode<K, V>): void {
    this.baseStrategy.write(id, node)
  }

  delete(id: string): void {
    this.baseStrategy.delete(id)
  }

  readHead(): SerializeStrategyHead | null {
    return this.snapshotHead
  }

  writeHead(head: SerializeStrategyHead): void {
    this.snapshotHead.root = head.root
    this.snapshotHead.data = { ...head.data }
  }

  compareAndSwapHead(oldRoot: string | null, newRoot: string): boolean {
    return this.baseStrategy.compareAndSwapHead(oldRoot, newRoot)
  }

  getHeadData(key: string, defaultValue: Json): Json {
    return this.snapshotHead.data[key] ?? defaultValue
  }

  setHeadData(key: string, data: Json): void {
    this.snapshotHead.data[key] = data
  }

  autoIncrement(key: string, defaultValue: number): number {
    return (this.snapshotHead.data[key] as number) ?? defaultValue
  }
}
