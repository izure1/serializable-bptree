import type {
  SerializeStrategyHead,
  BPTreeNode,
  Json
} from '../types'
import { SerializeStrategyAsync } from '../SerializeStrategyAsync'

export class BPTreeAsyncSnapshotStrategy<K, V> extends SerializeStrategyAsync<K, V> {
  private readonly baseStrategy: SerializeStrategyAsync<K, V>
  private readonly snapshotHead: SerializeStrategyHead

  constructor(baseStrategy: SerializeStrategyAsync<K, V>, root: string) {
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

  async id(isLeaf: boolean): Promise<string> {
    return await this.baseStrategy.id(isLeaf)
  }

  async read(id: string): Promise<BPTreeNode<K, V>> {
    return await this.baseStrategy.read(id)
  }

  async write(id: string, node: BPTreeNode<K, V>): Promise<void> {
    await this.baseStrategy.write(id, node)
  }

  async delete(id: string): Promise<void> {
    await this.baseStrategy.delete(id)
  }

  async readHead(): Promise<SerializeStrategyHead | null> {
    return this.snapshotHead
  }

  async writeHead(head: SerializeStrategyHead): Promise<void> {
    this.snapshotHead.root = head.root
    this.snapshotHead.data = { ...head.data }
  }

  async compareAndSwapHead(oldRoot: string | null, newRoot: string): Promise<boolean> {
    return await this.baseStrategy.compareAndSwapHead(oldRoot, newRoot)
  }

  async getHeadData(key: string, defaultValue: Json): Promise<Json> {
    return this.snapshotHead.data[key] ?? defaultValue
  }

  async setHeadData(key: string, data: Json): Promise<void> {
    this.snapshotHead.data[key] = data
  }

  async autoIncrement(key: string, defaultValue: number): Promise<number> {
    return (this.snapshotHead.data[key] as number) ?? defaultValue
  }
}
