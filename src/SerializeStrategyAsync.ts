import { BPTreeNode } from './base/BPTree'
import { SerializeStrategy, SerializeStrategyHead } from './base/SerializeStrategy'
import { Json } from './utils/types'

export abstract class SerializeStrategyAsync<K, V> extends SerializeStrategy<K, V> {
  abstract id(isLeaf: boolean): Promise<string>
  abstract read(id: string): Promise<BPTreeNode<K, V>>
  abstract write(id: string, node: BPTreeNode<K, V>): Promise<void>
  abstract delete(id: string): Promise<void>
  abstract readHead(): Promise<SerializeStrategyHead|null>
  abstract writeHead(head: SerializeStrategyHead): Promise<void>

  async getHeadData(key: string, defaultValue: Json): Promise<Json> {
    if (!Object.hasOwn(this.head.data, key)) {
      await this.setHeadData(key, defaultValue)
    }
    return this.head.data[key]
  }

  async setHeadData(key: string, data: Json): Promise<void> {
    this.head.data[key] = data
    await this.writeHead(this.head)
  }

  async autoIncrement(key: string, defaultValue: number): Promise<number> {
    const current = await this.getHeadData(key, defaultValue) as number
    const next = current+1
    await this.setHeadData(key, next)
    return current
  }
}

export class InMemoryStoreStrategyAsync<K, V> extends SerializeStrategyAsync<K, V> {
  protected readonly node: Record<string, BPTreeNode<K, V>>

  constructor(order: number) {
    super(order)
    this.node = {}
  }

  async id(isLeaf: boolean): Promise<string> {
    return (await this.autoIncrement('index', 1)).toString()
  }

  async read(id: string): Promise<BPTreeNode<K, V>> {
    if (!Object.hasOwn(this.node, id)) {
      throw new Error(`The tree attempted to reference node '${id}', but couldn't find the corresponding node.`)
    }
    return this.node[id] as BPTreeNode<K, V>
  }

  async write(id: string, node: BPTreeNode<K, V>): Promise<void> {
    this.node[id] = node
  }

  async delete(id: string): Promise<void> {
    delete this.node[id]
  }

  async readHead(): Promise<SerializeStrategyHead|null> {
    if (this.head.root === null) {
      return null
    }
    return this.head
  }

  async writeHead(head: SerializeStrategyHead): Promise<void> {
    (this as any).head = head
  }
}
