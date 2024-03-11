import { BPTreeNode } from './base/BPTree'
import { SerializeStrategy, SerializeStrategyHead } from './base/SerializeStrategy'

export abstract class SerializeStrategyAsync<K, V> extends SerializeStrategy<K, V> {
  abstract id(): Promise<number>
  abstract read(id: number): Promise<BPTreeNode<K, V>>
  abstract write(id: number, node: BPTreeNode<K, V>): Promise<void>
  abstract readHead(): Promise<SerializeStrategyHead|null>
  abstract writeHead(head: SerializeStrategyHead): Promise<void>
}

export class InMemoryStoreStrategyAsync<K, V> extends SerializeStrategyAsync<K, V> {
  protected readonly data: {
    head: SerializeStrategyHead|null,
    node: Record<number, BPTreeNode<K, V>>
  }

  constructor(order: number) {
    super(order)
    this.data = {
      head: null,
      node: {}
    }
  }

  async id(): Promise<number> {
    return Math.ceil(Math.random()*Number.MAX_SAFE_INTEGER-1)
  }

  async read(id: number): Promise<BPTreeNode<K, V>> {
    if (!Object.prototype.hasOwnProperty.call(this.data.node, id)) {
      throw new Error(`The tree attempted to reference node '${id}', but couldn't find the corresponding node.`)
    }
    return this.data.node[id]
  }

  async write(id: number, node: BPTreeNode<K, V>): Promise<void> {
    this.data.node[id] = node
  }

  async readHead(): Promise<SerializeStrategyHead|null> {
    return this.data.head
  }

  async writeHead(head: SerializeStrategyHead): Promise<void> {
    this.data.head = head
  }
}
