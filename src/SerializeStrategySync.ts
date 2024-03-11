import { BPTreeNode } from './base/BPTree'
import { SerializeStrategy, SerializeStrategyHead } from './base/SerializeStrategy'

export abstract class SerializeStrategySync<K, V> extends SerializeStrategy<K, V> {
  abstract id(): number
  abstract read(id: number): BPTreeNode<K, V>
  abstract write(id: number, node: BPTreeNode<K, V>): void
  abstract readHead(): SerializeStrategyHead|null
  abstract writeHead(head: SerializeStrategyHead): void
}

export class InMemoryStoreStrategySync<K, V> extends SerializeStrategySync<K, V> {
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

  id(): number {
    return Math.ceil(Math.random()*Number.MAX_SAFE_INTEGER-1)
  }

  read(id: number): BPTreeNode<K, V> {
    if (!Object.prototype.hasOwnProperty.call(this.data.node, id)) {
      throw new Error(`The tree attempted to reference node '${id}', but couldn't find the corresponding node.`)
    }
    return this.data.node[id]
  }

  write(id: number, node: BPTreeNode<K, V>): void {
    this.data.node[id] = node
  }

  readHead(): SerializeStrategyHead|null {
    return this.data.head
  }

  writeHead(head: SerializeStrategyHead): void {
    this.data.head = head
  }
}
