import { BPTreeNode } from './base/BPTree'
import { SerializeStrategy, SerializeStrategyHead } from './base/SerializeStrategy'
import { Json } from './utils/types'

export abstract class SerializeStrategySync<K, V> extends SerializeStrategy<K, V> {
  abstract id(isLeaf: boolean): string
  abstract read(id: string): BPTreeNode<K, V>
  abstract write(id: string, node: BPTreeNode<K, V>): void
  abstract delete(id: string): void
  abstract readHead(): SerializeStrategyHead|null
  abstract writeHead(head: SerializeStrategyHead): void

  getHeadData(key: string, defaultValue: Json): Json {
    if (!Object.hasOwn(this.head.data, key)) {
      this.setHeadData(key, defaultValue)
    }
    return this.head.data[key]
  }

  setHeadData(key: string, data: Json): void {
    this.head.data[key] = data
    this.writeHead(this.head)
  }

  autoIncrement(key: string, defaultValue: number): number {
    const current = this.getHeadData(key, defaultValue) as number
    const next = current+1
    this.setHeadData(key, next)
    return current
  }
}

export class InMemoryStoreStrategySync<K, V> extends SerializeStrategySync<K, V> {
  protected readonly node: Record<string, BPTreeNode<K, V>>

  constructor(order: number) {
    super(order)
    this.node = {}
  }

  id(isLeaf: boolean): string {
    return this.autoIncrement('index', 1).toString()
  }

  read(id: string): BPTreeNode<K, V> {
    if (!Object.hasOwn(this.node, id)) {
      throw new Error(`The tree attempted to reference node '${id}', but couldn't find the corresponding node.`)
    }
    return this.node[id] as BPTreeNode<K, V>
  }

  write(id: string, node: BPTreeNode<K, V>): void {
    this.node[id] = node
  }

  delete(id: string): void {
    delete this.node[id]
  }

  readHead(): SerializeStrategyHead|null {
    if (this.head.root === null) {
      return null
    }
    return this.head
  }

  writeHead(head: SerializeStrategyHead): void {
    (this as any).head = head
  }
}
