import { BPTreeNode } from './base/BPTree'
import { SerializeStrategy, SerializeStrategyHead } from './base/SerializeStrategy'
import { Json } from './utils/types'

export abstract class SerializeStrategySync<K, V> extends SerializeStrategy<K, V> {
  abstract id(isLeaf: boolean): number
  abstract read(id: number): BPTreeNode<K, V>
  abstract write(id: number, node: BPTreeNode<K, V>): void
  abstract readHead(): SerializeStrategyHead|null
  abstract writeHead(head: SerializeStrategyHead): void

  getHeadData(key: string, defaultValue: Json): Json {
    if (!Object.hasOwn(this.head.data, key)) {
      return defaultValue
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
  protected readonly node: Record<number, BPTreeNode<K, V>>

  constructor(order: number) {
    super(order)
    this.node = {}
  }

  id(isLeaf: boolean): number {
    return this.autoIncrement('index', 1)
  }

  read(id: number): BPTreeNode<K, V> {
    if (!Object.hasOwn(this.node, id)) {
      throw new Error(`The tree attempted to reference node '${id}', but couldn't find the corresponding node.`)
    }
    return this.node[id] as BPTreeNode<K, V>
  }

  write(id: number, node: BPTreeNode<K, V>): void {
    this.node[id] = node
  }

  readHead(): SerializeStrategyHead|null {
    if (this.head.root === 0) {
      return null
    }
    return this.head
  }

  writeHead(head: SerializeStrategyHead): void {
    (this as any).head = head
  }
}
