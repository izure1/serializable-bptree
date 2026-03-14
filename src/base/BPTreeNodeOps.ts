import type {
  BPTreeUnknownNode,
  SerializableData,
  SerializeStrategyHead,
} from '../types'

/**
 * Abstraction for B+Tree node access operations.
 * Both MVCC-based (BPTreeSyncTransaction) and strategy-direct (BPTreePure)
 * implementations provide this interface to the shared algorithm functions.
 */
export interface BPTreeNodeOps<K, V> {
  getNode(id: string): BPTreeUnknownNode<K, V>
  createNode(
    leaf: boolean,
    keys: string[] | K[][],
    values: V[],
    parent?: string | null,
    next?: string | null,
    prev?: string | null,
  ): BPTreeUnknownNode<K, V>
  updateNode(node: BPTreeUnknownNode<K, V>): void
  deleteNode(node: BPTreeUnknownNode<K, V>): void
  readHead(): SerializeStrategyHead | null
  writeHead(head: SerializeStrategyHead): void
}

/**
 * Mutable algorithm context passed to B+Tree algorithm functions.
 * `rootId` is mutable because tree mutations (insert, delete) may change the root.
 */
export interface BPTreeAlgoContext<K, V> {
  rootId: string
  order: number
  readonly headData: () => SerializableData
}

/**
 * Async version of BPTreeNodeOps for async B+Tree operations.
 */
export interface BPTreeNodeOpsAsync<K, V> {
  getNode(id: string): Promise<BPTreeUnknownNode<K, V>>
  createNode(
    leaf: boolean,
    keys: string[] | K[][],
    values: V[],
    parent?: string | null,
    next?: string | null,
    prev?: string | null,
  ): Promise<BPTreeUnknownNode<K, V>>
  updateNode(node: BPTreeUnknownNode<K, V>): Promise<void>
  deleteNode(node: BPTreeUnknownNode<K, V>): Promise<void>
  readHead(): Promise<SerializeStrategyHead | null>
  writeHead(head: SerializeStrategyHead): Promise<void>
}

