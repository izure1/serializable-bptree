import { CacheStorage } from '../utils/CacheStorage'
import { ValueComparator } from './ValueComparator'
import { SerializableData, SerializeStrategy, SerializeStrategyHead } from './SerializeStrategy'

type Sync<T> = T
type Async<T> = Promise<T>
type Deferred<T> = Sync<T>|Async<T>

export type BPTreeNodeKey<K> = number|K
export type BPTreeCondition<V> = Partial<{
  /** Searches for pairs greater than the given value. */
  gt: Partial<V>
  /** Searches for pairs less than the given value. */
  lt: Partial<V>
  /** Searches for pairs greater than or equal to the given value. */
  gte: Partial<V>
  /** Searches for pairs less than or equal to the given value. */
  lte: Partial<V>
  /** "Searches for pairs equal to the given value. */
  equal: Partial<V>
  /** Searches for pairs not equal to the given value. */
  notEqual: Partial<V>
  /** Searches for values matching the given pattern. '%' matches zero or more characters, and '_' matches exactly one character. */
  like: Partial<V>
}>
export type BPTreePair<K, V> = { key: K, value: V }

export type BPTreeUnknownNode<K, V> = BPTreeInternalNode<K, V>|BPTreeLeafNode<K, V>

export interface BPTreeNode<K, V> {
  id: number
  keys: number[]|K[][],
  values: V[],
  leaf: boolean
  parent: number
  next: number
  prev: number
}

export interface BPTreeInternalNode<K, V> extends BPTreeNode<K, V> {
  leaf: false
  keys: number[]
}

export interface BPTreeLeafNode<K, V> extends BPTreeNode<K, V> {
  leaf: true
  keys: K[][]
}

export abstract class BPTree<K, V> {
  private readonly _regexpCache: CacheStorage<string, RegExp>

  protected readonly strategy: SerializeStrategy<K, V>
  protected readonly comparator: ValueComparator<V>
  protected readonly nodes: Map<number, BPTreeUnknownNode<K, V>>
  protected order!: number
  protected root!: BPTreeUnknownNode<K, V>

  protected readonly _nodeCreateBuffer: Map<number, BPTreeUnknownNode<K, V>>
  protected readonly _nodeUpdateBuffer: Map<number, BPTreeUnknownNode<K, V>>

  protected readonly verifierMap: Record<
    keyof BPTreeCondition<V>,
    (nodeValue: V, value: V) => boolean
  > = {
    gt: (nv, v) => this.comparator.isHigher(nv, v),
    gte: (nv, v) => this.comparator.isHigher(nv, v) || this.comparator.isSame(nv, v),
    lt: (nv, v) => this.comparator.isLower(nv, v),
    lte: (nv, v) => this.comparator.isLower(nv, v) || this.comparator.isSame(nv, v),
    equal: (nv, v) => this.comparator.isSame(nv, v),
    notEqual: (nv, v) => this.comparator.isSame(nv, v) === false,
    like: (nv, v) => {
      const nodeValue = this.comparator.match(nv)
      const value = this.comparator.match(v)
      const regexp = this._regexpCache.ensure(value, () => {
        const pattern = value.replace(/%/g, '.*').replace(/_/g, '.')
        return new RegExp(`^${pattern}$`, 'i')
      })
      return regexp.test(nodeValue)
    },
  }

  protected readonly verifierStartNode: Record<
    keyof BPTreeCondition<V>,
    (value: V) => Deferred<BPTreeLeafNode<K, V>>
  > = {
    gt: (v) => this.insertableNode(v),
    gte: (v) => this.insertableNode(v),
    lt: (v) => this.insertableNode(v),
    lte: (v) => this.insertableNode(v),
    equal: (v) => this.insertableNode(v),
    notEqual: (v) => this.leftestNode(),
    like: (v) => this.leftestNode(),
  }
  
  protected readonly verifierDirection: Record<keyof BPTreeCondition<V>, -1|1> = {
    gt: 1,
    gte: 1,
    lt: -1,
    lte: -1,
    equal: 1,
    notEqual: 1,
    like: 1,
  }

  protected readonly verifierFullScan: Record<keyof BPTreeCondition<V>, boolean> = {
    gt: false,
    gte: false,
    lt: false,
    lte: false,
    equal: false,
    notEqual: true,
    like: true,
  }

  protected constructor(strategy: SerializeStrategy<K, V>, comparator: ValueComparator<V>) {
    this._regexpCache = new CacheStorage()
    this._nodeCreateBuffer = new Map()
    this._nodeUpdateBuffer = new Map()
    this.nodes = new Map()
    this.strategy = strategy
    this.comparator = comparator
  }

  protected abstract _getPairsRightToLeft(
    value: V,
    startNode: BPTreeLeafNode<K, V>,
    fullSearch: boolean,
    comparator: (nodeValue: V, value: V) => boolean
  ): Deferred<BPTreePair<K, V>[]>
  protected abstract _getPairsLeftToRight(
    value: V,
    startNode: BPTreeLeafNode<K, V>,
    fullSearch: boolean,
    comparator: (nodeValue: V, value: V) => boolean
  ): Deferred<BPTreePair<K, V>[]>
  protected abstract getPairs(
    value: V,
    startNode: BPTreeLeafNode<K, V>,
    fullSearch: boolean,
    comparator: (nodeValue: V, value: V) => boolean,
    direction: -1|1
  ): Deferred<BPTreePair<K, V>[]>
  protected abstract _createNodeId(): Deferred<number>
  protected abstract _createNode(
    keys: number[]|K[][],
    values: V[],
    leaf?: boolean,
    parent?: number,
    next?: number,
    prev?: number
  ): Deferred<BPTreeUnknownNode<K, V>>
  protected abstract _deleteEntry(node: BPTreeUnknownNode<K, V>, key: BPTreeNodeKey<K>, value: V): Deferred<void>
  protected abstract _insertInParent(node: BPTreeUnknownNode<K, V>, value: V, pointer: BPTreeUnknownNode<K, V>): Deferred<void>
  protected abstract getNode(id: number): Deferred<BPTreeUnknownNode<K, V>>
  protected abstract insertableNode(value: V): Deferred<BPTreeLeafNode<K, V>>
  protected abstract leftestNode(): Deferred<BPTreeLeafNode<K, V>>
  protected abstract commitHeadBuffer(): Deferred<void>
  protected abstract commitNodeCreateBuffer(): Deferred<void>
  protected abstract commitNodeUpdateBuffer(): Deferred<void>
  
  /**
   * After creating a tree instance, it must be called.  
   * This method is used to initialize the stored tree and recover data.
   * If it is not called, the tree will not function.
   */
  abstract init(): Deferred<void>
  /**
   * It searches for a key within the tree. The result is returned as an array sorted in ascending order based on the value.  
   * The result is key set instance, and you can use the `gt`, `lt`, `gte`, `lte`, `equal`, `notEqual`, `like` condition statements.
   * This method operates much faster than first searching with `where` and then retrieving only the key list.
   * @param condition You can use the `gt`, `lt`, `gte`, `lte`, `equal`, `notEqual`, `like` condition statements.
   */
  public abstract keys(condition: BPTreeCondition<V>): Deferred<Set<K>>
  /**
   * It searches for a value within the tree. The result is returned as an array sorted in ascending order based on the value.  
   * The result includes the key and value attributes, and you can use the `gt`, `lt`, `gte`, `lte`, `equal`, `notEqual`, `like` condition statements.
   * @param condition You can use the `gt`, `lt`, `gte`, `lte`, `equal`, `notEqual`, `like` condition statements.
   */
  public abstract where(condition: BPTreeCondition<V>): Deferred<BPTreePair<K, V>[]>
  /**
   * You enter the key and value as a pair. You can later search for the pair by value.
   * This data is stored in the tree, sorted in ascending order of value.
   * @param key The key of the pair.
   * @param value The value of the pair.
   */
  public abstract insert(key: K, value: V): Deferred<void>
  /**
   * Deletes the pair that matches the key and value.
   * @param key The key of the pair.
   * @param value The value of the pair.
   */
  public abstract delete(key: K, value: V): Deferred<void>
  /**
   * It returns whether there is a value in the tree.
   * @param key The key value to search for.
   * @param value The value to search for.
   */
  public abstract exists(key: K, value: V): Deferred<boolean>
   /**
   * Inserts user-defined data into the head of the tree.
   * This feature is useful when you need to store separate, non-volatile information in the tree.
   * For example, you can store information such as the last update time and the number of insertions.
   * @param data User-defined data to be stored in the head of the tree.
   */
  public abstract setHeadData(data: SerializableData): Deferred<void>
  /**
   * This method deletes nodes cached in-memory and caches new nodes from the stored nodes.  
   * Typically, there's no need to use this method, but it can be used to synchronize data in scenarios where the remote storage and the client are in a 1:n relationship.
   * @returns The return value is the total number of nodes updated.
   */
  public abstract forceUpdate(nodeId: number): Deferred<number>

  protected _insertAtLeaf(node: BPTreeLeafNode<K, V>, key: K, value: V): void {
    if (node.values.length) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        if (this.comparator.isSame(value, nValue)) {
          const keys = node.keys[i]
          keys.push(key)
          this.bufferForNodeUpdate(node)
          break
        }
        else if (this.comparator.isLower(value, nValue)) {
          node.values.splice(i, 0, value)
          node.keys.splice(i, 0, [key])
          this.bufferForNodeUpdate(node)
          break
        }
        else if (i+1 === node.values.length) {
          node.values.push(value)
          node.keys.push([key])
          this.bufferForNodeUpdate(node)
          break
        }
      }
    }
    else {
      node.values = [value]
      node.keys = [[key]]
      this.bufferForNodeUpdate(node)
    }
  }

  protected bufferForNodeCreate(node: BPTreeUnknownNode<K, V>): Deferred<void> {
    this._nodeCreateBuffer.set(node.id, node)
  }

  protected bufferForNodeUpdate(node: BPTreeUnknownNode<K, V>): Deferred<void> {
    this._nodeUpdateBuffer.set(node.id, node)
  }

  /**
   * Returns the user-defined data stored in the head of the tree.
   * This value can be set using the `setHeadData` method. If no data has been previously inserted, the default value is returned, and the default value is `{}`.
   * @returns User-defined data stored in the head of the tree.
   */
  getHeadData(): SerializableData {
    return this.strategy.head.data
  }
}
