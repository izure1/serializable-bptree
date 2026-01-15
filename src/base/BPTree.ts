import type { BPTreeCondition, BPTreeConstructorOption, BPTreeUnknownNode, Deferred, BPTreeLeafNode, BPTreeNodeKey, BPTreePair, SerializableData, BPTreeTransactionResult } from '../types'
import { CacheEntanglementSync, CacheEntanglementAsync } from 'cache-entanglement'
import { ValueComparator } from './ValueComparator'
import { SerializeStrategy } from './SerializeStrategy'

export abstract class BPTree<K, V> {
  private readonly _cachedRegexp: ReturnType<typeof this._createCachedRegexp>
  protected abstract readonly nodes: CacheEntanglementSync<any, any> | CacheEntanglementAsync<any, any>

  protected readonly strategy: SerializeStrategy<K, V>
  protected readonly comparator: ValueComparator<V>
  protected readonly option: BPTreeConstructorOption
  protected order!: number
  protected rootId!: string

  /**
   * Returns the ID of the root node.
   * @returns The root node ID.
   */
  public getRootId(): string {
    return this.rootId
  }

  /**
   * Returns the order of the B+Tree.
   * @returns The order of the tree.
   */
  public getOrder(): number {
    return this.order
  }

  protected _strategyDirty: boolean
  protected readonly _nodeCreateBuffer: Map<string, BPTreeUnknownNode<K, V>>
  protected readonly _nodeUpdateBuffer: Map<string, BPTreeUnknownNode<K, V>>
  protected readonly _nodeDeleteBuffer: Map<string, BPTreeUnknownNode<K, V>>

  protected readonly verifierMap: Record<
    keyof BPTreeCondition<V>,
    (nodeValue: V, value: V | V[]) => boolean
  > = {
      gt: (nv, v) => this.comparator.isHigher(nv, v as V),
      gte: (nv, v) => this.comparator.isHigher(nv, v as V) || this.comparator.isSame(nv, v as V),
      lt: (nv, v) => this.comparator.isLower(nv, v as V),
      lte: (nv, v) => this.comparator.isLower(nv, v as V) || this.comparator.isSame(nv, v as V),
      equal: (nv, v) => this.comparator.isSame(nv, v as V),
      notEqual: (nv, v) => this.comparator.isSame(nv, v as V) === false,
      or: (nv, v) => this.ensureValues(v).some((v) => this.comparator.isSame(nv, v)),
      primaryGt: (nv, v) => this.comparator.isPrimaryHigher(nv, v as V),
      primaryGte: (nv, v) => this.comparator.isPrimaryHigher(nv, v as V) || this.comparator.isPrimarySame(nv, v as V),
      primaryLt: (nv, v) => this.comparator.isPrimaryLower(nv, v as V),
      primaryLte: (nv, v) => this.comparator.isPrimaryLower(nv, v as V) || this.comparator.isPrimarySame(nv, v as V),
      primaryEqual: (nv, v) => this.comparator.isPrimarySame(nv, v as V),
      primaryNotEqual: (nv, v) => this.comparator.isPrimarySame(nv, v as V) === false,
      primaryOr: (nv, v) => this.ensureValues(v).some((v) => this.comparator.isPrimarySame(nv, v)),
      like: (nv, v) => {
        const nodeValue = this.comparator.match(nv)
        const value = v as unknown as string
        const cache = this._cachedRegexp.cache(value)
        const regexp = cache.raw
        return regexp.test(nodeValue)
      },
    }

  protected readonly verifierStartNode: Record<
    keyof BPTreeCondition<V>,
    (value: V) => Deferred<BPTreeLeafNode<K, V>>
  > = {
      gt: (v) => this.insertableNodeByPrimary(v),
      gte: (v) => this.insertableNodeByPrimary(v),
      lt: (v) => this.insertableNodeByPrimary(v),
      lte: (v) => this.insertableRightestNodeByPrimary(v),
      equal: (v) => this.insertableNodeByPrimary(v),
      notEqual: (v) => this.leftestNode(),
      or: (v) => this.insertableNodeByPrimary(this.lowestPrimaryValue(this.ensureValues(v))),
      primaryGt: (v) => this.insertableNodeByPrimary(v),
      primaryGte: (v) => this.insertableNodeByPrimary(v),
      primaryLt: (v) => this.insertableNodeByPrimary(v),
      primaryLte: (v) => this.insertableRightestNodeByPrimary(v),
      primaryEqual: (v) => this.insertableNodeByPrimary(v),
      primaryNotEqual: (v) => this.leftestNode(),
      primaryOr: (v) => this.insertableNodeByPrimary(this.lowestPrimaryValue(this.ensureValues(v))),
      like: (v) => this.leftestNode(),
    }

  protected readonly verifierEndNode: Record<
    keyof BPTreeCondition<V>,
    (value: V) => Deferred<BPTreeLeafNode<K, V> | null>
  > = {
      gt: (v) => null,
      gte: (v) => null,
      lt: (v) => null,
      lte: (v) => null,
      equal: (v) => this.insertableEndNode(v, this.verifierDirection.equal),
      notEqual: (v) => null,
      or: (v) => this.insertableEndNode(
        this.highestValue(this.ensureValues(v)),
        this.verifierDirection.or
      ),
      primaryGt: (v) => null,
      primaryGte: (v) => null,
      primaryLt: (v) => null,
      primaryLte: (v) => null,
      primaryEqual: (v) => this.insertableRightestEndNodeByPrimary(v),
      primaryNotEqual: (v) => null,
      primaryOr: (v) => this.insertableRightestEndNodeByPrimary(
        this.highestPrimaryValue(this.ensureValues(v))
      ),
      like: (v) => null,
    }

  protected readonly verifierDirection: Record<keyof BPTreeCondition<V>, -1 | 1> = {
    gt: 1,
    gte: 1,
    lt: -1,
    lte: -1,
    equal: 1,
    notEqual: 1,
    or: 1,
    primaryGt: 1,
    primaryGte: 1,
    primaryLt: -1,
    primaryLte: -1,
    primaryEqual: 1,
    primaryNotEqual: 1,
    primaryOr: 1,
    like: 1,
  }

  /**
   * Determines whether early termination is allowed for each condition.
   * When true, the search will stop once a match is found and then a non-match is encountered.
   * Only applicable for conditions that guarantee contiguous matches in a sorted B+Tree.
   */
  protected readonly verifierEarlyTerminate: Record<keyof BPTreeCondition<V>, boolean> = {
    gt: false,
    gte: false,
    lt: false,
    lte: false,
    equal: true,
    notEqual: false,
    or: false,
    primaryGt: false,
    primaryGte: false,
    primaryLt: false,
    primaryLte: false,
    primaryEqual: true,
    primaryNotEqual: false,
    primaryOr: false,
    like: false,
  }

  /**
   * Priority map for condition types.
   * Higher value = higher selectivity (fewer expected results).
   * Used by `chooseDriver` to select the most selective index.
   */
  protected static readonly conditionPriority: Record<keyof BPTreeCondition<unknown>, number> = {
    equal: 100,
    primaryEqual: 100,
    or: 80,
    primaryOr: 80,
    gt: 50,
    gte: 50,
    lt: 50,
    lte: 50,
    primaryGt: 50,
    primaryGte: 50,
    primaryLt: 50,
    primaryLte: 50,
    like: 30,
    notEqual: 10,
    primaryNotEqual: 10,
  }

  /**
   * Selects the best driver tree from multiple tree/condition pairs.
   * Uses rule-based optimization to choose the tree with highest estimated selectivity.
   * 
   * @param candidates Array of { tree, condition } pairs to evaluate
   * @returns The candidate with highest priority condition, or null if empty
   * 
   * @example
   * ```typescript
   * const driver = BPTreeSync.chooseDriver([
   *   { tree: idxId, condition: { equal: 100 } },
   *   { tree: idxAge, condition: { gt: 20 } }
   * ])
   * // Returns { tree: idxId, condition: { equal: 100 } } because 'equal' has higher priority
   * ```
   */
  static ChooseDriver<T>(
    candidates: Array<{ tree: T, condition: BPTreeCondition<unknown> }>
  ): { tree: T, condition: BPTreeCondition<unknown> } | null {
    if (candidates.length === 0) return null
    if (candidates.length === 1) return candidates[0]

    let best = candidates[0]
    let bestScore = 0

    for (const candidate of candidates) {
      let score = 0
      for (const key in candidate.condition) {
        const condKey = key as keyof BPTreeCondition<unknown>
        const priority = BPTree.conditionPriority[condKey] ?? 0
        if (priority > score) {
          score = priority
        }
      }
      if (score > bestScore) {
        bestScore = score
        best = candidate
      }
    }

    return best
  }

  /**
   * Verified if the value satisfies the condition.
   * 
   * @param nodeValue The value to verify.
   * @param condition The condition to verify against.
   * @returns Returns true if the value satisfies the condition.
   */
  public verify(nodeValue: V, condition: BPTreeCondition<V>): boolean {
    for (const key in condition) {
      const verify = this.verifierMap[key as keyof BPTreeCondition<V>]
      const condValue = condition[key as keyof BPTreeCondition<V>] as V
      if (!verify(nodeValue, condValue)) {
        return false
      }
    }
    return true
  }

  protected constructor(
    strategy: SerializeStrategy<K, V>,
    comparator: ValueComparator<V>,
    option?: BPTreeConstructorOption
  ) {
    this.strategy = strategy
    this.comparator = comparator
    this.option = option ?? {}
    this._strategyDirty = false
    this._nodeCreateBuffer = new Map()
    this._nodeUpdateBuffer = new Map()
    this._nodeDeleteBuffer = new Map()
    this._cachedRegexp = this._createCachedRegexp()
  }

  private _createCachedRegexp() {
    return new CacheEntanglementSync((key) => {
      const pattern = key.replace(/%/g, '.*').replace(/_/g, '.')
      const regexp = new RegExp(`^${pattern}$`, 'i')
      return regexp
    }, {
      capacity: this.option.capacity ?? 1000
    })
  }

  protected abstract _createNodeId(isLeaf: boolean): Deferred<string>
  protected abstract _createNode(
    isLeaf: boolean,
    keys: string[] | K[][],
    values: V[],
    leaf?: boolean,
    parent?: string | null,
    next?: string | null,
    prev?: string | null
  ): Deferred<BPTreeUnknownNode<K, V>>
  protected abstract _deleteEntry(node: BPTreeUnknownNode<K, V>, key: BPTreeNodeKey<K>, value: V): Deferred<void>
  protected abstract _insertInParent(node: BPTreeUnknownNode<K, V>, value: V, pointer: BPTreeUnknownNode<K, V>): Deferred<void>
  protected abstract getNode(id: string): Deferred<BPTreeUnknownNode<K, V>>
  protected abstract insertableNode(value: V): Deferred<BPTreeLeafNode<K, V>>
  protected abstract insertableNodeByPrimary(value: V): Deferred<BPTreeLeafNode<K, V>>
  protected abstract insertableRightestNodeByPrimary(value: V): Deferred<BPTreeLeafNode<K, V>>
  protected abstract insertableRightestEndNodeByPrimary(value: V): Deferred<BPTreeLeafNode<K, V> | null>
  protected abstract insertableEndNode(value: V, direction: 1 | -1): Deferred<BPTreeLeafNode<K, V> | null>
  protected abstract leftestNode(): Deferred<BPTreeLeafNode<K, V>>
  protected abstract rightestNode(): Deferred<BPTreeLeafNode<K, V>>
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
   * @param filterValues The `Set` containing values to check for intersection.
   * Returns a `Set` containing values that are common to both the input `Set` and the intersection `Set`.
   * If this parameter is not provided, it searches for all keys inserted into the tree.
   */
  public abstract keys(condition: BPTreeCondition<V>, filterValues?: Set<K>): Deferred<Set<K>>
  /**
   * It searches for a value within the tree. The result is returned as an array sorted in ascending order based on the value.  
   * The result includes the key and value attributes, and you can use the `gt`, `lt`, `gte`, `lte`, `equal`, `notEqual`, `like` condition statements.
   * @param condition You can use the `gt`, `lt`, `gte`, `lte`, `equal`, `notEqual`, `like` condition statements.
   */
  public abstract where(condition: BPTreeCondition<V>): Deferred<BPTreePair<K, V>>
  /**
   * You enter the key and value as a pair. You can later search for the pair by value.
   * This data is stored in the tree, sorted in ascending order of value.
   * @param key The key of the pair. This key must be unique.
   * @param value The value of the pair.
   */
  public abstract insert(key: K, value: V): Deferred<void>
  /**
   * Deletes the pair that matches the key and value.
   * @param key The key of the pair. This key must be unique.
   * @param value The value of the pair.
   */
  public abstract delete(key: K, value: V): Deferred<void>
  /**
   * It returns whether there is a value in the tree.
   * @param key The key value to search for. This key must be unique.
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
   * If you do not specify an ID, all nodes will be updated.
   * @param id The ID of the node to update.
   * @returns The return value is the total number of nodes updated.
   */
  public abstract forceUpdate(id?: string): Deferred<number>

  protected ensureValues(v: V | V[]): V[] {
    if (!Array.isArray(v)) {
      v = [v]
    }
    return v
  }

  protected lowestValue(v: V[]): V {
    const i = 0
    return [...v].sort((a, b) => this.comparator.asc(a, b))[i]
  }

  protected highestValue(v: V[]): V {
    const i = v.length - 1
    return [...v].sort((a, b) => this.comparator.asc(a, b))[i]
  }

  protected lowestPrimaryValue(v: V[]): V {
    const i = 0
    return [...v].sort((a, b) => this.comparator.primaryAsc(a, b))[i]
  }

  protected highestPrimaryValue(v: V[]): V {
    const i = v.length - 1
    return [...v].sort((a, b) => this.comparator.primaryAsc(a, b))[i]
  }

  protected _insertAtLeaf(node: BPTreeLeafNode<K, V>, key: K, value: V): Deferred<void> {
    if (node.values.length) {
      for (let i = 0, len = node.values.length; i < len; i++) {
        const nValue = node.values[i]
        if (this.comparator.isSame(value, nValue)) {
          const keys = node.keys[i]
          if (keys.includes(key)) {
            break
          }
          keys.push(key)
          return this.bufferForNodeUpdate(node)
        }
        else if (this.comparator.isLower(value, nValue)) {
          node.values.splice(i, 0, value)
          node.keys.splice(i, 0, [key])
          return this.bufferForNodeUpdate(node)
        }
        else if (i + 1 === node.values.length) {
          node.values.push(value)
          node.keys.push([key])
          return this.bufferForNodeUpdate(node)
        }
      }
    }
    else {
      node.values = [value]
      node.keys = [[key]]
      return this.bufferForNodeUpdate(node)
    }
  }

  protected bufferForNodeCreate(node: BPTreeUnknownNode<K, V>): Deferred<void> {
    if (node.id === this.rootId) {
      this._strategyDirty = true
    }
    this._nodeCreateBuffer.set(node.id, node)
  }

  protected bufferForNodeUpdate(node: BPTreeUnknownNode<K, V>): Deferred<void> {
    if (node.id === this.rootId) {
      this._strategyDirty = true
    }
    this._nodeUpdateBuffer.set(node.id, node)
  }

  protected bufferForNodeDelete(node: BPTreeUnknownNode<K, V>): Deferred<void> {
    if (node.id === this.rootId) {
      this._strategyDirty = true
    }
    this._nodeDeleteBuffer.set(node.id, node)
  }

  /**
   * Returns the user-defined data stored in the head of the tree.
   * This value can be set using the `setHeadData` method. If no data has been previously inserted, the default value is returned, and the default value is `{}`.
   * @returns User-defined data stored in the head of the tree.
   */
  getHeadData(): SerializableData {
    return this.strategy.head.data
  }

  /**
   * Clears all cached nodes.
   * This method is useful for freeing up memory when the tree is no longer needed.
   */
  clear(): void {
    this._cachedRegexp.clear()
    this.nodes.clear()
  }
}
