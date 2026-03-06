import type { TransactionEntry, TransactionResult } from 'mvcc-api'
import type {
  BPTreeCondition,
  BPTreeConstructorOption,
  BPTreeUnknownNode,
  Deferred,
  BPTreeLeafNode,
  BPTreeNodeKey,
  BPTreePair,
  SerializableData,
  BPTreeNode,
  BPTreeMVCC,
  BPTreeSearchOption,
} from '../types'
import { MVCCTransaction } from 'mvcc-api'
import { ValueComparator } from './ValueComparator'
import { SerializeStrategy } from './SerializeStrategy'

export abstract class BPTreeTransaction<K, V> {
  private readonly _cachedRegexp: Map<string, RegExp> = new Map()
  protected readonly nodes: Map<string, BPTreeUnknownNode<K, V>> = new Map()
  protected readonly deletedNodeBuffer: Map<string, BPTreeUnknownNode<K, V>> = new Map()
  protected readonly rootTx: BPTreeTransaction<K, V>
  protected readonly mvccRoot: BPTreeMVCC<K, V>
  protected readonly mvcc: BPTreeMVCC<K, V>
  protected readonly strategy: SerializeStrategy<K, V>
  protected readonly comparator: ValueComparator<V>
  protected readonly option: BPTreeConstructorOption
  protected order!: number
  protected rootId!: string
  protected isInitialized: boolean = false
  protected isDestroyed: boolean = false

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
        if (!this._cachedRegexp.has(value)) {
          const pattern = value.replace(/%/g, '.*').replace(/_/g, '.')
          const regexp = new RegExp(`^${pattern}$`, 'i')
          this._cachedRegexp.set(value, regexp)
        }
        const regexp = this._cachedRegexp.get(value) as RegExp
        return regexp.test(nodeValue)
      },
    }

  protected readonly searchConfigs: Record<
    keyof BPTreeCondition<V>,
    Record<
      'asc' | 'desc',
      {
        start: (tx: BPTreeTransaction<K, V>, v: V[]) => Deferred<BPTreeLeafNode<K, V> | null>
        end: (tx: BPTreeTransaction<K, V>, v: V[]) => Deferred<BPTreeLeafNode<K, V> | null>
        direction: 1 | -1
        earlyTerminate: boolean
      }
    >
  > = {
      gt: {
        asc: {
          start: (tx, v) => tx.findUpperBoundLeaf(v[0]),
          end: () => null as any,
          direction: 1,
          earlyTerminate: false
        },
        desc: {
          start: (tx) => tx.rightestNode(),
          end: (tx, v) => tx.findOuterBoundaryLeaf(v[0], -1),
          direction: -1,
          earlyTerminate: true
        }
      },
      gte: {
        asc: {
          start: (tx, v) => tx.findLowerBoundLeaf(v[0]),
          end: () => null as any,
          direction: 1,
          earlyTerminate: false
        },
        desc: {
          start: (tx) => tx.rightestNode(),
          end: (tx, v) => tx.findOuterBoundaryLeaf(v[0], -1),
          direction: -1,
          earlyTerminate: true
        }
      },
      lt: {
        asc: {
          start: (tx) => tx.leftestNode(),
          end: (tx, v) => tx.findOuterBoundaryLeaf(v[0], 1),
          direction: 1,
          earlyTerminate: true
        },
        desc: {
          start: (tx, v) => tx.findLowerBoundLeaf(v[0]),
          end: () => null as any,
          direction: -1,
          earlyTerminate: false
        }
      },
      lte: {
        asc: {
          start: (tx) => tx.leftestNode(),
          end: (tx, v) => tx.findOuterBoundaryLeaf(v[0], 1),
          direction: 1,
          earlyTerminate: true
        },
        desc: {
          start: (tx, v) => tx.findUpperBoundLeaf(v[0]),
          end: () => null as any,
          direction: -1,
          earlyTerminate: false
        }
      },
      equal: {
        asc: {
          start: (tx, v) => tx.findLowerBoundLeaf(v[0]),
          end: (tx, v) => tx.findOuterBoundaryLeaf(v[0], 1),
          direction: 1,
          earlyTerminate: true
        },
        desc: {
          start: (tx, v) => tx.findOuterBoundaryLeaf(v[0], 1),
          end: (tx, v) => tx.findOuterBoundaryLeaf(v[0], -1),
          direction: -1,
          earlyTerminate: true
        }
      },
      notEqual: {
        asc: {
          start: (tx) => tx.leftestNode(),
          end: () => null as any,
          direction: 1,
          earlyTerminate: false
        },
        desc: {
          start: (tx) => tx.rightestNode(),
          end: () => null as any,
          direction: -1,
          earlyTerminate: false
        }
      },
      or: {
        asc: {
          start: (tx, v) => tx.findLowerBoundLeaf(tx.lowestValue(v)),
          end: (tx, v) => tx.findOuterBoundaryLeaf(tx.highestValue(v), 1),
          direction: 1,
          earlyTerminate: false
        },
        desc: {
          start: (tx, v) => tx.findOuterBoundaryLeaf(tx.highestValue(v), 1),
          end: (tx, v) => tx.findOuterBoundaryLeaf(tx.lowestValue(v), -1),
          direction: -1,
          earlyTerminate: false
        }
      },
      primaryGt: {
        asc: {
          start: (tx, v) => tx.findUpperBoundLeaf(v[0]),
          end: () => null as any,
          direction: 1,
          earlyTerminate: false
        },
        desc: {
          start: (tx) => tx.rightestNode(),
          end: (tx, v) => tx.findOuterBoundaryLeaf(v[0], -1),
          direction: -1,
          earlyTerminate: true
        }
      },
      primaryGte: {
        asc: {
          start: (tx, v) => tx.findLowerBoundLeaf(v[0]),
          end: () => null as any,
          direction: 1,
          earlyTerminate: false
        },
        desc: {
          start: (tx) => tx.rightestNode(),
          end: (tx, v) => tx.findOuterBoundaryLeaf(v[0], -1),
          direction: -1,
          earlyTerminate: true
        }
      },
      primaryLt: {
        asc: {
          start: (tx) => tx.leftestNode(),
          end: (tx, v) => tx.findOuterBoundaryLeaf(v[0], 1),
          direction: 1,
          earlyTerminate: true
        },
        desc: {
          start: (tx, v) => tx.findLowerBoundLeaf(v[0]),
          end: () => null as any,
          direction: -1,
          earlyTerminate: false
        }
      },
      primaryLte: {
        asc: {
          start: (tx) => tx.leftestNode(),
          end: (tx, v) => tx.findOuterBoundaryLeaf(v[0], 1),
          direction: 1,
          earlyTerminate: true
        },
        desc: {
          start: (tx, v) => tx.findUpperBoundLeaf(v[0]),
          end: () => null as any,
          direction: -1,
          earlyTerminate: false
        }
      },
      primaryEqual: {
        asc: {
          start: (tx, v) => tx.findLowerBoundLeaf(v[0]),
          end: (tx, v) => tx.findOuterBoundaryLeaf(v[0], 1),
          direction: 1,
          earlyTerminate: true
        },
        desc: {
          start: (tx, v) => tx.findUpperBoundLeaf(v[0]),
          end: (tx, v) => tx.findOuterBoundaryLeaf(v[0], -1),
          direction: -1,
          earlyTerminate: true
        }
      },
      primaryNotEqual: {
        asc: {
          start: (tx) => tx.leftestNode(),
          end: () => null as any,
          direction: 1,
          earlyTerminate: false
        },
        desc: {
          start: (tx) => tx.rightestNode(),
          end: () => null as any,
          direction: -1,
          earlyTerminate: false
        }
      },
      primaryOr: {
        asc: {
          start: (tx, v) => tx.findLowerBoundLeaf(tx.lowestPrimaryValue(v)),
          end: (tx, v) => tx.findOuterBoundaryLeaf(tx.highestPrimaryValue(v), 1),
          direction: 1,
          earlyTerminate: false
        },
        desc: {
          start: (tx, v) => tx.findUpperBoundLeaf(tx.highestPrimaryValue(v)),
          end: (tx, v) => tx.findOuterBoundaryLeaf(tx.lowestPrimaryValue(v), -1),
          direction: -1,
          earlyTerminate: false
        }
      },
      like: {
        asc: {
          start: (tx) => tx.leftestNode(),
          end: () => null as any,
          direction: 1,
          earlyTerminate: false
        },
        desc: {
          start: (tx) => tx.rightestNode(),
          end: () => null as any,
          direction: -1,
          earlyTerminate: false
        }
      }
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
        const priority = BPTreeTransaction.conditionPriority[condKey] ?? 0
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
   * Checks for conflicts between multiple transactions.
   * 
   * @param transactions Array of BPTreeTransaction instances to check
   * @returns An array of keys that are in conflict. Empty array if no conflicts.
   */
  static CheckConflicts<K, V>(transactions: BPTreeTransaction<K, V>[]): string[] {
    return MVCCTransaction.CheckConflicts(transactions.map(tx => tx.mvcc))
  }

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

  /**
   * Inserts a key-value pair into an already-cloned leaf node in-place.
   * Unlike _insertAtLeaf, this does NOT clone or update the node via MVCC.
   * Used by batchInsert to batch multiple insertions with a single clone/update.
   * @returns true if the leaf was modified, false if the key already exists.
   */
  protected _insertValueIntoLeaf(leaf: BPTreeLeafNode<K, V>, key: K, value: V): boolean {
    if (leaf.values.length) {
      for (let i = 0, len = leaf.values.length; i < len; i++) {
        const nValue = leaf.values[i]
        if (this.comparator.isSame(value, nValue)) {
          if (leaf.keys[i].includes(key)) {
            return false
          }
          leaf.keys[i].push(key)
          return true
        }
        else if (this.comparator.isLower(value, nValue)) {
          leaf.values.splice(i, 0, value)
          leaf.keys.splice(i, 0, [key])
          return true
        }
        else if (i + 1 === leaf.values.length) {
          leaf.values.push(value)
          leaf.keys.push([key])
          return true
        }
      }
    }
    else {
      leaf.values = [value]
      leaf.keys = [[key]]
      return true
    }
    return false
  }

  protected _cloneNode<T extends BPTreeUnknownNode<K, V>>(node: T): T {
    return JSON.parse(JSON.stringify(node)) as T
  }

  /**
   * Resolves the best start/end configuration by independently examining
   * all conditions. Selects the tightest lower bound for start and the
   * tightest upper bound for end (in asc; reversed for desc).
   *
   * @param condition The condition to analyze.
   * @param order The sort order ('asc' or 'desc').
   * @returns The resolved start/end keys, values, and traversal direction.
   */
  resolveStartEndConfigs(
    condition: BPTreeCondition<V>,
    order: 'asc' | 'desc'
  ): {
    startKey: keyof BPTreeCondition<V> | null
    endKey: keyof BPTreeCondition<V> | null
    startValues: V[]
    endValues: V[]
    direction: 1 | -1
  } {
    const direction: 1 | -1 = order === 'asc' ? 1 : -1

    // For asc: start = lower bound, end = upper bound
    // For desc: start = upper bound, end = lower bound
    const startCandidates = order === 'asc'
      ? BPTreeTransaction._lowerBoundKeys
      : BPTreeTransaction._upperBoundKeys
    const endCandidates = order === 'asc'
      ? BPTreeTransaction._upperBoundKeys
      : BPTreeTransaction._lowerBoundKeys

    let startKey: keyof BPTreeCondition<V> | null = null
    let endKey: keyof BPTreeCondition<V> | null = null
    let startValues: V[] = []
    let endValues: V[] = []

    for (let i = 0, len = startCandidates.length; i < len; i++) {
      const key = startCandidates[i]
      if (key in condition) {
        startKey = key
        startValues = BPTreeTransaction._multiValueKeys.includes(key)
          ? this.ensureValues(condition[key] as V)
          : [condition[key] as V]
        break
      }
    }

    for (let i = 0, len = endCandidates.length; i < len; i++) {
      const key = endCandidates[i]
      if (key in condition) {
        endKey = key
        endValues = BPTreeTransaction._multiValueKeys.includes(key)
          ? this.ensureValues(condition[key] as V)
          : [condition[key] as V]
        break
      }
    }

    return { startKey, endKey, startValues, endValues, direction }
  }

  // Lower bound providers, ordered by selectivity (tightest first)
  // Used for asc start / desc end
  private static readonly _lowerBoundKeys: (keyof BPTreeCondition<unknown>)[] = [
    'primaryEqual', 'equal',
    'primaryGt', 'gt', 'primaryGte', 'gte',
    'primaryOr', 'or',
  ]

  // Upper bound providers, ordered by selectivity (tightest first)
  // Used for asc end / desc start
  private static readonly _upperBoundKeys: (keyof BPTreeCondition<unknown>)[] = [
    'primaryEqual', 'equal',
    'primaryLt', 'lt', 'primaryLte', 'lte',
    'primaryOr', 'or',
  ]

  // Condition keys that accept multiple values (V[]) rather than a single value (V)
  private static readonly _multiValueKeys: (keyof BPTreeCondition<unknown>)[] = [
    'or',
    'primaryOr',
  ]

  protected constructor(
    rootTx: BPTreeTransaction<K, V> | null,
    mvccRoot: BPTreeMVCC<K, V>,
    mvcc: BPTreeMVCC<K, V>,
    strategy: SerializeStrategy<K, V>,
    comparator: ValueComparator<V>,
    option?: BPTreeConstructorOption
  ) {
    this.rootTx = rootTx === null ? this : rootTx
    this.mvccRoot = mvccRoot
    this.mvcc = mvcc
    this.strategy = strategy
    this.comparator = comparator
    this.option = option ?? {}
  }

  protected abstract _createNode(
    leaf: boolean,
    keys: string[] | K[][],
    values: V[],
    parent?: string | null,
    next?: string | null,
    prev?: string | null
  ): Deferred<BPTreeUnknownNode<K, V>>
  protected abstract _deleteEntry(node: BPTreeUnknownNode<K, V>, key: BPTreeNodeKey<K>): Deferred<BPTreeUnknownNode<K, V>>
  protected abstract _insertInParent(node: BPTreeUnknownNode<K, V>, value: V, pointer: BPTreeUnknownNode<K, V>): Deferred<void>
  protected abstract _insertAtLeaf(node: BPTreeUnknownNode<K, V>, key: BPTreeNodeKey<K>, value: V): Deferred<BPTreeUnknownNode<K, V>>
  protected abstract getNode(id: string): Deferred<BPTreeUnknownNode<K, V>>
  protected abstract locateLeaf(value: V): Deferred<BPTreeLeafNode<K, V>>
  protected abstract findLowerBoundLeaf(value: V): Deferred<BPTreeLeafNode<K, V>>
  protected abstract findUpperBoundLeaf(value: V): Deferred<BPTreeLeafNode<K, V>>
  protected abstract findOuterBoundaryLeaf(value: V, direction: 1 | -1): Deferred<BPTreeLeafNode<K, V> | null>
  protected abstract leftestNode(): Deferred<BPTreeLeafNode<K, V>>
  protected abstract rightestNode(): Deferred<BPTreeLeafNode<K, V>>

  /**
   * After creating a tree instance, it must be called.  
   * This method is used to initialize the stored tree and recover data.
   * If it is not called, the tree will not function.
   */
  public abstract init(): Deferred<void>
  /**
   * Retrieves the value associated with the given key.
   * @param key The key to search for.
   * @returns A Deferred that resolves to the value if found, or undefined if not found.
   */
  public abstract get(key: K): Deferred<V | undefined>
  /**
   * Returns a generator that yields keys satisfying the given condition.
   * This is a memory-efficient way to iterate through keys when dealing with large result sets.
   * @param condition The search condition (e.g., gt, lt, equal, like).
   * @param options Search options including filterValues, limit, and order.
   * @returns An async or synchronous generator yielding keys of type K.
   */
  public abstract keysStream(condition: BPTreeCondition<V>, options?: BPTreeSearchOption<K>): AsyncGenerator<K> | Generator<K>
  /**
   * Returns a generator that yields [key, value] pairs satisfying the given condition.
   * This is a memory-efficient way to iterate through pairs when dealing with large result sets.
   * @param condition The search condition (e.g., gt, lt, equal, like).
   * @param options Search options including filterValues, limit, and order.
   * @returns An async or synchronous generator yielding [K, V] tuples.
   */
  public abstract whereStream(condition: BPTreeCondition<V>, options?: BPTreeSearchOption<K>): AsyncGenerator<[K, V]> | Generator<[K, V]>
  /**
   * It searches for a key within the tree. The result is returned as an array sorted in ascending order based on the value.  
   * The result is key set instance, and you can use the `gt`, `lt`, `gte`, `lte`, `equal`, `notEqual`, `like` condition statements.
   * This method operates much faster than first searching with `where` and then retrieving only the key list.
   * @param condition You can use the `gt`, `lt`, `gte`, `lte`, `equal`, `notEqual`, `like` condition statements.
   * @param options Search options including filterValues, limit, and order.
   */
  public abstract keys(condition: BPTreeCondition<V>, options?: BPTreeSearchOption<K>): Deferred<Set<K>>
  /**
   * It searches for a value within the tree. The result is returned as an array sorted in ascending order based on the value.  
   * The result includes the key and value attributes, and you can use the `gt`, `lt`, `gte`, `lte`, `equal`, `notEqual`, `like` condition statements.
   * @param condition You can use the `gt`, `lt`, `gte`, `lte`, `equal`, `notEqual`, `like` condition statements.
   * @param options Search options including filterValues, limit, and order.
   */
  public abstract where(condition: BPTreeCondition<V>, options?: BPTreeSearchOption<K>): Deferred<BPTreePair<K, V>>
  /**
   * You enter the key and value as a pair. You can later search for the pair by value.
   * This data is stored in the tree, sorted in ascending order of value.
   * @param key The key of the pair. This key must be unique.
   * @param value The value of the pair.
   */
  public abstract insert(key: K, value: V): Deferred<void>
  /**
   * Inserts multiple key-value pairs into the tree in a single batch operation.
   * Entries are sorted by value before insertion to optimize tree traversal.
   * This is more efficient than calling insert() multiple times.
   * @param entries Array of [key, value] pairs to insert.
   */
  public abstract batchInsert(entries: [K, V][]): Deferred<void>
  /**
   * Deletes the pair that matches the key and value.
   * @param key The key of the pair. This key must be unique.
   * @param value The value of the pair.
   * @warning If the 'value' is not specified, a full scan will be performed to find the value associated with the key, which may lead to performance degradation.
   */
  public abstract delete(key: K, value?: V): Deferred<void>
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
   * Returns the user-defined data stored in the head of the tree.
   */
  abstract getHeadData(): Deferred<SerializableData>
  /**
   * Commits the transaction and returns the result.
   * @param label The label of the transaction.
   */
  abstract commit(label?: string): Deferred<TransactionResult<string, BPTreeNode<K, V>>>
  /**
   * Rolls back the transaction and returns the result.
   */
  abstract rollback(): Deferred<TransactionResult<string, BPTreeNode<K, V>>>

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

  /**
   * Returns the result entries of the transaction.
   * @returns Returns the node entries that will be created, updated, and deleted by this transaction.
   */
  getResultEntries(): {
    created: TransactionEntry<string, BPTreeNode<K, V>>[]
    updated: TransactionEntry<string, BPTreeNode<K, V>>[]
    deleted: TransactionEntry<string, BPTreeNode<K, V>>[]
  } {
    return this.mvcc.getResultEntries()
  }

  protected _clearCache(): void {
    this._cachedRegexp.clear()
  }

  /**
   * Clears all cached nodes.
   * This method is useful for freeing up memory when the tree is no longer needed.
   */
  public clear(): void {
    if (this.rootTx !== this) {
      throw new Error('Cannot call clear on a nested transaction')
    }
    this._clearInternal()
  }

  protected _clearInternal(): void {
    if (this.isDestroyed) {
      throw new Error('Transaction already destroyed')
    }
    this._clearCache()
    this.isDestroyed = true
  }

  protected _binarySearchValues(
    values: V[],
    target: V,
    usePrimary: boolean = false,
    upperBound: boolean = false
  ): { index: number, found: boolean } {
    let low = 0
    let high = values.length
    let found = false
    while (low < high) {
      const mid = (low + high) >>> 1
      const cmp = usePrimary
        ? this.comparator.primaryAsc(target, values[mid])
        : this.comparator.asc(target, values[mid])
      if (cmp === 0) {
        found = true
        if (upperBound) low = mid + 1
        else high = mid
      }
      else if (cmp < 0) {
        high = mid
      }
      else {
        low = mid + 1
      }
    }
    return { index: low, found }
  }
}
