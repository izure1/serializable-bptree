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
  IBPTree,
} from '../types'
import { MVCCTransaction } from 'mvcc-api'
import { ValueComparator } from './ValueComparator'
import { SerializeStrategy } from './SerializeStrategy'

export abstract class BPTreeTransaction<K, V> implements IBPTree<K, V> {
  protected readonly _cachedRegexp: Map<string, RegExp> = new Map()
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

  public abstract getRootNode(): Deferred<BPTreeUnknownNode<K, V>>

  public getRootId(): string {
    return this.rootId
  }

  public getOrder(): number {
    return this.order
  }

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
      const { index, found } = this._binarySearchValues(leaf.values, value)
      if (found) {
        if (leaf.keys[index].includes(key)) {
          return false
        }
        leaf.keys[index].push(key)
        return true
      }
      leaf.values.splice(index, 0, value)
      leaf.keys.splice(index, 0, [key])
      return true
    }
    else {
      leaf.values = [value]
      leaf.keys = [[key]]
      return true
    }
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

  public abstract init(): Deferred<void>
  public abstract reload(): Deferred<void>
  public abstract get(key: K): Deferred<V | undefined>
  public abstract keysStream(condition: BPTreeCondition<V>, options?: BPTreeSearchOption<K>): AsyncGenerator<K> | Generator<K>
  public abstract whereStream(condition: BPTreeCondition<V>, options?: BPTreeSearchOption<K>): AsyncGenerator<[K, V]> | Generator<[K, V]>
  public abstract keys(condition: BPTreeCondition<V>, options?: BPTreeSearchOption<K>): Deferred<Set<K>>
  public abstract where(condition: BPTreeCondition<V>, options?: BPTreeSearchOption<K>): Deferred<BPTreePair<K, V>>
  public abstract insert(key: K, value: V): Deferred<void>
  public abstract batchInsert(entries: [K, V][]): Deferred<void>
  public abstract bulkLoad(entries: [K, V][]): Deferred<void>
  public abstract delete(key: K, value?: V): Deferred<void>
  public abstract batchDelete(entries: [K, V?][]): Deferred<void>
  public abstract exists(key: K, value: V): Deferred<boolean>
  public abstract setHeadData(data: SerializableData): Deferred<void>
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

  protected _resetForReload(): void {
    this._cachedRegexp.clear()
    this.isInitialized = false
    this.isDestroyed = false;
    (this.mvccRoot as any).diskCache.clear()
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
