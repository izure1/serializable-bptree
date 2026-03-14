import type {
  BPTreeCondition,
  BPTreeConstructorOption,
  BPTreePair,
  BPTreeSearchOption,
  BPTreeUnknownNode,
  SerializableData,
  SerializeStrategyHead
} from './types'
import type { BPTreeNodeOps, BPTreeAlgoContext } from './base/BPTreeNodeOps'
import {
  createVerifierMap,
  createSearchConfigs,
  insertOp,
  deleteOp,
  batchInsertOp,
  bulkLoadOp,
  existsOp,
  getOp,
  whereStreamOp,
  keysStreamOp,
  initOp
} from './base/BPTreeAlgorithmSync'
import { SerializeStrategySync } from './SerializeStrategySync'
import { ValueComparator } from './base/ValueComparator'
import { BPTreeTransaction } from './base/BPTreeTransaction'

export class BPTreePureSync<K, V> {
  protected readonly strategy: SerializeStrategySync<K, V>
  protected readonly comparator: ValueComparator<V>
  protected readonly option: BPTreeConstructorOption
  private readonly _cachedRegexp: Map<string, RegExp> = new Map()
  private _ctx!: BPTreeAlgoContext<K, V>
  private _ops!: BPTreeNodeOps<K, V>

  private readonly _verifierMap: ReturnType<typeof createVerifierMap<V>>
  private readonly _searchConfigs: ReturnType<typeof createSearchConfigs<K, V>>

  constructor(
    strategy: SerializeStrategySync<K, V>,
    comparator: ValueComparator<V>,
    option?: BPTreeConstructorOption,
  ) {
    this.strategy = strategy
    this.comparator = comparator
    this.option = option ?? {}

    const ensureValues = (v: V | V[]): V[] => Array.isArray(v) ? v : [v]
    this._verifierMap = createVerifierMap(comparator, this._cachedRegexp, ensureValues)
    this._searchConfigs = createSearchConfigs(comparator, ensureValues)
  }

  private _ensureValues(v: V | V[]): V[] {
    return Array.isArray(v) ? v : [v]
  }

  private _createOps(): BPTreeNodeOps<K, V> {
    const strategy = this.strategy
    return {
      getNode(id: string): BPTreeUnknownNode<K, V> {
        return strategy.read(id) as BPTreeUnknownNode<K, V>
      },
      createNode(
        leaf: boolean,
        keys: string[] | K[][],
        values: V[],
        parent: string | null = null,
        next: string | null = null,
        prev: string | null = null,
      ): BPTreeUnknownNode<K, V> {
        const id = strategy.id(leaf)
        const node = { id, keys, values, leaf, parent, next, prev } as BPTreeUnknownNode<K, V>
        strategy.write(id, node)
        return node
      },
      updateNode(node: BPTreeUnknownNode<K, V>): void {
        strategy.write(node.id, node)
      },
      deleteNode(node: BPTreeUnknownNode<K, V>): void {
        strategy.delete(node.id)
      },
      readHead(): SerializeStrategyHead | null {
        return strategy.readHead()
      },
      writeHead(head: SerializeStrategyHead): void {
        strategy.writeHead(head)
      },
    }
  }

  public init(): void {
    this._ops = this._createOps()
    this._ctx = {
      rootId: '',
      order: this.strategy.order,
      headData: () => this.strategy.head.data,
    }

    initOp(
      this._ops,
      this._ctx,
      this.strategy.order,
      this.strategy.head,
      (head) => { this.strategy.head = head },
    )
  }

  /**
   * Returns the ID of the root node.
   */
  public getRootId(): string {
    return this._ctx.rootId
  }

  /**
   * Returns the order of the B+Tree.
   */
  public getOrder(): number {
    return this._ctx.order
  }

  /**
   * Verified if the value satisfies the condition.
   */
  public verify(nodeValue: V, condition: BPTreeCondition<V>): boolean {
    for (const key in condition) {
      const verifyFn = this._verifierMap[key as keyof BPTreeCondition<V>]
      const condValue = condition[key as keyof BPTreeCondition<V>] as V
      if (!verifyFn(nodeValue, condValue)) {
        return false
      }
    }
    return true
  }

  // ─── Query ───────────────────────────────────────────────────────

  public get(key: K): V | undefined {
    return getOp(this._ops, this._ctx.rootId, key)
  }

  public exists(key: K, value: V): boolean {
    return existsOp(this._ops, this._ctx.rootId, key, value, this.comparator)
  }

  public *keysStream(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>,
  ): Generator<K> {
    yield* keysStreamOp(
      this._ops, this._ctx.rootId, condition,
      this.comparator, this._verifierMap, this._searchConfigs,
      this._ensureValues, options,
    )
  }

  public *whereStream(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>,
  ): Generator<[K, V]> {
    yield* whereStreamOp(
      this._ops, this._ctx.rootId, condition,
      this.comparator, this._verifierMap, this._searchConfigs,
      this._ensureValues, options,
    )
  }

  public keys(condition: BPTreeCondition<V>, options?: BPTreeSearchOption<K>): Set<K> {
    const set = new Set<K>()
    for (const key of this.keysStream(condition, options)) {
      set.add(key)
    }
    return set
  }

  public where(condition: BPTreeCondition<V>, options?: BPTreeSearchOption<K>): BPTreePair<K, V> {
    const map = new Map<K, V>()
    for (const [key, value] of this.whereStream(condition, options)) {
      map.set(key, value)
    }
    return map
  }

  // ─── Mutation ────────────────────────────────────────────────────

  public insert(key: K, value: V): void {
    insertOp(this._ops, this._ctx, key, value, this.comparator)
  }

  public delete(key: K, value?: V): void {
    deleteOp(this._ops, this._ctx, key, this.comparator, value)
  }

  public batchInsert(entries: [K, V][]): void {
    batchInsertOp(this._ops, this._ctx, entries, this.comparator)
  }

  public bulkLoad(entries: [K, V][]): void {
    bulkLoadOp(this._ops, this._ctx, entries, this.comparator)
  }

  // ─── Head Data ───────────────────────────────────────────────────

  public getHeadData(): SerializableData {
    const head = this._ops.readHead()
    if (head === null) {
      throw new Error('Head not found')
    }
    return head.data
  }

  public setHeadData(data: SerializableData): void {
    const head = this._ops.readHead()
    if (head === null) {
      throw new Error('Head not found')
    }
    this._ops.writeHead({
      root: head.root,
      order: head.order,
      data,
    })
  }

  // ─── Static utilities ────────────────────────────────────────────

  static ChooseDriver = BPTreeTransaction.ChooseDriver
}
