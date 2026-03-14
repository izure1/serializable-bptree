import type {
  BPTreeCondition,
  BPTreeConstructorOption,
  BPTreePair,
  BPTreeSearchOption,
  BPTreeUnknownNode,
  SerializableData,
  SerializeStrategyHead,
} from './types'
import type { BPTreeNodeOpsAsync, BPTreeAlgoContext } from './base/BPTreeNodeOps'
import {
  createVerifierMap,
  createSearchConfigsAsync,
  insertOpAsync,
  deleteOpAsync,
  batchInsertOpAsync,
  bulkLoadOpAsync,
  existsOpAsync,
  getOpAsync,
  whereStreamOpAsync,
  keysStreamOpAsync,
  initOpAsync,
} from './base/BPTreeAlgorithmAsync'
import { SerializeStrategyAsync } from './SerializeStrategyAsync'
import { ValueComparator } from './base/ValueComparator'
import { BPTreeTransaction } from './base/BPTreeTransaction'

export class BPTreePureAsync<K, V> {
  protected readonly strategy: SerializeStrategyAsync<K, V>
  protected readonly comparator: ValueComparator<V>
  protected readonly option: BPTreeConstructorOption
  private readonly _cachedRegexp: Map<string, RegExp> = new Map()
  private _ctx!: BPTreeAlgoContext<K, V>
  private _ops!: BPTreeNodeOpsAsync<K, V>

  private readonly _verifierMap: ReturnType<typeof createVerifierMap<V>>
  private readonly _searchConfigs: ReturnType<typeof createSearchConfigsAsync<K, V>>

  constructor(
    strategy: SerializeStrategyAsync<K, V>,
    comparator: ValueComparator<V>,
    option?: BPTreeConstructorOption,
  ) {
    this.strategy = strategy
    this.comparator = comparator
    this.option = option ?? {}

    const ensureValues = (v: V | V[]): V[] => Array.isArray(v) ? v : [v]
    this._verifierMap = createVerifierMap(comparator, this._cachedRegexp, ensureValues)
    this._searchConfigs = createSearchConfigsAsync(comparator, ensureValues)
  }

  private _ensureValues(v: V | V[]): V[] {
    return Array.isArray(v) ? v : [v]
  }

  private _createOps(): BPTreeNodeOpsAsync<K, V> {
    const strategy = this.strategy
    return {
      async getNode(id: string): Promise<BPTreeUnknownNode<K, V>> {
        return await strategy.read(id) as BPTreeUnknownNode<K, V>
      },
      async createNode(
        leaf: boolean,
        keys: string[] | K[][],
        values: V[],
        parent: string | null = null,
        next: string | null = null,
        prev: string | null = null,
      ): Promise<BPTreeUnknownNode<K, V>> {
        const id = await strategy.id(leaf)
        const node = { id, keys, values, leaf, parent, next, prev } as BPTreeUnknownNode<K, V>
        await strategy.write(id, node)
        return node
      },
      async updateNode(node: BPTreeUnknownNode<K, V>): Promise<void> {
        await strategy.write(node.id, node)
      },
      async deleteNode(node: BPTreeUnknownNode<K, V>): Promise<void> {
        await strategy.delete(node.id)
      },
      async readHead(): Promise<SerializeStrategyHead | null> {
        return await strategy.readHead()
      },
      async writeHead(head: SerializeStrategyHead): Promise<void> {
        await strategy.writeHead(head)
      },
    }
  }

  public async init(): Promise<void> {
    this._ops = this._createOps()
    this._ctx = {
      rootId: '',
      order: this.strategy.order,
      headData: () => this.strategy.head.data,
    }

    await initOpAsync(
      this._ops,
      this._ctx,
      this.strategy.order,
      this.strategy.head,
      (head) => { this.strategy.head = head },
    )
  }

  public getRootId(): string {
    return this._ctx.rootId
  }

  public getOrder(): number {
    return this._ctx.order
  }

  public verify(nodeValue: V, condition: BPTreeCondition<V>): boolean {
    for (const key in condition) {
      const verifyFn = this._verifierMap[key as keyof BPTreeCondition<V>]
      const condValue = condition[key as keyof BPTreeCondition<V>] as V
      if (!verifyFn(nodeValue, condValue)) return false
    }
    return true
  }

  // ─── Query ───────────────────────────────────────────────────────

  public async get(key: K): Promise<V | undefined> {
    return getOpAsync(this._ops, this._ctx.rootId, key)
  }

  public async exists(key: K, value: V): Promise<boolean> {
    return existsOpAsync(this._ops, this._ctx.rootId, key, value, this.comparator)
  }

  public async *keysStream(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>,
  ): AsyncGenerator<K> {
    yield* keysStreamOpAsync(
      this._ops, this._ctx.rootId, condition,
      this.comparator, this._verifierMap, this._searchConfigs,
      this._ensureValues, options,
    )
  }

  public async *whereStream(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>,
  ): AsyncGenerator<[K, V]> {
    yield* whereStreamOpAsync(
      this._ops, this._ctx.rootId, condition,
      this.comparator, this._verifierMap, this._searchConfigs,
      this._ensureValues, options,
    )
  }

  public async keys(condition: BPTreeCondition<V>, options?: BPTreeSearchOption<K>): Promise<Set<K>> {
    const set = new Set<K>()
    for await (const key of this.keysStream(condition, options)) {
      set.add(key)
    }
    return set
  }

  public async where(condition: BPTreeCondition<V>, options?: BPTreeSearchOption<K>): Promise<BPTreePair<K, V>> {
    const map = new Map<K, V>()
    for await (const [key, value] of this.whereStream(condition, options)) {
      map.set(key, value)
    }
    return map
  }

  // ─── Mutation ────────────────────────────────────────────────────

  public async insert(key: K, value: V): Promise<void> {
    await insertOpAsync(this._ops, this._ctx, key, value, this.comparator)
  }

  public async delete(key: K, value?: V): Promise<void> {
    await deleteOpAsync(this._ops, this._ctx, key, this.comparator, value)
  }

  public async batchInsert(entries: [K, V][]): Promise<void> {
    await batchInsertOpAsync(this._ops, this._ctx, entries, this.comparator)
  }

  public async bulkLoad(entries: [K, V][]): Promise<void> {
    await bulkLoadOpAsync(this._ops, this._ctx, entries, this.comparator)
  }

  // ─── Head Data ───────────────────────────────────────────────────

  public async getHeadData(): Promise<SerializableData> {
    const head = await this._ops.readHead()
    if (head === null) throw new Error('Head not found')
    return head.data
  }

  public async setHeadData(data: SerializableData): Promise<void> {
    const head = await this._ops.readHead()
    if (head === null) throw new Error('Head not found')
    await this._ops.writeHead({ root: head.root, order: head.order, data })
  }

  // ─── Static utilities ────────────────────────────────────────────

  static ChooseDriver = BPTreeTransaction.ChooseDriver
}
