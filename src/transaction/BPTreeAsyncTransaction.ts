import type { TransactionResult } from 'mvcc-api'
import type {
  AsyncBPTreeMVCC,
  BPTreeCondition,
  BPTreeConstructorOption,
  BPTreeNode,
  BPTreeNodeKey,
  BPTreePair,
  BPTreeUnknownNode,
  SerializableData,
  SerializeStrategyHead,
  BPTreeSearchOption,
} from '../types'
import type { BPTreeNodeOpsAsync, BPTreeAlgoContext } from '../base/BPTreeNodeOps'
import { Ryoiki } from 'ryoiki'
import {
  insertOpAsync,
  deleteOpAsync,
  batchInsertOpAsync,
  bulkLoadOpAsync,
  existsOpAsync,
  getOpAsync,
  whereStreamOpAsync,
  keysStreamOpAsync,
  initOpAsync,
  insertAtLeafAsync,
  insertInParentAsync,
  deleteEntryAsync,
  locateLeafAsync,
  findLowerBoundLeafAsync,
  findUpperBoundLeafAsync,
  findOuterBoundaryLeafAsync,
  leftestNodeAsync,
  rightestNodeAsync,
  getPairsGeneratorAsync,
  createSearchConfigsAsync,
  createVerifierMap,
} from '../base/BPTreeAlgorithmAsync'
import { BPTreeTransaction } from '../base/BPTreeTransaction'
import { SerializeStrategyAsync } from '../SerializeStrategyAsync'
import { ValueComparator } from '../base/ValueComparator'

export class BPTreeAsyncTransaction<K, V> extends BPTreeTransaction<K, V> {
  declare protected readonly rootTx: BPTreeAsyncTransaction<K, V>
  declare protected readonly mvccRoot: AsyncBPTreeMVCC<K, V>
  declare protected readonly mvcc: AsyncBPTreeMVCC<K, V>
  declare protected readonly strategy: SerializeStrategyAsync<K, V>
  declare protected readonly comparator: ValueComparator<V>
  declare protected readonly option: BPTreeConstructorOption
  protected readonly lock: Ryoiki

  private _ops!: BPTreeNodeOpsAsync<K, V>
  private _ctx!: BPTreeAlgoContext<K, V>
  private _verifierMapCached!: ReturnType<typeof createVerifierMap<V>>
  private _searchConfigsCached!: ReturnType<typeof createSearchConfigsAsync<K, V>>

  constructor(
    rootTx: BPTreeAsyncTransaction<K, V> | null,
    mvccRoot: AsyncBPTreeMVCC<K, V>,
    mvcc: AsyncBPTreeMVCC<K, V>,
    strategy: SerializeStrategyAsync<K, V>,
    comparator: ValueComparator<V>,
    option?: BPTreeConstructorOption
  ) {
    super(
      rootTx,
      mvccRoot,
      mvcc,
      strategy,
      comparator,
      option,
    )
    this.lock = new Ryoiki()
    this._initAlgoContext()
  }

  private _initAlgoContext(): void {
    const mvcc = this.mvcc
    const strategy = this.strategy
    const self = this

    this._ops = {
      async getNode(id: string): Promise<BPTreeUnknownNode<K, V>> {
        return await mvcc.read(id) as BPTreeUnknownNode<K, V>
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
        await mvcc.create(id, node)
        return node
      },
      async updateNode(node: BPTreeUnknownNode<K, V>): Promise<void> {
        if (mvcc.isDeleted(node.id)) {
          return
        }
        await mvcc.write(node.id, node)
      },
      async deleteNode(node: BPTreeUnknownNode<K, V>): Promise<void> {
        if (mvcc.isDeleted(node.id)) {
          return
        }
        await mvcc.delete(node.id)
      },
      async readHead(): Promise<SerializeStrategyHead | null> {
        return await mvcc.read('__HEAD__') as unknown as SerializeStrategyHead | null
      },
      async writeHead(head: SerializeStrategyHead): Promise<void> {
        if (!(await mvcc.exists('__HEAD__'))) {
          await mvcc.create('__HEAD__', head as unknown as BPTreeUnknownNode<K, V>)
        }
        else {
          await mvcc.write('__HEAD__', head as unknown as BPTreeUnknownNode<K, V>)
        }
        self.rootId = head.root!
      },
    }

    this._ctx = {
      get rootId() { return self.rootId },
      set rootId(v: string) { self.rootId = v },
      get order() { return self.order },
      set order(v: number) { self.order = v },
      headData: () => this.strategy.head.data,
    }

    const ensureValues = (v: V | V[]): V[] => this.ensureValues(v)
    this._verifierMapCached = createVerifierMap(this.comparator, this._cachedRegexp, ensureValues)
    this._searchConfigsCached = createSearchConfigsAsync(this.comparator, ensureValues)
  }

  protected async writeLock<T>(id: number, fn: () => Promise<T>): Promise<T> {
    let lockId: string
    return this.lock.writeLock([id, id + 0.1], async (_lockId) => {
      lockId = _lockId
      return fn()
    }).finally(() => {
      this.lock.writeUnlock(lockId)
    })
  }

  // ─── Legacy protected methods (delegating to ops) ────────────────

  protected async getNode(id: string): Promise<BPTreeUnknownNode<K, V>> {
    return this._ops.getNode(id)
  }

  protected async _createNode(
    leaf: boolean,
    keys: string[] | K[][],
    values: V[],
    parent: string | null = null,
    next: string | null = null,
    prev: string | null = null
  ): Promise<BPTreeUnknownNode<K, V>> {
    return this._ops.createNode(leaf, keys, values, parent, next, prev)
  }

  protected async _updateNode(node: BPTreeUnknownNode<K, V>): Promise<void> {
    await this._ops.updateNode(node)
  }

  protected async _deleteNode(node: BPTreeUnknownNode<K, V>): Promise<void> {
    await this._ops.deleteNode(node)
  }

  protected async _readHead(): Promise<SerializeStrategyHead | null> {
    return this._ops.readHead()
  }

  protected async _writeHead(head: SerializeStrategyHead): Promise<void> {
    await this._ops.writeHead(head)
  }

  // ─── Tree traversal (delegating to algorithm) ────────────────────

  protected async _insertAtLeaf(node: BPTreeUnknownNode<K, V>, key: BPTreeNodeKey<K>, value: V): Promise<BPTreeUnknownNode<K, V>> {
    return insertAtLeafAsync(this._ops, node, key, value, this.comparator)
  }

  protected async _insertInParent(node: BPTreeUnknownNode<K, V>, value: V, newSiblingNode: BPTreeUnknownNode<K, V>): Promise<void> {
    await insertInParentAsync(this._ops, this._ctx, node, value, newSiblingNode)
  }

  protected async locateLeaf(value: V) {
    return locateLeafAsync(this._ops, this._ctx.rootId, value, this.comparator)
  }

  protected async findLowerBoundLeaf(value: V) {
    return findLowerBoundLeafAsync(this._ops, this._ctx.rootId, value, this.comparator)
  }

  protected async findUpperBoundLeaf(value: V) {
    return findUpperBoundLeafAsync(this._ops, this._ctx.rootId, value, this.comparator)
  }

  protected async findOuterBoundaryLeaf(value: V, direction: 1 | -1) {
    return findOuterBoundaryLeafAsync(this._ops, this._ctx.rootId, value, direction, this.comparator)
  }

  protected async leftestNode() {
    return leftestNodeAsync(this._ops, this._ctx.rootId)
  }

  protected async rightestNode() {
    return rightestNodeAsync(this._ops, this._ctx.rootId)
  }

  protected async *getPairsGenerator(
    startNode: Parameters<typeof getPairsGeneratorAsync<K, V>>[1],
    endNode: Parameters<typeof getPairsGeneratorAsync<K, V>>[2],
    direction: 1 | -1,
  ) {
    yield* getPairsGeneratorAsync(this._ops, startNode, endNode, direction)
  }

  // ─── Lifecycle ───────────────────────────────────────────────────

  public async init(): Promise<void> {
    if (this.rootTx !== this) {
      throw new Error('Cannot call init on a nested transaction')
    }
    return await this._initInternal()
  }

  protected async _initInternal(): Promise<void> {
    if (this.isInitialized) {
      throw new Error('Transaction already initialized')
    }
    if (this.isDestroyed) {
      throw new Error('Transaction already destroyed')
    }
    this.isInitialized = true
    try {
      this._clearCache()
      await initOpAsync(
        this._ops,
        this._ctx,
        this.strategy.order,
        this.strategy.head,
        (head) => { this.strategy.head = head },
      )
    } catch (e) {
      this.isInitialized = false
      throw e
    }
  }

  public async reload(): Promise<void> {
    if (this.rootTx !== this) {
      throw new Error('Cannot call reload on a nested transaction')
    }
    return await this._reloadInternal()
  }

  protected async _reloadInternal(): Promise<void> {
    this._resetForReload()
    await this._initInternal()
  }

  // ─── Query (delegating to algorithm) ─────────────────────────────

  public async exists(key: K, value: V): Promise<boolean> {
    return existsOpAsync(this._ops, this._ctx.rootId, key, value, this.comparator)
  }

  public async get(key: K): Promise<V | undefined> {
    return getOpAsync(this._ops, this._ctx.rootId, key)
  }

  public async *keysStream(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>
  ): AsyncGenerator<K> {
    yield* keysStreamOpAsync(
      this._ops, this._ctx.rootId, condition,
      this.comparator, this._verifierMapCached, this._searchConfigsCached,
      this.ensureValues.bind(this), options,
    )
  }

  public async *whereStream(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>
  ): AsyncGenerator<[K, V]> {
    yield* whereStreamOpAsync(
      this._ops, this._ctx.rootId, condition,
      this.comparator, this._verifierMapCached, this._searchConfigsCached,
      this.ensureValues.bind(this), options,
    )
  }

  public async keys(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>
  ): Promise<Set<K>> {
    const set = new Set<K>()
    for await (const key of this.keysStream(condition, options)) {
      set.add(key)
    }
    return set
  }

  public async where(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>
  ): Promise<BPTreePair<K, V>> {
    const map = new Map<K, V>()
    for await (const [key, value] of this.whereStream(condition, options)) {
      map.set(key, value)
    }
    return map
  }

  // ─── Mutation (delegating to algorithm) ──────────────────────────

  public async insert(key: K, value: V): Promise<void> {
    return this.writeLock(0, async () => {
      await insertOpAsync(this._ops, this._ctx, key, value, this.comparator)
    })
  }

  public async batchInsert(entries: [K, V][]): Promise<void> {
    if (entries.length === 0) return
    return this.writeLock(0, async () => {
      await batchInsertOpAsync(this._ops, this._ctx, entries, this.comparator)
    })
  }

  public async bulkLoad(entries: [K, V][]): Promise<void> {
    if (entries.length === 0) return
    return this.writeLock(0, async () => {
      await bulkLoadOpAsync(this._ops, this._ctx, entries, this.comparator)
    })
  }

  protected async _deleteEntry(
    node: BPTreeUnknownNode<K, V>,
    key: BPTreeNodeKey<K>
  ): Promise<BPTreeUnknownNode<K, V>> {
    return deleteEntryAsync(this._ops, this._ctx, node, key, this.comparator)
  }

  public async delete(key: K, value?: V): Promise<void> {
    return this.writeLock(0, async () => {
      await deleteOpAsync(this._ops, this._ctx, key, this.comparator, value)
    })
  }

  // ─── Head Data ───────────────────────────────────────────────────

  public async getHeadData(): Promise<SerializableData> {
    const head = await this._readHead()
    if (head === null) {
      throw new Error('Head not found')
    }
    return head.data
  }

  public async setHeadData(data: SerializableData): Promise<void> {
    const head = await this._readHead()
    if (head === null) {
      throw new Error('Head not found')
    }
    await this._writeHead({
      root: head.root,
      order: head.order,
      data,
    })
  }

  // ─── Transaction ─────────────────────────────────────────────────

  public async commit(label?: string): Promise<TransactionResult<string, BPTreeNode<K, V>>> {
    let result = await this.mvcc.commit(label)
    if (result.success) {
      const isRootTx = this.rootTx === this
      if (!isRootTx) {
        result = await this.rootTx.commit(label)
        if (result.success) {
          this.rootTx.rootId = this.rootId
        }
        else {
          this.mvcc.rollback()
        }
      }
    }
    else {
      this.mvcc.rollback()
    }
    return result
  }

  public async rollback(): Promise<TransactionResult<string, BPTreeNode<K, V>>> {
    return this.mvcc.rollback()
  }
}
