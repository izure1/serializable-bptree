import type { TransactionResult } from 'mvcc-api'
import type {
  BPTreeCondition,
  BPTreeConstructorOption,
  BPTreeNode,
  BPTreeNodeKey,
  BPTreePair,
  BPTreeUnknownNode,
  SerializableData,
  SerializeStrategyHead,
  SyncBPTreeMVCC,
  BPTreeSearchOption,
  BPTreeLeafNode
} from '../types'
import type { BPTreeNodeOps, BPTreeAlgoContext } from '../base/BPTreeNodeOps'
import {
  insertOp,
  deleteOp,
  batchInsertOp,
  bulkLoadOp,
  existsOp,
  getOp,
  whereStreamOp,
  keysStreamOp,
  createVerifierMap,
  createSearchConfigs,
  initOp,
  insertAtLeaf,
  insertInParent,
  deleteEntry,
  locateLeaf,
  findLowerBoundLeaf,
  findUpperBoundLeaf,
  findOuterBoundaryLeaf,
  leftestNode,
  rightestNode,
  getPairsGenerator
} from '../base/BPTreeAlgorithmSync'
import { BPTreeTransaction } from '../base/BPTreeTransaction'
import { SerializeStrategySync } from '../SerializeStrategySync'
import { ValueComparator } from '../base/ValueComparator'

export class BPTreeSyncTransaction<K, V> extends BPTreeTransaction<K, V> {
  declare protected readonly rootTx: BPTreeSyncTransaction<K, V>
  declare protected readonly mvccRoot: SyncBPTreeMVCC<K, V>
  declare protected readonly mvcc: SyncBPTreeMVCC<K, V>
  declare protected readonly strategy: SerializeStrategySync<K, V>
  declare protected readonly comparator: ValueComparator<V>
  declare protected readonly option: BPTreeConstructorOption

  private _ops!: BPTreeNodeOps<K, V>
  private _ctx!: BPTreeAlgoContext<K, V>
  private _verifierMapCached!: ReturnType<typeof createVerifierMap<V>>
  private _searchConfigsCached!: ReturnType<typeof createSearchConfigs<K, V>>

  constructor(
    rootTx: BPTreeSyncTransaction<K, V>,
    mvccRoot: SyncBPTreeMVCC<K, V>,
    mvcc: SyncBPTreeMVCC<K, V>,
    strategy: SerializeStrategySync<K, V>,
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
    this._initAlgoContext()
  }

  private _initAlgoContext(): void {
    const mvcc = this.mvcc
    const strategy = this.strategy
    const self = this

    this._ops = {
      getNode(id: string): BPTreeUnknownNode<K, V> {
        return mvcc.read(id) as BPTreeUnknownNode<K, V>
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
        mvcc.create(id, node)
        return node
      },
      updateNode(node: BPTreeUnknownNode<K, V>): void {
        if (mvcc.isDeleted(node.id)) {
          return
        }
        mvcc.write(node.id, node)
      },
      deleteNode(node: BPTreeUnknownNode<K, V>): void {
        if (mvcc.isDeleted(node.id)) {
          return
        }
        mvcc.delete(node.id)
      },
      readHead(): SerializeStrategyHead | null {
        return mvcc.read('__HEAD__') as unknown as SerializeStrategyHead | null
      },
      writeHead(head: SerializeStrategyHead): void {
        if (!mvcc.exists('__HEAD__')) {
          mvcc.create('__HEAD__', head as any)
        }
        else {
          mvcc.write('__HEAD__', head as any)
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
    this._searchConfigsCached = createSearchConfigs(this.comparator, ensureValues)
  }

  public getRootNode(): BPTreeUnknownNode<K, V> {
    return this.getNode(this.rootId)
  }

  // ─── Legacy protected methods (delegating to ops) ────────────────

  protected getNode(id: string): BPTreeUnknownNode<K, V> {
    return this._ops.getNode(id)
  }

  protected _createNode(
    leaf: boolean,
    keys: string[] | K[][],
    values: V[],
    parent: string | null = null,
    next: string | null = null,
    prev: string | null = null
  ): BPTreeUnknownNode<K, V> {
    return this._ops.createNode(leaf, keys, values, parent, next, prev)
  }

  protected _updateNode(node: BPTreeUnknownNode<K, V>): void {
    this._ops.updateNode(node)
  }

  protected _deleteNode(node: BPTreeUnknownNode<K, V>): void {
    this._ops.deleteNode(node)
  }

  protected _readHead(): SerializeStrategyHead | null {
    return this._ops.readHead()
  }

  protected _writeHead(head: SerializeStrategyHead): void {
    this._ops.writeHead(head)
  }

  // ─── Tree traversal (delegating to algorithm) ────────────────────

  protected _insertAtLeaf(node: BPTreeUnknownNode<K, V>, key: BPTreeNodeKey<K>, value: V): BPTreeUnknownNode<K, V> {
    return insertAtLeaf(this._ops, node, key, value, this.comparator)
  }

  protected _insertInParent(node: BPTreeUnknownNode<K, V>, value: V, newSiblingNode: BPTreeUnknownNode<K, V>): void {
    insertInParent(this._ops, this._ctx, node, value, newSiblingNode)
  }

  protected locateLeaf(value: V): BPTreeLeafNode<K, V> {
    return locateLeaf(this._ops, this._ctx.rootId, value, this.comparator)
  }

  protected findLowerBoundLeaf(value: V): BPTreeLeafNode<K, V> {
    return findLowerBoundLeaf(this._ops, this._ctx.rootId, value, this.comparator)
  }

  protected findUpperBoundLeaf(value: V): BPTreeLeafNode<K, V> {
    return findUpperBoundLeaf(this._ops, this._ctx.rootId, value, this.comparator)
  }

  protected findOuterBoundaryLeaf(value: V, direction: 1 | -1): BPTreeLeafNode<K, V> | null {
    return findOuterBoundaryLeaf(this._ops, this._ctx.rootId, value, direction, this.comparator)
  }

  protected leftestNode(): BPTreeLeafNode<K, V> {
    return leftestNode(this._ops, this._ctx.rootId)
  }

  protected rightestNode(): BPTreeLeafNode<K, V> {
    return rightestNode(this._ops, this._ctx.rootId)
  }

  protected *getPairsGenerator(
    startNode: Parameters<typeof getPairsGenerator<K, V>>[1],
    endNode: Parameters<typeof getPairsGenerator<K, V>>[2],
    direction: 1 | -1,
  ): Generator<[BPTreeNodeKey<K>, V], void, unknown> {
    yield* getPairsGenerator(this._ops, startNode, endNode, direction)
  }

  // ─── Lifecycle ───────────────────────────────────────────────────

  public init(): void {
    if (this.rootTx !== this) {
      throw new Error('Cannot call init on a nested transaction')
    }
    this._initInternal()
  }

  protected _initInternal(): void {
    if (this.isInitialized) {
      throw new Error('Transaction already initialized')
    }
    if (this.isDestroyed) {
      throw new Error('Transaction already destroyed')
    }
    this.isInitialized = true
    try {
      this._clearCache()
      initOp(
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

  public reload(): void {
    if (this.rootTx !== this) {
      throw new Error('Cannot call reload on a nested transaction')
    }
    this._reloadInternal()
  }

  protected _reloadInternal(): void {
    this._resetForReload()
    this._initInternal()
  }

  // ─── Query (delegating to algorithm) ─────────────────────────────

  public exists(key: K, value: V): boolean {
    return existsOp(this._ops, this._ctx.rootId, key, value, this.comparator)
  }

  public get(key: K): V | undefined {
    return getOp(this._ops, this._ctx.rootId, key)
  }

  public *keysStream(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>
  ): Generator<K> {
    yield* keysStreamOp(
      this._ops, this._ctx.rootId, condition,
      this.comparator, this._verifierMapCached, this._searchConfigsCached,
      this.ensureValues.bind(this), options,
    )
  }

  public *whereStream(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>
  ): Generator<[K, V]> {
    yield* whereStreamOp(
      this._ops, this._ctx.rootId, condition,
      this.comparator, this._verifierMapCached, this._searchConfigsCached,
      this.ensureValues.bind(this), options,
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

  // ─── Mutation (delegating to algorithm) ──────────────────────────

  public insert(key: K, value: V): void {
    insertOp(this._ops, this._ctx, key, value, this.comparator)
  }

  public batchInsert(entries: [K, V][]): void {
    batchInsertOp(this._ops, this._ctx, entries, this.comparator)
  }

  public bulkLoad(entries: [K, V][]): void {
    bulkLoadOp(this._ops, this._ctx, entries, this.comparator)
  }

  protected _deleteEntry(
    node: BPTreeUnknownNode<K, V>,
    key: BPTreeNodeKey<K>
  ): BPTreeUnknownNode<K, V> {
    return deleteEntry(this._ops, this._ctx, node, key, this.comparator)
  }

  public delete(key: K, value?: V): void {
    deleteOp(this._ops, this._ctx, key, this.comparator, value)
  }

  // ─── Head Data ───────────────────────────────────────────────────

  public getHeadData(): SerializableData {
    const head = this._readHead()
    if (head === null) {
      throw new Error('Head not found')
    }
    return head.data
  }

  public setHeadData(data: SerializableData): void {
    const head = this._readHead()
    if (head === null) {
      throw new Error('Head not found')
    }
    this._writeHead({
      root: head.root,
      order: head.order,
      data,
    })
  }

  // ─── Transaction ─────────────────────────────────────────────────

  public commit(label?: string): TransactionResult<string, BPTreeNode<K, V>> {
    let result = this.mvcc.commit(label)
    if (result.success) {
      const isRootTx = this.rootTx === this
      if (!isRootTx) {
        result = this.rootTx.commit(label)
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

  public rollback(): TransactionResult<string, BPTreeNode<K, V>> {
    return this.mvcc.rollback()
  }
}
