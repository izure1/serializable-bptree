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

  private _createReadOps(): BPTreeNodeOps<K, V> {
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
        return node
      },
      updateNode(): void { },
      deleteNode(): void { },
      readHead(): SerializeStrategyHead | null {
        return strategy.readHead()
      },
      writeHead(): void { },
    }
  }

  private _createBufferedOps(): { ops: BPTreeNodeOps<K, V>, flush: () => void } {
    const strategy = this.strategy
    const writeBuffer = new Map<string, BPTreeUnknownNode<K, V>>()
    const deleteBuffer = new Set<string>()
    let headBuffer: SerializeStrategyHead | null = null

    const ops: BPTreeNodeOps<K, V> = {
      getNode(id: string): BPTreeUnknownNode<K, V> {
        const buffered = writeBuffer.get(id)
        if (buffered) return buffered
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
        writeBuffer.set(id, node)
        return node
      },
      updateNode(node: BPTreeUnknownNode<K, V>): void {
        writeBuffer.set(node.id, node)
      },
      deleteNode(node: BPTreeUnknownNode<K, V>): void {
        deleteBuffer.add(node.id)
        writeBuffer.delete(node.id)
      },
      readHead(): SerializeStrategyHead | null {
        if (headBuffer) return headBuffer
        return strategy.readHead()
      },
      writeHead(head: SerializeStrategyHead): void {
        headBuffer = head
      },
    }

    function flush(): void {
      for (const id of deleteBuffer) {
        strategy.delete(id)
      }
      for (const [id, node] of writeBuffer) {
        strategy.write(id, node)
      }
      if (headBuffer) {
        strategy.writeHead(headBuffer)
      }
    }

    return { ops, flush }
  }

  private _readCtx(): { rootId: string, order: number } {
    const head = this.strategy.readHead()
    if (head === null) {
      throw new Error('Tree not initialized. Call init() first.')
    }
    return { rootId: head.root!, order: head.order }
  }

  private _createCtx(): BPTreeAlgoContext<K, V> {
    const strategy = this.strategy
    const head = strategy.readHead()
    if (head === null) {
      throw new Error('Tree not initialized. Call init() first.')
    }
    return {
      rootId: head.root!,
      order: head.order,
      headData: () => strategy.head.data,
    }
  }

  public init(): void {
    const { ops, flush } = this._createBufferedOps()
    const ctx: BPTreeAlgoContext<K, V> = {
      rootId: '',
      order: this.strategy.order,
      headData: () => this.strategy.head.data,
    }

    initOp(
      ops,
      ctx,
      this.strategy.order,
      this.strategy.head,
      (head) => { this.strategy.head = head },
    )
    flush()
  }

  public getRootNode(): BPTreeUnknownNode<K, V> {
    const ctx = this._readCtx()
    return this.strategy.read(ctx.rootId) as BPTreeUnknownNode<K, V>
  }

  public getRootId(): string {
    const ctx = this._readCtx()
    return ctx.rootId
  }

  public getOrder(): number {
    const ctx = this._readCtx()
    return ctx.order
  }

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
    const ctx = this._readCtx()
    return getOp(this._createReadOps(), ctx.rootId, key)
  }

  public exists(key: K, value: V): boolean {
    const ctx = this._readCtx()
    return existsOp(this._createReadOps(), ctx.rootId, key, value, this.comparator)
  }

  public *keysStream(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>,
  ): Generator<K> {
    const ctx = this._readCtx()
    yield* keysStreamOp(
      this._createReadOps(), ctx.rootId, condition,
      this.comparator, this._verifierMap, this._searchConfigs,
      this._ensureValues, options,
    )
  }

  public *whereStream(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>,
  ): Generator<[K, V]> {
    const ctx = this._readCtx()
    yield* whereStreamOp(
      this._createReadOps(), ctx.rootId, condition,
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
    const { ops, flush } = this._createBufferedOps()
    const ctx = this._createCtx()
    insertOp(ops, ctx, key, value, this.comparator)
    flush()
  }

  public delete(key: K, value?: V): void {
    const { ops, flush } = this._createBufferedOps()
    const ctx = this._createCtx()
    deleteOp(ops, ctx, key, this.comparator, value)
    flush()
  }

  public batchInsert(entries: [K, V][]): void {
    const { ops, flush } = this._createBufferedOps()
    const ctx = this._createCtx()
    batchInsertOp(ops, ctx, entries, this.comparator)
    flush()
  }

  public bulkLoad(entries: [K, V][]): void {
    const { ops, flush } = this._createBufferedOps()
    const ctx = this._createCtx()
    bulkLoadOp(ops, ctx, entries, this.comparator)
    flush()
  }

  // ─── Head Data ───────────────────────────────────────────────────

  public getHeadData(): SerializableData {
    const head = this.strategy.readHead()
    if (head === null) {
      throw new Error('Head not found')
    }
    return head.data
  }

  public setHeadData(data: SerializableData): void {
    const { ops, flush } = this._createBufferedOps()
    const head = ops.readHead()
    if (head === null) {
      throw new Error('Head not found')
    }
    ops.writeHead({
      root: head.root,
      order: head.order,
      data,
    })
    flush()
  }

  // ─── Static utilities ────────────────────────────────────────────

  static ChooseDriver = BPTreeTransaction.ChooseDriver
}
