import type {
  BPTreeCondition,
  BPTreeConstructorOption,
  BPTreePair,
  BPTreeSearchOption,
  BPTreeUnknownNode,
  SerializableData,
  SerializeStrategyHead,
  IBPTree,
} from './types'
import type { BPTreeNodeOpsAsync, BPTreeAlgoContext } from './base/BPTreeNodeOps'
import { Ryoiki } from 'ryoiki'
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
  initOpAsync
} from './base/BPTreeAlgorithmAsync'
import { SerializeStrategyAsync } from './SerializeStrategyAsync'
import { ValueComparator } from './base/ValueComparator'
import { BPTreeTransaction } from './base/BPTreeTransaction'

export class BPTreePureAsync<K, V> implements IBPTree<K, V> {
  protected readonly strategy: SerializeStrategyAsync<K, V>
  protected readonly comparator: ValueComparator<V>
  protected readonly option: BPTreeConstructorOption
  protected readonly lock: Ryoiki = new Ryoiki()
  private readonly _cachedRegexp: Map<string, RegExp> = new Map()

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

  private _createReadOps(): BPTreeNodeOpsAsync<K, V> {
    const strategy = this.strategy
    let headBuffer: SerializeStrategyHead | null = null
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
        return node
      },
      async updateNode(): Promise<void> { },
      async deleteNode(): Promise<void> { },
      async readHead(): Promise<SerializeStrategyHead | null> {
        if (headBuffer) return headBuffer
        headBuffer = await strategy.readHead()
        return headBuffer
      },
      async writeHead(head: SerializeStrategyHead): Promise<void> {
        headBuffer = head
      },
    }
  }

  private _createBufferedOps(): { ops: BPTreeNodeOpsAsync<K, V>, flush: () => Promise<void> } {
    const strategy = this.strategy
    const writeBuffer = new Map<string, BPTreeUnknownNode<K, V>>()
    const deleteBuffer = new Set<string>()
    let headBuffer: SerializeStrategyHead | null = null

    const ops: BPTreeNodeOpsAsync<K, V> = {
      async getNode(id: string): Promise<BPTreeUnknownNode<K, V>> {
        const buffered = writeBuffer.get(id)
        if (buffered) return buffered
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
        writeBuffer.set(id, node)
        return node
      },
      async updateNode(node: BPTreeUnknownNode<K, V>): Promise<void> {
        writeBuffer.set(node.id, node)
      },
      async deleteNode(node: BPTreeUnknownNode<K, V>): Promise<void> {
        deleteBuffer.add(node.id)
        writeBuffer.delete(node.id)
      },
      async readHead(): Promise<SerializeStrategyHead | null> {
        if (headBuffer) return headBuffer
        headBuffer = await strategy.readHead()
        return headBuffer
      },
      async writeHead(head: SerializeStrategyHead): Promise<void> {
        headBuffer = head
      },
    }

    async function flush(): Promise<void> {
      for (const id of deleteBuffer) {
        await strategy.delete(id)
      }
      for (const [id, node] of writeBuffer) {
        await strategy.write(id, node)
      }
      if (headBuffer) {
        await strategy.writeHead(headBuffer)
      }
      writeBuffer.clear()
      deleteBuffer.clear()
      headBuffer = null
    }

    return { ops, flush }
  }

  private async _readCtx(): Promise<{ rootId: string, order: number }> {
    const head = await this.strategy.readHead()
    if (head === null) {
      throw new Error('Tree not initialized. Call init() first.')
    }
    return { rootId: head.root!, order: head.order }
  }

  private async _createCtx(): Promise<BPTreeAlgoContext<K, V>> {
    const strategy = this.strategy
    const head = await strategy.readHead()
    if (head === null) {
      throw new Error('Tree not initialized. Call init() first.')
    }
    return {
      rootId: head.root!,
      order: head.order,
      headData: () => strategy.head.data,
    }
  }

  protected async writeLock<T>(fn: () => Promise<T>): Promise<T> {
    let lockId: string
    return this.lock.writeLock(async (_lockId: string) => {
      lockId = _lockId
      return fn()
    }).finally(() => {
      this.lock.writeUnlock(lockId)
    })
  }

  public async init(): Promise<void> {
    return this.writeLock(async () => {
      const { ops, flush } = this._createBufferedOps()
      const ctx: BPTreeAlgoContext<K, V> = {
        rootId: '',
        order: this.strategy.order,
        headData: () => this.strategy.head.data,
      }

      await initOpAsync(
        ops,
        ctx,
        this.strategy.order,
        this.strategy.head,
        (head) => { this.strategy.head = head },
      )
      await flush()
    })
  }

  public async getRootNode(): Promise<BPTreeUnknownNode<K, V>> {
    const ctx = await this._readCtx()
    return await this.strategy.read(ctx.rootId) as BPTreeUnknownNode<K, V>
  }

  public async getRootId(): Promise<string> {
    const ctx = await this._readCtx()
    return ctx.rootId
  }

  public async getOrder(): Promise<number> {
    const ctx = await this._readCtx()
    return ctx.order
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
    const ctx = await this._readCtx()
    return getOpAsync(this._createReadOps(), ctx.rootId, key)
  }

  public async exists(key: K, value: V): Promise<boolean> {
    const ctx = await this._readCtx()
    return existsOpAsync(this._createReadOps(), ctx.rootId, key, value, this.comparator)
  }

  public async *keysStream(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>,
  ): AsyncGenerator<K> {
    let lockId: string | undefined
    try {
      lockId = (await this.lock.readLock([0, 0.1], async (id: string) => id)) as string
      const ctx = await this._readCtx()
      yield* keysStreamOpAsync(
        this._createReadOps(), ctx.rootId, condition,
        this.comparator, this._verifierMap, this._searchConfigs,
        this._ensureValues, options,
      )
    } finally {
      if (lockId) this.lock.readUnlock(lockId)
    }
  }

  public async *whereStream(
    condition: BPTreeCondition<V>,
    options?: BPTreeSearchOption<K>,
  ): AsyncGenerator<[K, V]> {
    let lockId: string | undefined
    try {
      lockId = (await this.lock.readLock([0, 0.1], async (id: string) => id)) as string
      const ctx = await this._readCtx()
      yield* whereStreamOpAsync(
        this._createReadOps(), ctx.rootId, condition,
        this.comparator, this._verifierMap, this._searchConfigs,
        this._ensureValues, options,
      )
    } finally {
      if (lockId) this.lock.readUnlock(lockId)
    }
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
    return this.writeLock(async () => {
      const { ops, flush } = this._createBufferedOps()
      const ctx = await this._createCtx()
      await insertOpAsync(ops, ctx, key, value, this.comparator)
      await flush()
    })
  }

  public async delete(key: K, value?: V): Promise<void> {
    return this.writeLock(async () => {
      const { ops, flush } = this._createBufferedOps()
      const ctx = await this._createCtx()
      await deleteOpAsync(ops, ctx, key, this.comparator, value)
      await flush()
    })
  }

  public async batchInsert(entries: [K, V][]): Promise<void> {
    return this.writeLock(async () => {
      const { ops, flush } = this._createBufferedOps()
      const ctx = await this._createCtx()
      await batchInsertOpAsync(ops, ctx, entries, this.comparator)
      await flush()
    })
  }

  public async bulkLoad(entries: [K, V][]): Promise<void> {
    return this.writeLock(async () => {
      const { ops, flush } = this._createBufferedOps()
      const ctx = await this._createCtx()
      await bulkLoadOpAsync(ops, ctx, entries, this.comparator)
      await flush()
    })
  }

  // ─── Head Data ───────────────────────────────────────────────────

  public async getHeadData(): Promise<SerializableData> {
    const head = await this.strategy.readHead()
    if (head === null) throw new Error('Head not found')
    return head.data
  }

  public async setHeadData(data: SerializableData): Promise<void> {
    return this.writeLock(async () => {
      const { ops, flush } = this._createBufferedOps()
      const head = await ops.readHead()
      if (head === null) throw new Error('Head not found')
      await ops.writeHead({ root: head.root, order: head.order, data })
      await flush()
    })
  }

  // ─── Static utilities ────────────────────────────────────────────

  static ChooseDriver = BPTreeTransaction.ChooseDriver
}
