export class InvertedWeakMap<K extends string|number|symbol, V extends WeakKey> {
  private readonly _map: Map<K, WeakRef<V>>
  private readonly _registry: FinalizationRegistry<K>

  constructor() {
    this._map = new Map()
    this._registry = new FinalizationRegistry((key) => this._map.delete(key))
  }

  clear(): void {
    return this._map.clear()
  }

  delete(key: K): boolean {
    return this._map.delete(key)
  }

  get(key: K): V|undefined {
    return this._map.get(key)?.deref()
  }

  has(key: K): boolean {
    return this._map.has(key) && this.get(key) !== undefined
  }

  set(key: K, value: V): this {
    this._map.set(key, new WeakRef(value))
    this._registry.register(value, key)
    return this
  }

  get size(): number {
    return this._map.size
  }

  keys(): IterableIterator<K> {
    return this._map.keys()
  }
}
