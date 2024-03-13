export class CacheStorage<K, V> extends Map<K, V> {
  ensure(key: K, generator: () => V): V {
    if (!this.has(key)) {
      this.set(key, generator())
    }
    return this.get(key) as V
  }
}
