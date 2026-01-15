export type Sync<T> = T
export type Async<T> = Promise<T>
export type Deferred<T> = Sync<T> | Async<T>

export type BPTreeNodeKey<K> = string | K
export type BPTreeCondition<V> = Partial<{
  /** Searches for pairs greater than the given value. */
  gt: Partial<V>
  /** Searches for pairs less than the given value. */
  lt: Partial<V>
  /** Searches for pairs greater than or equal to the given value. */
  gte: Partial<V>
  /** Searches for pairs less than or equal to the given value. */
  lte: Partial<V>
  /** "Searches for pairs equal to the given value. */
  equal: Partial<V>
  /** Searches for pairs not equal to the given value. */
  notEqual: Partial<V>
  /** Searches for pairs that satisfy at least one of the conditions. */
  or: Partial<V>[]
  /** Searches for values matching the given pattern. '%' matches zero or more characters, and '_' matches exactly one character. */
  /** 
   * Searches for pairs where the primary field equals the given value.
   * Uses `primaryAsc` method for comparison, which compares only the primary sorting field.
   * Useful for composite values where you want to find all entries with the same primary value.
   */
  primaryEqual: Partial<V>
  /** Searches for pairs where the primary field is greater than the given value. */
  primaryGt: Partial<V>
  /** Searches for pairs where the primary field is greater than or equal to the given value. */
  primaryGte: Partial<V>
  /** Searches for pairs where the primary field is less than the given value. */
  primaryLt: Partial<V>
  /** Searches for pairs where the primary field is less than or equal to the given value. */
  primaryLte: Partial<V>
  /** Searches for pairs where the primary field is not equal to the given value. */
  primaryNotEqual: Partial<V>
  /** Searches for pairs where the primary field matches at least one of the given values. */
  primaryOr: Partial<V>[]
  /** 
   * Searches for values matching the given pattern on the primary field. 
   * Uses `match` method for getting string representation.
   * '%' matches zero or more characters, and '_' matches exactly one character. 
   */
  like: string
}>
export type BPTreePair<K, V> = Map<K, V>

export type BPTreeUnknownNode<K, V> = BPTreeInternalNode<K, V> | BPTreeLeafNode<K, V>

export interface BPTreeConstructorOption {
  /**
   * The capacity of the cache.
   * This value is used to determine how many nodes can be cached.
   * If not specified, the default value is 1000.
   */
  capacity?: number
}

export interface BPTreeNode<K, V> {
  id: string
  keys: string[] | K[][],
  values: V[],
  leaf: boolean
  parent: string | null
  next: string | null
  prev: string | null
}

export interface BPTreeInternalNode<K, V> extends BPTreeNode<K, V> {
  leaf: false
  keys: string[]
}

export interface BPTreeLeafNode<K, V> extends BPTreeNode<K, V> {
  leaf: true
  keys: K[][]
}

export type SerializableData = Record<string, Json>

export interface SerializeStrategyHead {
  root: string | null
  order: number
  data: SerializableData
}

type Primitive = string | number | null | boolean
type Json = Primitive | Primitive[] | Json[] | { [key: string]: Json }

export type { Json }

export interface Transaction<K, V> {
  commit(): Deferred<void>
  rollback(): Deferred<void>
}
