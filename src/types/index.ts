import type { SyncMVCCTransaction, AsyncMVCCTransaction, MVCCTransaction, MVCCStrategy } from 'mvcc-api'
import { BPTreeMVCCStrategyAsync } from '../transaction/BPTreeMVCCStrategyAsync'
import { BPTreeMVCCStrategySync } from '../transaction/BPTreeMVCCStrategySync'

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

/**
 * Specifies the traversal order for query results.
 * - `'asc'`: Ascending order (default) - traverses from left to right
 * - `'desc'`: Descending order - traverses from right to left
 */
export type BPTreeOrder = 'asc' | 'desc'

export interface BPTreeSearchOption<K> {
  filterValues?: Set<K>
  limit?: number
  order?: BPTreeOrder
}

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

/** Result of a transaction commit operation. */
export interface BPTreeTransactionResult {
  /** Whether the transaction was successfully committed. */
  success: boolean
  /** IDs of new nodes created and written to storage during the transaction. */
  createdIds: string[]
  /** IDs of nodes that became obsolete and can be deleted after a successful commit. */
  obsoleteIds: string[]
  /** Error message if the transaction failed. */
  error?: string
}

export type SerializableData = Record<string, Json>

export interface SerializeStrategyHead {
  root: string | null
  order: number
  data: SerializableData
}

export type BPTreeMVCC<K, V> = MVCCTransaction<
  MVCCStrategy<string, BPTreeNode<K, V>>,
  string,
  BPTreeNode<K, V>
>

export type SyncBPTreeMVCC<K, V> = SyncMVCCTransaction<
  BPTreeMVCCStrategySync<K, V, BPTreeNode<K, V>>,
  string,
  BPTreeNode<K, V>
>

export type AsyncBPTreeMVCC<K, V> = AsyncMVCCTransaction<
  BPTreeMVCCStrategyAsync<K, V, BPTreeNode<K, V>>,
  string,
  BPTreeNode<K, V>
>

export type Primitive = string | number | null | boolean
export type Json = Primitive | Primitive[] | Json[] | { [key: string]: Json }

export interface IBPTree<K, V> {
  /**
   * Returns the root node of the B+Tree.
   * @returns The root node.
   */
  getRootNode(): Deferred<BPTreeUnknownNode<K, V>>

  /**
   * Returns the ID of the root node.
   * @returns The root node ID.
   */
  getRootId(): Deferred<string>

  /**
   * Returns the order of the B+Tree.
   * @returns The order of the tree.
   */
  getOrder(): Deferred<number>

  /**
   * Verified if the value satisfies the condition.
   * 
   * @param nodeValue The value to verify.
   * @param condition The condition to verify against.
   * @returns Returns true if the value satisfies the condition.
   */
  verify(nodeValue: V, condition: BPTreeCondition<V>): boolean

  /**
   * After creating a tree instance, it must be called.  
   * This method is used to initialize the stored tree and recover data.
   * If it is not called, the tree will not function.
   */
  init(): Deferred<void>

  /**
   * Retrieves the value associated with the given key.
   * @param key The key to search for.
   * @returns A Deferred that resolves to the value if found, or undefined if not found.
   */
  get(key: K): Deferred<V | undefined>

  /**
   * Returns a generator that yields keys satisfying the given condition.
   * This is a memory-efficient way to iterate through keys when dealing with large result sets.
   * @param condition The search condition (e.g., gt, lt, equal, like).
   * @param options Search options including filterValues, limit, and order.
   * @returns An async or synchronous generator yielding keys of type K.
   */
  keysStream(condition: BPTreeCondition<V>, options?: BPTreeSearchOption<K>): AsyncGenerator<K> | Generator<K>

  /**
   * Returns a generator that yields [key, value] pairs satisfying the given condition.
   * This is a memory-efficient way to iterate through pairs when dealing with large result sets.
   * @param condition The search condition (e.g., gt, lt, equal, like).
   * @param options Search options including filterValues, limit, and order.
   * @returns An async or synchronous generator yielding [K, V] tuples.
   */
  whereStream(condition: BPTreeCondition<V>, options?: BPTreeSearchOption<K>): AsyncGenerator<[K, V]> | Generator<[K, V]>

  /**
   * It searches for a key within the tree. The result is returned as an array sorted in ascending order based on the value.  
   * The result is key set instance, and you can use the `gt`, `lt`, `gte`, `lte`, `equal`, `notEqual`, `like` condition statements.
   * This method operates much faster than first searching with `where` and then retrieving only the key list.
   * @param condition You can use the `gt`, `lt`, `gte`, `lte`, `equal`, `notEqual`, `like` condition statements.
   * @param options Search options including filterValues, limit, and order.
   */
  keys(condition: BPTreeCondition<V>, options?: BPTreeSearchOption<K>): Deferred<Set<K>>

  /**
   * It searches for a value within the tree. The result is returned as an array sorted in ascending order based on the value.  
   * The result includes the key and value attributes, and you can use the `gt`, `lt`, `gte`, `lte`, `equal`, `notEqual`, `like` condition statements.
   * @param condition You can use the `gt`, `lt`, `gte`, `lte`, `equal`, `notEqual`, `like` condition statements.
   * @param options Search options including filterValues, limit, and order.
   */
  where(condition: BPTreeCondition<V>, options?: BPTreeSearchOption<K>): Deferred<BPTreePair<K, V>>

  /**
   * You enter the key and value as a pair. You can later search for the pair by value.
   * This data is stored in the tree, sorted in ascending order of value.
   * @param key The key of the pair. This key must be unique.
   * @param value The value of the pair.
   */
  insert(key: K, value: V): Deferred<void>

  /**
   * Inserts multiple key-value pairs into the tree in a single batch operation.
   * Entries are sorted by value before insertion to optimize tree traversal.
   * This is more efficient than calling insert() multiple times.
   * @param entries Array of [key, value] pairs to insert.
   */
  batchInsert(entries: [K, V][]): Deferred<void>

  /**
   * Builds a B+Tree from scratch using bulk loading (bottom-up construction).
   * This is significantly faster than batchInsert for initial tree construction,
   * as it avoids top-down traversal and creates nodes directly.
   * 
   * **Precondition**: The tree must be empty. If the tree already contains data,
   * an error will be thrown. Use batchInsert for adding data to an existing tree.
   * 
   * @param entries Array of [key, value] pairs to bulk load.
   * @throws Error if the tree is not empty.
   */
  bulkLoad(entries: [K, V][]): Deferred<void>

  /**
   * Deletes the pair that matches the key and value.
   * @param key The key of the pair. This key must be unique.
   * @param value The value of the pair.
   * @warning If the 'value' is not specified, a full scan will be performed to find the value associated with the key, which may lead to performance degradation.
   */
  delete(key: K, value?: V): Deferred<void>

  /**
   * Deletes multiple key-value pairs from the tree in a single batch operation.
   * This is more efficient than calling delete() multiple times.
   * @param entries Array of [key, value?] pairs to delete. If value is omitted, a full scan will be performed.
   */
  batchDelete(entries: [K, V?][]): Deferred<void>

  /**
   * It returns whether there is a value in the tree.
   * @param key The key value to search for. This key must be unique.
   * @param value The value to search for.
   */
  exists(key: K, value: V): Deferred<boolean>

  /**
   * Inserts user-defined data into the head of the tree.
   * This feature is useful when you need to store separate, non-volatile information in the tree.
   * For example, you can store information such as the last update time and the number of insertions.
   * @param data User-defined data to be stored in the head of the tree.
   */
  setHeadData(data: SerializableData): Deferred<void>

  /**
   * Returns the user-defined data stored in the head of the tree.
   */
  getHeadData(): Deferred<SerializableData>
}

