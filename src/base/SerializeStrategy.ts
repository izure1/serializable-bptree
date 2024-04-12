import { BPTreeNode } from './BPTree'
import type { Json } from '../utils/types'

export type SerializableData = Record<string, Json>

export interface SerializeStrategyHead {
  root: number
  order: number
  data: SerializableData
}

export abstract class SerializeStrategy<K, V> {
  readonly order: number
  head: SerializeStrategyHead
  constructor(order: number) {
    this.order = order
    this.head = {
      order,
      root: 0,
      data: {
      }
    }
  }

  /**
   * The rule for generating node IDs is set.  
   * When a new node is created within the tree, the value returned by this method becomes the node's ID.
   * 
   * **WARNING!** The return value should never be `0`.
   * @param isLeaf This is a flag that indicates whether the node is a leaf node or not.
   */
  abstract id(isLeaf: boolean): number|Promise<number>

  /**
   * Read the stored node from the ID.  
   * The JSON object of the read node should be returned.
   * @param id This is the ID of the node to be read.
   */
  abstract read(id: number): BPTreeNode<K, V>|Promise<BPTreeNode<K, V>>

  /**
   * It is called when a node is created or updated and needs to be stored.  
   * The node ID and the node JSON object are passed as parameters. Use this to store the data.
   * @param id This is the ID of the node to be stored.
   * @param node This is the JSON object of the node to be stored.
   */
  abstract write(id: number, node: BPTreeNode<K, V>): void|Promise<void>

  /**
   * This method is called when previously created nodes become no longer needed due to deletion or other processes.  
   * It can be used to free up space by deleting existing stored nodes.
   * @param id This is the ID of the node to be deleted.
   */
  abstract delete(id: number): void|Promise<void>

  /**
   * It is called when the `init` method of the tree instance is called.
   * This method should return the information needed to initialize the tree. This information refers to the values stored in the `writeHead` method.
   * 
   * If it is the initial creation and there is no stored head, it should return `null`.
   * In this case, the tree is created based on the order specified in the strategy instance constructor parameters.
   */
  abstract readHead(): (SerializeStrategyHead|null)|Promise<(SerializeStrategyHead|null)>

  /**
   * It is called when the root node is created or updated.  
   * The method takes the current state of the tree as a parameter. Serialize and store this value. It will be used for the `readHead` method later.
   * @param head This is the current state of the tree.
   */
  abstract writeHead(head: SerializeStrategyHead): void|Promise<void>

  /**
   * Retrieves the data stored in the tree.
   * If there are no values stored in the tree, it returns the `defaultValue`.
   * @param key The key of the data stored in the tree.
   */
  abstract getHeadData(key: string, defaultValue: Json): Json|Promise<Json>

  /**
   * Stores data in the tree.
   * This data is permanently stored in the head.
   * The stored data can be retrieved later using the `getHeadData` method.
   * @param key The key of the data to be stored in the tree.
   * @param data The data to be stored in the tree.
   */
  abstract setHeadData(key: string, data: Json): void|Promise<void>

  /**
   * This method returns a numeric value and increments it by `1`, storing it in the tree's header.  
   * Therefore, when called again, the value incremented by `+1` is returned.
   * 
   * This is a syntactic sugar for using the `setHeadData` and `getHeadData` methods.
   * Therefore, the value specified by this key can be retrieved using the `getHeadData(key)` method or by accessing it directly through `this.head.data[key]`.  
   * It assists in simplifying the implementation of node ID generation in the `id` method.
   * @param key The key of the data to be stored in the tree.
   * @param defaultValue The data to be stored in the tree.
   */
  abstract autoIncrement(key: string, defaultValue: number): number|Promise<number>
}
