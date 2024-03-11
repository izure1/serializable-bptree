import { BPTreeNode } from './BPTree'
import type { Json } from '../utils/types'

export interface SerializeStrategyHead {
  root: number
  order: number
  data: Record<string, Json>
}

export abstract class SerializeStrategy<K, V> {
  readonly order: number

  constructor(order: number) {
    this.order = order
  }

  /**
   * The rule for generating node IDs is set.  
   * When a new node is created within the tree, the value returned by this method becomes the node's ID.
   * 
   * **WARNING!** The return value should never be `0`.
   */
  abstract id(): number|Promise<number>

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
}
