import { BPTreeSync } from '../src/BPTreeSync'
import { BPTreeAsync } from '../src/BPTreeAsync'
import { SerializeStrategySync } from '../src/SerializeStrategySync'
import { SerializeStrategyAsync } from '../src/SerializeStrategyAsync'
import { StringComparator } from '../src/base/ValueComparator'
import { BPTreeNode, SerializeStrategyHead } from '../src/types'

describe('BPTree reload method test', () => {

  // Shared store definition
  class SharedStore {
    public nodes: Record<string, any> = {}
    public head: SerializeStrategyHead = { root: null, order: 3, data: {} }
  }

  // Mock sync strategy that accesses a shared store object
  class SharedMockStrategySync extends SerializeStrategySync<number, string> {
    constructor(private store: SharedStore, order: number) {
      super(order)
      this.head = store.head
    }

    id(isLeaf: boolean): string {
      return this.autoIncrement('index', 1).toString()
    }

    read(id: string): BPTreeNode<number, string> {
      if (!Object.hasOwn(this.store.nodes, id)) {
        throw new Error(`Node ${id} not found`)
      }
      return JSON.parse(JSON.stringify(this.store.nodes[id]))
    }

    write(id: string, node: BPTreeNode<number, string>): void {
      this.store.nodes[id] = node
    }

    delete(id: string): void {
      delete this.store.nodes[id]
    }

    readHead(): SerializeStrategyHead | null {
      if (this.store.head.root === null) return null
      return this.store.head
    }

    writeHead(head: SerializeStrategyHead): void {
      this.store.head = head
      this.head = head
    }
  }

  // Mock async strategy that accesses a shared store object
  class SharedMockStrategyAsync extends SerializeStrategyAsync<number, string> {
    constructor(private store: SharedStore, order: number) {
      super(order)
      this.head = store.head
    }

    async id(isLeaf: boolean): Promise<string> {
      return (await this.autoIncrement('index', 1)).toString()
    }

    async read(id: string): Promise<BPTreeNode<number, string>> {
      if (!Object.hasOwn(this.store.nodes, id)) {
        throw new Error(`Node ${id} not found`)
      }
      return JSON.parse(JSON.stringify(this.store.nodes[id]))
    }

    async write(id: string, node: BPTreeNode<number, string>): Promise<void> {
      this.store.nodes[id] = node
    }

    async delete(id: string): Promise<void> {
      delete this.store.nodes[id]
    }

    async readHead(): Promise<SerializeStrategyHead | null> {
      if (this.store.head.root === null) return null
      return this.store.head
    }

    async writeHead(head: SerializeStrategyHead): Promise<void> {
      this.store.head = head
      this.head = head
    }
  }

  test('BPTreeSync reload should clear cache and re-read from storage', () => {
    const comparator = new StringComparator()
    const sharedStore = new SharedStore()

    // 1. Initialize tree 1
    const tree1 = new BPTreeSync(new SharedMockStrategySync(sharedStore, 3), comparator)
    tree1.init()
    tree1.insert(1, 'A')
    tree1.insert(2, 'B')

    expect(tree1.get(1)).toBe('A')

    // 2. Simulate external modification by another instance using the SAME shared store
    const tree2 = new BPTreeSync(new SharedMockStrategySync(sharedStore, 3), comparator)
    tree2.init()
    tree2.insert(3, 'C') // adds new data
    tree2.delete(1) // deletes existing data

    // 3. Before tree1 reloads, it still has cached data and doesn't see new data
    expect(tree1.get(1)).toBe('A') // Still reads from cache
    expect(tree1.get(3)).toBeUndefined() // Hasn't read new data

    // 4. Reload tree1
    tree1.reload()

    // 5. After reload, tree1 should see the external changes
    expect(tree1.get(1)).toBeUndefined() // External delete reflected
    expect(tree1.get(2)).toBe('B') // Unchanged data
    expect(tree1.get(3)).toBe('C') // External insert reflected
  })

  test('BPTreeAsync reload should clear cache and re-read from storage', async () => {
    const comparator = new StringComparator()
    const sharedStore = new SharedStore()

    // 1. Initialize tree 1
    const tree1 = new BPTreeAsync(new SharedMockStrategyAsync(sharedStore, 3), comparator)
    await tree1.init()
    await tree1.insert(1, 'A')
    await tree1.insert(2, 'B')

    expect(await tree1.get(1)).toBe('A')

    // 2. Simulate external modification by another instance using the SAME shared store
    const tree2 = new BPTreeAsync(new SharedMockStrategyAsync(sharedStore, 3), comparator)
    await tree2.init()
    await tree2.insert(3, 'C') // adds new data
    await tree2.delete(1) // deletes existing data

    // 3. Before tree1 reloads, it still has cached data and doesn't see new data
    expect(await tree1.get(1)).toBe('A') // Still reads from cache
    expect(await tree1.get(3)).toBeUndefined() // Hasn't read new data

    // 4. Reload tree1
    await tree1.reload()

    // 5. After reload, tree1 should see the external changes
    expect(await tree1.get(1)).toBeUndefined() // External delete reflected
    expect(await tree1.get(2)).toBe('B') // Unchanged data
    expect(await tree1.get(3)).toBe('C') // External insert reflected
  })
})
