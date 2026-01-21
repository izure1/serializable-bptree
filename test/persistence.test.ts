import {
  BPTreeSync,
  NumericComparator,
  SerializeStrategySync,
  BPTreeNode,
  SerializeStrategyHead
} from '../src'
import { join } from 'path'
import { writeFileSync, readFileSync, existsSync, mkdirSync, unlinkSync, readdirSync, rmdirSync } from 'fs'
import { randomUUID } from 'crypto'

class FileIOStrategySync extends SerializeStrategySync<string, number> {
  protected readonly dir: string

  constructor(order: number, dir: string) {
    super(order)
    this.dir = dir
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true })
    }
  }

  private _filePath(name: string): string {
    return join(this.dir, name)
  }

  id(isLeaf: boolean): string {
    return randomUUID()
  }

  read(id: string): BPTreeNode<string, number> {
    try {
      const raw = readFileSync(this._filePath(id), 'utf8')
      return JSON.parse(raw)
    } catch (e) {
      throw new Error(`Failed to read node ${id}: ${e}`)
    }
  }

  write(id: string, node: BPTreeNode<string, number>): void {
    const stringify = JSON.stringify(node, null, 2)
    writeFileSync(this._filePath(id), stringify, 'utf8')
  }

  delete(id: string): void {
    if (existsSync(this._filePath(id))) unlinkSync(this._filePath(id))
  }

  writeHead(head: SerializeStrategyHead): void {
    const stringify = JSON.stringify(head, null, 2)
    writeFileSync(this._filePath('head'), stringify, 'utf8')
  }

  readHead(): SerializeStrategyHead | null {
    const filePath = this._filePath('head')
    if (!existsSync(filePath)) {
      return null
    }
    const raw = readFileSync(filePath, 'utf8')
    return JSON.parse(raw)
  }
}

describe('Persistence Test', () => {
  const testDir = join(__dirname, 'temp_persistence_test')

  beforeEach(() => {
    if (existsSync(testDir)) {
      // Simple cleanup
      const files = readdirSync(testDir)
      for (const file of files) {
        unlinkSync(join(testDir, file))
      }
      rmdirSync(testDir)
    }
    mkdirSync(testDir, { recursive: true })
  })

  afterAll(() => {
    if (existsSync(testDir)) {
      const files = readdirSync(testDir)
      for (const file of files) {
        unlinkSync(join(testDir, file))
      }
      rmdirSync(testDir)
    }
  })

  test('Basic Persistence: Data should remain after instance recreation', () => {
    // 1. Create instance and insert data
    let tree = new BPTreeSync(new FileIOStrategySync(4, testDir), new NumericComparator())
    tree.init()

    tree.insert('key1', 1)
    tree.insert('key2', 2)
    tree.insert('key3', 3)

    // 2. Destroy instance (simulate memory wipe by letting go of reference)
    tree = null as any

    // 3. Re-create instance
    const tree2 = new BPTreeSync(new FileIOStrategySync(4, testDir), new NumericComparator())
    tree2.init()

    expect(tree2.get('key1')).toBe(1)
    expect(tree2.get('key2')).toBe(2)
    expect(tree2.get('key3')).toBe(3)
  })

  test('Persistence after Modify: Updates should be persisted', () => {
    // 1. Setup initial data
    let tree = new BPTreeSync(new FileIOStrategySync(4, testDir), new NumericComparator())
    tree.init()
    for (let i = 0; i < 10; i++) {
      tree.insert(i.toString(), i)
    }

    // 2. Re-open and modify
    let tree2 = new BPTreeSync(new FileIOStrategySync(4, testDir), new NumericComparator())
    tree2.init()

    // Delete evens, insert new 100
    for (let i = 0; i < 10; i += 2) {
      tree2.delete(i.toString(), i)
    }
    tree2.insert('100', 100)

    // 3. Re-open and check
    const tree3 = new BPTreeSync(new FileIOStrategySync(4, testDir), new NumericComparator())
    tree3.init()

    for (let i = 0; i < 10; i++) {
      if (i % 2 === 0) {
        expect(tree3.get(i.toString())).toBeUndefined()
      } else {
        expect(tree3.get(i.toString())).toBe(i)
      }
    }
    expect(tree3.get('100')).toBe(100)
  })

  test('No Change on Rollback: Data should not be corrupted by failed transaction', () => {
    let tree = new BPTreeSync(new FileIOStrategySync(4, testDir), new NumericComparator())
    tree.init()
    tree.insert('base', 0)

    const tx = tree.createTransaction()
    tx.insert('temp', 99)
    tx.delete('base', 0)

    // Rollback
    tx.rollback()

    // Base tree check
    expect(tree.get('base')).toBe(0)
    expect(tree.get('temp')).toBeUndefined()

    // Persisted check
    const tree2 = new BPTreeSync(new FileIOStrategySync(4, testDir), new NumericComparator())
    tree2.init()
    expect(tree2.get('base')).toBe(0)
    expect(tree2.get('temp')).toBeUndefined()
  })

  test('Persistence on Concurrent Commit Failure', () => {
    // 1. Setup initial data
    let tree = new BPTreeSync(new FileIOStrategySync(4, testDir), new NumericComparator())
    tree.init()
    tree.insert('shared', -1)

    // 2. Create 5 concurrent transactions
    const txs = Array.from({ length: 5 }, () => tree.createTransaction())

    // 3. Modify data in each transaction
    // All modify 'shared' key -> Conflict
    txs.forEach((tx, i) => {
      tx.delete('shared', -1) // Explicitly delete the old value
      tx.insert('shared', i + 1) // Each tries to set a different value
      tx.insert(`unique_${i}`, i) // And a unique value
    })

    // 4. Commit all
    const results = txs.map(tx => tx.commit())
    const successCount = results.filter(r => r.success).length

    // console.log(`Success count: ${successCount}`)

    // Expect exactly one success
    expect(successCount).toBe(1)

    // Find which one succeeded
    const winnerIndex = results.findIndex(r => r.success)
    const winnerValue = winnerIndex + 1

    // console.log(`Winner Index: ${winnerIndex}, Winner Value: ${winnerValue}`)

    const sharedVal = tree.get('shared')
    // console.log(`Tree get('shared'): ${sharedVal}`)

    console.log(results, winnerIndex, winnerValue)

    expect(sharedVal).toBe(winnerValue)
    expect(tree.get(`unique_${winnerIndex}`)).toBe(winnerIndex)

    // 6. Verify Persistence (New Instance)
    const tree2 = new BPTreeSync(new FileIOStrategySync(4, testDir), new NumericComparator())
    tree2.init()

    expect(tree2.get('shared')).toBe(winnerValue)
    expect(tree2.get(`unique_${winnerIndex}`)).toBe(winnerIndex)

    // Verify losers are NOT applied
    txs.forEach((_, i) => {
      if (i !== winnerIndex) {
        expect(tree2.get(`unique_${i}`)).toBeUndefined()
      }
    })
  })
})
