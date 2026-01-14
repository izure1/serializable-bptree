
import {
  BPTreeSync,
  NumericComparator,
  SerializeStrategySync,
  BPTreeNode,
  SerializeStrategyHead
} from '../src'
import { join } from 'path'
import { writeFileSync, readFileSync, existsSync, mkdirSync, unlinkSync, readdirSync } from 'fs'
import { randomUUID } from 'crypto'

class FileIOStrategySync extends SerializeStrategySync<string, number> {
  protected readonly dir: string

  constructor(order: number, dir: string) {
    super(order)
    this.dir = dir
    if (!existsSync(dir)) {
      mkdirSync(dir)
    }
  }

  private _filePath(name: string): string {
    return join(this.dir, name)
  }

  id(isLeaf: boolean): string {
    return randomUUID()
  }

  read(id: string): BPTreeNode<string, number> {
    const raw = readFileSync(this._filePath(id), 'utf8')
    return JSON.parse(raw)
  }

  write(id: string, node: BPTreeNode<string, number>): void {
    const stringify = JSON.stringify(node, null, 2)
    writeFileSync(this._filePath(id), stringify, 'utf8')
  }

  delete(id: string): void {
    if (existsSync(this._filePath(id))) unlinkSync(this._filePath(id))
  }

  readHead(): SerializeStrategyHead | null {
    const filePath = this._filePath('head')
    if (!existsSync(filePath)) {
      return null
    }
    const raw = readFileSync(filePath, 'utf8')
    return JSON.parse(raw)
  }

  writeHead(head: SerializeStrategyHead): void {
    const stringify = JSON.stringify(head, null, 2)
    writeFileSync(this._filePath('head'), stringify, 'utf8')
  }
}

describe('file-order-test', () => {
  const testDir = join(__dirname, 'temp_storage_order_test')

  test('verify-file-content-order', () => {
    const tree = new BPTreeSync(
      new FileIOStrategySync(4, testDir),
      new NumericComparator()
    )
    tree.init()

    // Insert enough data to create depth
    for (let i = 1; i <= 200; i++) {
      tree.insert((Math.random() * 100000).toString(), i)
    }

    // Now inspect files directly
    const files = readdirSync(testDir)
    let checkedCount = 0
    for (const file of files) {
      if (file === 'head') continue
      const content = readFileSync(join(testDir, file), 'utf8')
      const node = JSON.parse(content)

      // Check sorting in the FILE CONTENT
      if (node.values && node.values.length > 1) {
        for (let i = 0; i < node.values.length - 1; i++) {
          if (node.values[i] > node.values[i + 1]) {
            const msg = `File ${file} (Leaf: ${node.leaf}) has UNSORTED values: ${JSON.stringify(node.values)}`
            console.error(msg)
            writeFileSync(join(testDir, '_error.log'), msg, 'utf8')
            throw new Error(`File ${file} content is NOT sorted.`)
          }
        }
        checkedCount++
      }
    }
    console.log(`Checked ${checkedCount} nodes from disk. All sorted.`)
  })
})
