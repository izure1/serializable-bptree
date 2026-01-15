import fs from 'node:fs'
import path from 'node:path'
import { BPTreeAsync, BPTreeNode, NumericComparator, SerializeStrategyAsync, SerializeStrategyHead } from '../src'


const testDir = path.join(__dirname, 'temp_storage_split_node_test')

function delay(t: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, t))
}

class FileSerializeStrategyAsync extends SerializeStrategyAsync<number, number> {
  async id(isLeaf: boolean): Promise<string> {
    await delay(500)
    return Date.now().toString()
  }
  async read(id: string): Promise<BPTreeNode<number, number>> {
    const filePath = path.join(testDir, id + '.json')
    const raw = await fs.promises.readFile(filePath, 'utf8')
    return JSON.parse(raw)
  }
  async write(id: string, node: BPTreeNode<number, number>): Promise<void> {
    await fs.promises.writeFile(path.join(testDir, id + '.json'), JSON.stringify(node, null, 2))
  }
  async delete(id: string): Promise<void> {
    // throw new Error('Method not implemented.')
  }
  async readHead(): Promise<SerializeStrategyHead | null> {
    const filePath = path.join(testDir, 'head.json')
    if (!fs.existsSync(filePath)) {
      return null
    }
    const raw = await fs.promises.readFile(filePath, 'utf8')
    return JSON.parse(raw)
  }
  async writeHead(head: SerializeStrategyHead): Promise<void> {
    await fs.promises.writeFile(path.join(testDir, 'head.json'), JSON.stringify(head, null, 2))
  }
}

describe('split-node-test', () => {

  beforeAll(async () => {
    if (fs.existsSync(testDir)) {
      fs.rmSync(testDir, { recursive: true })
    }
    fs.mkdirSync(testDir)
  })

  test('split-node-test', async () => {
    const tree = new BPTreeAsync(
      new FileSerializeStrategyAsync(504),
      new NumericComparator()
    )

    await tree.init()

    const tx = await tree.createTransaction()
    for (let i = 1; i <= 1200; i++) {
      const v = i * 1000
      await tx.insert(v, i)
    }
    await tx.commit()

    tree.clear()
  }, 100000)

})