import { BPTree, NumericComparator, StringComparator, InMemoryStoreStrategy, SerializeStrategy, BPTreeNode, SerializeStrategyHead } from '../'
import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'fs'
import { join } from 'path'

describe('unit-test', () => {
  test('insert:number', () => {
    const tree = new BPTree(
      new InMemoryStoreStrategy(4),
      new NumericComparator()
    )
    tree.insert('a', 1)
    tree.insert('b', 2)
    tree.insert('c', 3)
    tree.insert('d', 4)
    tree.insert('e', 5)
    tree.insert('f', 6)
    tree.insert('g', 7)
    tree.insert('h', 8)
    tree.insert('i', 9)
    tree.insert('j', 10)
    tree.insert('k', 11)
    tree.insert('l', 12)
    tree.insert('m', 13)
    tree.insert('l', 14)
    tree.insert('o', 15)
    tree.insert('p', 16)
    tree.insert('q', 17)
    tree.insert('r', 18)
    tree.insert('s', 19)
    tree.insert('t', 20)
    tree.insert('u', 21)
    tree.insert('v', 22)
    tree.insert('w', 23)
    tree.insert('x', 24)
    tree.insert('y', 25)
    tree.insert('z', 26)
    tree.insert('ㄱ', 28)
    tree.insert('ㄴ', 26)
    tree.insert('ㄷ', 24)
    tree.insert('ㄹ', 22)
    tree.insert('ㅁ', 20)
    tree.insert('ㅂ', 18)
    tree.insert('ㅅ', 16)
    tree.insert('ㅇ', 14)
    tree.insert('ㅈ', 12)
    tree.insert('ㅊ', 10)
    tree.insert('ㅋ', 8)
    tree.insert('ㅌ', 6)
    tree.insert('ㅍ', 4)
    tree.insert('ㅎ', 2)

    expect(tree.where({ equal: 20 })).toEqual([
      { key: 't', value: 20 },
      { key: 'ㅁ', value: 20 },
    ])
    expect(tree.where({ lt: 5 })).toEqual([
      { key: 'a', value: 1 },
      { key: 'b', value: 2 },
      { key: 'ㅎ', value: 2 },
      { key: 'c', value: 3 },
      { key: 'd', value: 4 },
      { key: 'ㅍ', value: 4 },
    ])
    expect(tree.where({ gt: 23 })).toEqual([
      { key: 'x', value: 24 },
      { key: 'ㄷ', value: 24},
      { key: 'y', value: 25 },
      { key: 'z', value: 26},
      { key: 'ㄴ', value: 26},
      { key: 'ㄱ', value: 28},
    ])
    expect(tree.where({ gt: 5, lt: 10 })).toEqual([
      { key: 'f', value: 6 },
      { key: 'ㅌ', value: 6 },
      { key: 'g', value: 7 },
      { key: 'h', value: 8 },
      { key: 'ㅋ', value: 8 },
      { key: 'i', value: 9 },
    ])
  })

  test('insert:string', () => {
    const tree = new BPTree(
      new InMemoryStoreStrategy(5),
      new StringComparator()
    )
    tree.insert('a', 'why')
    tree.insert('b', 'do')
    tree.insert('c', 'cats')
    tree.insert('d', 'sit')
    tree.insert('e', 'on')
    tree.insert('f', 'the')
    tree.insert('g', 'things')
    tree.insert('h', 'we')
    tree.insert('i', 'use')

    expect(tree.where({ equal: 'cats' })).toEqual([
      { key: 'c', value: 'cats' }
    ])
    expect(tree.where({ gt: 'p' })).toEqual([
      { key: 'd', value: 'sit' },
      { key: 'f', value: 'the' },
      { key: 'g', value: 'things' },
      { key: 'i', value: 'use' },
      { key: 'h', value: 'we' },
      { key: 'a', value: 'why' },
    ])
    expect(tree.where({ lt: 'p' })).toEqual([
      { key: 'c', value: 'cats' },
      { key: 'b', value: 'do' },
      { key: 'e', value: 'on' },
    ])
    expect(tree.where({ gt: 'p', lt: 'u' })).toEqual([
      { key: 'd', value: 'sit' },
      { key: 'f', value: 'the' },
      { key: 'g', value: 'things' },
    ])
  })

  test('insert:notEqual', () => {
    const tree = new BPTree(
      new InMemoryStoreStrategy(4),
      new NumericComparator()
    )
    tree.insert('a', 1)
    tree.insert('b', 2)
    tree.insert('c', 3)

    expect(tree.where({ notEqual: 2 })).toEqual([
      { key: 'a', value: 1 },
      { key: 'c', value: 3 },
    ])
  })

  test('delete', () => {
    const tree = new BPTree(
      new InMemoryStoreStrategy(4),
      new NumericComparator()
    )
    tree.insert('a', 1)
    tree.insert('b', 2)
    tree.insert('c', 3)
    tree.insert('d', 4)
    tree.insert('e', 5)
    tree.insert('f', 6)
    tree.insert('g', 7)
    tree.insert('h', 8)
    tree.insert('i', 9)
    tree.insert('j', 10)

    tree.delete('d', 5) // do not work anything
    tree.delete('d', 4)

    expect(tree.where({ equal: 4 })).toEqual([])
    expect(tree.where({ gt: 3 })).toEqual([
      { key: 'e', value: 5 },
      { key: 'f', value: 6 },
      { key: 'g', value: 7 },
      { key: 'h', value: 8 },
      { key: 'i', value: 9 },
      { key: 'j', value: 10 },
    ])
    expect(tree.where({ lt: 6 })).toEqual([
      { key: 'a', value: 1 },
      { key: 'b', value: 2 },
      { key: 'c', value: 3 },
      { key: 'e', value: 5 },
    ])
    expect(tree.where({ gt: 3, lt: 8 })).toEqual([
      { key: 'e', value: 5 },
      { key: 'f', value: 6 },
      { key: 'g', value: 7 },
    ])
  })

  test('delete:notEqual', () => {
    const tree = new BPTree(
      new InMemoryStoreStrategy(4),
      new NumericComparator()
    )
    tree.insert('a', 1)
    tree.insert('b', 2)
    tree.insert('c', 3)
    tree.insert('d', 4)

    tree.delete('c', 3)

    expect(tree.where({ notEqual: 2 })).toEqual([
      { key: 'a', value: 1 },
      { key: 'd', value: 4 },
    ])
  })
})


class FileIOStrategy extends SerializeStrategy<string, number> {
  protected readonly dir: string

  constructor(order: number, dir: string) {
    super(order)
    this.dir = dir
    this._ensureDir(dir)
  }

  private _ensureDir(dir: string): void {
    if (!existsSync(dir)) {
      mkdirSync(dir)
    }
  }

  private _filePath(name: number|string): string {
    return join(this.dir, name.toString())
  }

  id(): number {
    return Math.ceil(Math.random()*(Number.MAX_SAFE_INTEGER-1))
  }

  read(id: number): BPTreeNode<string, number> {
    const raw = readFileSync(this._filePath(id), 'utf8')
    return JSON.parse(raw)
  }

  write(id: number, node: BPTreeNode<string, number>): void {
    const stringify = JSON.stringify(node, null, 2)
    writeFileSync(this._filePath(id), stringify, 'utf8')
  }

  readHead(): SerializeStrategyHead|null {
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

describe('strategy-test', () => {
  test('strategy', () => {
    const storageDirectory = join(__dirname, 'storage')
    const tree = new BPTree(
      new FileIOStrategy(6, storageDirectory),
      new NumericComparator()
    )
    tree.insert('a', 1)
    tree.insert('b', 2)
    tree.insert('c', 3)
    tree.insert('d', 4)
    tree.insert('e', 5)
    tree.insert('f', 6)
    tree.insert('g', 7)
    tree.insert('h', 8)
    tree.insert('i', 9)
    tree.insert('j', 10)

    tree.delete('d', 4)
    tree.delete('g', 7)
    tree.delete('h', 8)

    expect(tree.where({ equal: 4 })).toEqual([])
    expect(tree.where({ equal: 7 })).toEqual([])
    expect(tree.where({ equal: 8 })).toEqual([])
  })
})