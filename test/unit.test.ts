import {
  BPTreeSync,
  BPTreeAsync,
  NumericComparator,
  StringComparator,
  InMemoryStoreStrategySync,
  InMemoryStoreStrategyAsync,
  SerializeStrategySync,
  SerializeStrategyAsync,
  SerializeStrategyHead,
  BPTreeNode,
  ValueComparator
} from 'serializable-bptree'
import {
  readFileSync,
  writeFileSync,
  unlinkSync,
  existsSync,
  mkdirSync,
} from 'node:fs'
import {
  readFile,
  writeFile,
  unlink
} from 'node:fs/promises'
import { randomUUID } from 'node:crypto'
import { join } from 'node:path'

describe('unit-test', () => {
  test('insert:number', () => {
    const tree = new BPTreeSync(
      new InMemoryStoreStrategySync(4),
      new NumericComparator(),
    )
    tree.init()
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

    expect(tree.where({ equal: 20 })).toEqual(new Map([
      ['t', 20],
      ['ㅁ', 20],
    ]))
    expect(tree.where({ lt: 5 })).toEqual(new Map([
      ['a', 1],
      ['b', 2],
      ['ㅎ', 2],
      ['c', 3],
      ['d', 4],
      ['ㅍ', 4],
    ]))
    expect(tree.where({ gt: 23 })).toEqual(new Map([
      ['x', 24],
      ['ㄷ', 24],
      ['y', 25],
      ['z', 26],
      ['ㄴ', 26],
      ['ㄱ', 28],
    ]))
    expect(tree.where({ gt: 5, lt: 10 })).toEqual(new Map([
      ['f', 6],
      ['ㅌ', 6],
      ['g', 7],
      ['h', 8],
      ['ㅋ', 8],
      ['i', 9],
    ]))
    expect(tree.where({ gte: 5, lte: 10 })).toEqual(new Map([
      ['e', 5],
      ['f', 6],
      ['ㅌ', 6],
      ['g', 7],
      ['h', 8],
      ['ㅋ', 8],
      ['i', 9],
      ['j', 10],
      ['ㅊ', 10],
    ]))
    expect(tree.where({ gte: 5, lt: 10 })).toEqual(new Map([
      ['e', 5],
      ['f', 6],
      ['ㅌ', 6],
      ['g', 7],
      ['h', 8],
      ['ㅋ', 8],
      ['i', 9],
    ]))
    expect(tree.where({ gte: 5, lte: 10, equal: 6 })).toEqual(new Map([
      ['f', 6],
      ['ㅌ', 6],
    ]))
    
    tree.clear()
  })

  test('insert:number:async', async () => {
    const tree = new BPTreeAsync(
      new InMemoryStoreStrategyAsync(4),
      new NumericComparator()
    )
    await tree.init()
    await tree.insert('a', 1)
    await tree.insert('b', 2)
    await tree.insert('c', 3)
    await tree.insert('d', 4)
    await tree.insert('e', 5)
    await tree.insert('f', 6)
    await tree.insert('g', 7)
    await tree.insert('h', 8)
    await tree.insert('i', 9)
    await tree.insert('j', 10)
    await tree.insert('k', 11)
    await tree.insert('l', 12)
    await tree.insert('m', 13)
    await tree.insert('l', 14)
    await tree.insert('o', 15)
    await tree.insert('p', 16)
    await tree.insert('q', 17)
    await tree.insert('r', 18)
    await tree.insert('s', 19)
    await tree.insert('t', 20)
    await tree.insert('u', 21)
    await tree.insert('v', 22)
    await tree.insert('w', 23)
    await tree.insert('x', 24)
    await tree.insert('y', 25)
    await tree.insert('z', 26)
    await tree.insert('ㄱ', 28)
    await tree.insert('ㄴ', 26)
    await tree.insert('ㄷ', 24)
    await tree.insert('ㄹ', 22)
    await tree.insert('ㅁ', 20)
    await tree.insert('ㅂ', 18)
    await tree.insert('ㅅ', 16)
    await tree.insert('ㅇ', 14)
    await tree.insert('ㅈ', 12)
    await tree.insert('ㅊ', 10)
    await tree.insert('ㅋ', 8)
    await tree.insert('ㅌ', 6)
    await tree.insert('ㅍ', 4)
    await tree.insert('ㅎ', 2)

    expect(await tree.where({ equal: 20 })).toEqual(new Map([
      ['t', 20],
      ['ㅁ', 20],
    ]))
    expect(await tree.where({ lt: 5 })).toEqual(new Map([
      ['a', 1],
      ['b', 2],
      ['ㅎ', 2],
      ['c', 3],
      ['d', 4],
      ['ㅍ', 4],
    ]))
    expect(await tree.where({ gt: 23 })).toEqual(new Map([
      ['x', 24],
      ['ㄷ', 24],
      ['y', 25],
      ['z', 26],
      ['ㄴ', 26],
      ['ㄱ', 28],
    ]))
    expect(await tree.where({ gt: 5, lt: 10 })).toEqual(new Map([
      ['f', 6],
      ['ㅌ', 6],
      ['g', 7],
      ['h', 8],
      ['ㅋ', 8],
      ['i', 9],
    ]))
    expect(await tree.where({ gte: 5, lte: 10 })).toEqual(new Map([
      ['e', 5],
      ['f', 6],
      ['ㅌ', 6],
      ['g', 7],
      ['h', 8],
      ['ㅋ', 8],
      ['i', 9],
      ['j', 10],
      ['ㅊ', 10],
    ]))
    expect(await tree.where({ gte: 5, lt: 10 })).toEqual(new Map([
      ['e', 5],
      ['f', 6],
      ['ㅌ', 6],
      ['g', 7],
      ['h', 8],
      ['ㅋ', 8],
      ['i', 9],
    ]))
    expect(await tree.where({ gte: 5, lte: 10, equal: 6 })).toEqual(new Map([
      ['f', 6],
      ['ㅌ', 6],
    ]))

    tree.clear()
  })

  test('insert:string', () => {
    const tree = new BPTreeSync(
      new InMemoryStoreStrategySync(5),
      new StringComparator()
    )
    tree.init()
    tree.insert('a', 'why')
    tree.insert('b', 'do')
    tree.insert('c', 'cats')
    tree.insert('d', 'sit')
    tree.insert('e', 'on')
    tree.insert('f', 'the')
    tree.insert('g', 'things')
    tree.insert('h', 'we')
    tree.insert('i', 'use')

    expect(tree.where({ equal: 'cats' })).toEqual(new Map([
      ['c', 'cats']
    ]))
    expect(tree.where({ gt: 'p' })).toEqual(new Map([
      ['d', 'sit'],
      ['f', 'the'],
      ['g', 'things'],
      ['i', 'use'],
      ['h', 'we'],
      ['a', 'why'],
    ]))
    expect(tree.where({ lt: 'p' })).toEqual(new Map([
      ['c', 'cats'],
      ['b', 'do'],
      ['e', 'on'],
    ]))
    expect(tree.where({ gt: 'p', lt: 'u' })).toEqual(new Map([
      ['d', 'sit'],
      ['f', 'the'],
      ['g', 'things'],
    ]))
    expect(tree.where({ like: '%h%' })).toEqual(new Map([
      ['f', 'the'],
      ['g', 'things'],
      ['a', 'why'],
    ]))
    expect(tree.where({ like: '%_s' })).toEqual(new Map([
      ['c', 'cats'],
      ['g', 'things'],
    ]))
    expect(tree.where({ like: 'th%' })).toEqual(new Map([
      ['f', 'the'],
      ['g', 'things'],
    ]))

    tree.clear()
  })

  test('insert:string:async', async () => {
    const tree = new BPTreeAsync<string, string>(
      new InMemoryStoreStrategyAsync(5),
      new StringComparator()
    )
    await tree.init()
    await tree.insert('a', 'why')
    await tree.insert('b', 'do')
    await tree.insert('c', 'cats')
    await tree.insert('d', 'sit')
    await tree.insert('e', 'on')
    await tree.insert('f', 'the')
    await tree.insert('g', 'things')
    await tree.insert('h', 'we')
    await tree.insert('i', 'use')

    expect(await tree.where({ equal: 'cats' })).toEqual(new Map([
      ['c', 'cats']
    ]))
    expect(await tree.where({ gt: 'p' })).toEqual(new Map([
      ['d', 'sit'],
      ['f', 'the'],
      ['g', 'things'],
      ['i', 'use'],
      ['h', 'we'],
      ['a', 'why'],
    ]))
    expect(await tree.where({ lt: 'p' })).toEqual(new Map([
      ['c', 'cats'],
      ['b', 'do'],
      ['e', 'on'],
    ]))
    expect(await tree.where({ gt: 'p', lt: 'u' })).toEqual(new Map([
      ['d', 'sit'],
      ['f', 'the'],
      ['g', 'things'],
    ]))
    expect(await tree.where({ like: '%h%' })).toEqual(new Map([
      ['f', 'the'],
      ['g', 'things'],
      ['a', 'why'],
    ]))
    expect(await tree.where({ like: '%_s' })).toEqual(new Map([
      ['c', 'cats'],
      ['g', 'things'],
    ]))
    expect(await tree.where({ like: 'th%' })).toEqual(new Map([
      ['f', 'the'],
      ['g', 'things'],
    ]))

    tree.clear()
  })

  test('insert:notEqual', () => {
    const tree = new BPTreeSync(
      new InMemoryStoreStrategySync(4),
      new NumericComparator()
    )
    tree.init()
    tree.insert('a', 1)
    tree.insert('b', 2)
    tree.insert('c', 3)

    expect(tree.where({ notEqual: 2 })).toEqual(new Map([
      ['a', 1],
      ['c', 3],
    ]))

    tree.clear()
  })

  test('insert:notEqual:async', async () => {
    const tree = new BPTreeAsync(
      new InMemoryStoreStrategyAsync(4),
      new NumericComparator()
    )
    await tree.init()
    await tree.insert('a', 1)
    await tree.insert('b', 2)
    await tree.insert('c', 3)

    expect(await tree.where({ notEqual: 2 })).toEqual(new Map([
      ['a', 1],
      ['c', 3],
    ]))

    tree.clear()
  })

  test('or condition', () => {
    const tree = new BPTreeSync(
      new InMemoryStoreStrategySync(4),
      new StringComparator()
    )
    tree.init()
    tree.insert('a', 'alpha')
    tree.insert('b', 'bravo')
    tree.insert('c', 'charlie')
    tree.insert('d', 'delta')
    tree.insert('e', 'echo')
    tree.insert('f', 'foxtrot')
    tree.insert('g', 'golf')
    tree.insert('h', 'hotel')
    tree.insert('i', 'india')

    expect(tree.where({ or: ['alpha', 'foxtrot'] })).toEqual(new Map([
      ['a', 'alpha'],
      ['f', 'foxtrot'],
    ]))
    expect(tree.where({ or: ['foxtrot', 'alpha'] })).toEqual(new Map([
      ['a', 'alpha'],
      ['f', 'foxtrot'],
    ]))

    tree.clear()
  })

  test('delete', () => {
    const tree = new BPTreeSync(
      new InMemoryStoreStrategySync(4),
      new NumericComparator()
    )
    tree.init()
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

    expect(tree.where({ equal: 4 })).toEqual(new Map([]))
    expect(tree.where({ gt: 3 })).toEqual(new Map([
      ['e', 5],
      ['f', 6],
      ['g', 7],
      ['h', 8],
      ['i', 9],
      ['j', 10],
    ]))
    expect(tree.where({ lt: 6 })).toEqual(new Map([
      ['a', 1],
      ['b', 2],
      ['c', 3],
      ['e', 5],
    ]))
    expect(tree.where({ gt: 3, lt: 8 })).toEqual(new Map([
      ['e', 5],
      ['f', 6],
      ['g', 7],
    ]))

    tree.clear()
  })

  test('delete:async', async () => {
    const tree = new BPTreeAsync(
      new InMemoryStoreStrategyAsync(4),
      new NumericComparator()
    )
    await tree.init()
    await tree.insert('a', 1)
    await tree.insert('b', 2)
    await tree.insert('c', 3)
    await tree.insert('d', 4)
    await tree.insert('e', 5)
    await tree.insert('f', 6)
    await tree.insert('g', 7)
    await tree.insert('h', 8)
    await tree.insert('i', 9)
    await tree.insert('j', 10)

    await tree.delete('d', 5) // do not work anything
    await tree.delete('d', 4)

    expect(await tree.where({ equal: 4 })).toEqual(new Map([]))
    expect(await tree.where({ gt: 3 })).toEqual(new Map([
      ['e', 5],
      ['f', 6],
      ['g', 7],
      ['h', 8],
      ['i', 9],
      ['j', 10],
    ]))
    expect(await tree.where({ lt: 6 })).toEqual(new Map([
      ['a', 1],
      ['b', 2],
      ['c', 3],
      ['e', 5],
    ]))
    expect(await tree.where({ gt: 3, lt: 8 })).toEqual(new Map([
      ['e', 5],
      ['f', 6],
      ['g', 7],
    ]))

    tree.clear()
  })

  test('delete:notEqual', () => {
    const tree = new BPTreeSync(
      new InMemoryStoreStrategySync(4),
      new NumericComparator()
    )
    tree.init()
    tree.insert('a', 1)
    tree.insert('b', 2)
    tree.insert('c', 3)
    tree.insert('d', 4)

    tree.delete('c', 3)

    expect(tree.where({ notEqual: 2 })).toEqual(new Map([
      ['a', 1],
      ['d', 4],
    ]))

    tree.clear()
  })

  test('delete:notEqual:async', async () => {
    const tree = new BPTreeAsync(
      new InMemoryStoreStrategyAsync(4),
      new NumericComparator()
    )
    await tree.init()
    await tree.insert('a', 1)
    await tree.insert('b', 2)
    await tree.insert('c', 3)
    await tree.insert('d', 4)

    await tree.delete('c', 3)

    expect(await tree.where({ notEqual: 2 })).toEqual(new Map([
      ['a', 1],
      ['d', 4],
    ]))

    tree.clear()
  })
})

class FileIOStrategySync extends SerializeStrategySync<string, number> {
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
    unlinkSync(this._filePath(id))
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

class FileIOStrategyAsync extends SerializeStrategyAsync<string, number> {
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

  private _filePath(name: string): string {
    return join(this.dir, name)
  }

  async id(isLeaf: boolean): Promise<string> {
    return randomUUID()
  }

  async read(id: string): Promise<BPTreeNode<string, number>> {
    const raw = await readFile(this._filePath(id), 'utf8')
    return JSON.parse(raw)
  }

  async write(id: string, node: BPTreeNode<string, number>): Promise<void> {
    const stringify = JSON.stringify(node, null, 2)
    await writeFile(this._filePath(id), stringify, 'utf8')
  }

  async delete(id: string): Promise<void> {
    await unlink(this._filePath(id))
  }

  async readHead(): Promise<SerializeStrategyHead|null> {
    const filePath = this._filePath('head')
    if (!existsSync(filePath)) {
      return null
    }
    const raw = await readFile(filePath, 'utf8')
    return JSON.parse(raw)
  }

  async writeHead(head: SerializeStrategyHead): Promise<void> {
    const stringify = JSON.stringify(head, null, 2)
    await writeFile(this._filePath('head'), stringify, 'utf8')
  }
}

describe('strategy-test', () => {
  test('strategy', () => {
    const storageDirectory = join(__dirname, 'storage')
    const tree = new BPTreeSync(
      new FileIOStrategySync(6, storageDirectory),
      new NumericComparator()
    )
    tree.init()

    const max = 50
    for (let i = 1; i < max; i++) {
      tree.insert(i.toString(), i)
    }
    for (let i = 1; i < max; i++) {
      if (i%3 === 0) {
        tree.delete(i.toString(), i)
      }
    }

    tree.setHeadData({
      ...tree.getHeadData(),
      count: (tree.getHeadData().count as number ?? 0)+1,
      at: Date.now()
    })
    for (let i = 1; i < max; i++) {
      const r = tree.where({ equal: i })
      if (i%3 === 0) {
        expect(r).toEqual(new Map([]))
      }
    }

    tree.clear()
  })

  test('strategy:async', async () => {
    const storageDirectory = join(__dirname, 'storage-async')
    const tree = new BPTreeAsync(
      new FileIOStrategyAsync(6, storageDirectory),
      new NumericComparator()
    )
    await tree.init()

    const max = 50
    for (let i = 1; i < max; i++) {
      await tree.insert(i.toString(), i)
    }
    for (let i = 1; i < max; i++) {
      if (i%3 === 0) {
        await tree.delete(i.toString(), i)
      }
    }

    await tree.setHeadData({
      ...tree.getHeadData(),
      count: (tree.getHeadData().count as number ?? 0)+1,
      at: Date.now()
    })
    for (let i = 1; i < max; i++) {
      const r = await tree.where({ equal: i })
      if (i%3 === 0) {
        expect(r).toEqual(new Map([]))
      }
    }

    tree.clear()
  })
})

interface CompositeValue {
  name: string
  capital: string
}

class CompositeValueComparator extends ValueComparator<CompositeValue> {
  asc(a: CompositeValue, b: CompositeValue): number {
    return a.name.localeCompare(b.name)
  }
  match(value: CompositeValue): string {
    return value.name
  }
}

describe('composite-value-test', () => {
  test('like', () => {
    const tree = new BPTreeSync<number, CompositeValue>(
      new InMemoryStoreStrategySync(4),
      new CompositeValueComparator()
    )
    tree.init()

    const countries = [
      { name: 'Argentina', capital: 'Buenos Aires' }, // 1
      { name: 'Brazil', capital: 'Brasilia' }, // 2
      { name: 'China', capital: 'Beijing' }, // 3
      { name: 'Colombia', capital: 'Bogota' }, // 4
      { name: 'France', capital: 'Paris' }, // 5
      { name: 'Japan', capital: 'Tokyo' }, // 6
      { name: 'Germany', capital: 'Berlin' }, // 7
      { name: 'Italy', capital: 'Rome' }, // 8
      { name: 'Korea', capital: 'Seoul' }, // 9
      { name: 'Portugal', capital: 'Lisbon' }, // 10
      { name: 'Spain', capital: 'Madrid' }, // 11
      { name: 'United States', capital: 'Washington' }, // 12
    ]

    let i = 0
    for (const country of countries) {
      tree.insert(++i, country)
    }

    expect(tree.where({ like: { name: 'J%' } })).toEqual(new Map([
      [6, { name: 'Japan', capital: 'Tokyo' }],
    ]))
    expect(tree.where({ like: { name: 'C%' } })).toEqual(new Map([
      [3, { name: 'China', capital: 'Beijing' }],
      [4, { name: 'Colombia', capital: 'Bogota' }],
    ]))
    expect(tree.where({ like: { name: '%or%' } })).toEqual(new Map([
      [9, { name: 'Korea', capital: 'Seoul' }],
      [10, { name: 'Portugal', capital: 'Lisbon' }],
    ]))
    expect(tree.where({ like: { name: '_r%' } })).toEqual(new Map([
      [1, { name: 'Argentina', capital: 'Buenos Aires' }],
      [2, { name: 'Brazil', capital: 'Brasilia' }],
      [5, { name: 'France', capital: 'Paris' }],
    ]))

    tree.clear()
  })

  test('like:async', async () => {
    const tree = new BPTreeAsync<number, CompositeValue>(
      new InMemoryStoreStrategyAsync(4),
      new CompositeValueComparator()
    )
    await tree.init()

    const countries = [
      { name: 'Argentina', capital: 'Buenos Aires' }, // 1
      { name: 'Brazil', capital: 'Brasilia' }, // 2
      { name: 'China', capital: 'Beijing' }, // 3
      { name: 'Colombia', capital: 'Bogota' }, // 4
      { name: 'France', capital: 'Paris' }, // 5
      { name: 'Japan', capital: 'Tokyo' }, // 6
      { name: 'Germany', capital: 'Berlin' }, // 7
      { name: 'Italy', capital: 'Rome' }, // 8
      { name: 'Korea', capital: 'Seoul' }, // 9
      { name: 'Portugal', capital: 'Lisbon' }, // 10
      { name: 'Spain', capital: 'Madrid' }, // 11
      { name: 'United States', capital: 'Washington' }, // 12
    ]

    let i = 0
    for (const country of countries) {
      await tree.insert(++i, country)
    }

    expect(await tree.where({ like: { name: 'J%' } })).toEqual(new Map([
      [6, { name: 'Japan', capital: 'Tokyo' }],
    ]))
    expect(await tree.where({ like: { name: 'C%' } })).toEqual(new Map([
      [3, { name: 'China', capital: 'Beijing' }],
      [4, { name: 'Colombia', capital: 'Bogota' }],
    ]))
    expect(await tree.where({ like: { name: '%or%' } })).toEqual(new Map([
      [9, { name: 'Korea', capital: 'Seoul' }],
      [10, { name: 'Portugal', capital: 'Lisbon' }],
    ]))
    expect(await tree.where({ like: { name: '_r%' } })).toEqual(new Map([
      [1, { name: 'Argentina', capital: 'Buenos Aires' }],
      [2, { name: 'Brazil', capital: 'Brasilia' }],
      [5, { name: 'France', capital: 'Paris' }],
    ]))

    tree.clear()
  })
})