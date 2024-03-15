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
} from '../'
import {
  readFileSync,
  writeFileSync,
  existsSync,
  mkdirSync,
  rmSync
} from 'fs'
import {
  readFile,
  writeFile,
} from 'fs/promises'
import { join } from 'path'

describe('unit-test', () => {
  test('insert:number', () => {
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
    expect(tree.where({ gte: 5, lte: 10 })).toEqual([
      { key: 'e', value: 5 },
      { key: 'f', value: 6 },
      { key: 'ㅌ', value: 6 },
      { key: 'g', value: 7 },
      { key: 'h', value: 8 },
      { key: 'ㅋ', value: 8 },
      { key: 'i', value: 9 },
      { key: 'j', value: 10 },
      { key: 'ㅊ', value: 10 },
    ])
    expect(tree.where({ gte: 5, lt: 10 })).toEqual([
      { key: 'e', value: 5 },
      { key: 'f', value: 6 },
      { key: 'ㅌ', value: 6 },
      { key: 'g', value: 7 },
      { key: 'h', value: 8 },
      { key: 'ㅋ', value: 8 },
      { key: 'i', value: 9 },
    ])
    expect(tree.where({ gte: 5, lte: 10, equal: 6 })).toEqual([
      { key: 'f', value: 6 },
      { key: 'ㅌ', value: 6 },
    ])

    console.log(tree.keys({ gt: 0, lt: 10 }))
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

    expect(await tree.where({ equal: 20 })).toEqual([
      { key: 't', value: 20 },
      { key: 'ㅁ', value: 20 },
    ])
    expect(await tree.where({ lt: 5 })).toEqual([
      { key: 'a', value: 1 },
      { key: 'b', value: 2 },
      { key: 'ㅎ', value: 2 },
      { key: 'c', value: 3 },
      { key: 'd', value: 4 },
      { key: 'ㅍ', value: 4 },
    ])
    expect(await tree.where({ gt: 23 })).toEqual([
      { key: 'x', value: 24 },
      { key: 'ㄷ', value: 24},
      { key: 'y', value: 25 },
      { key: 'z', value: 26},
      { key: 'ㄴ', value: 26},
      { key: 'ㄱ', value: 28},
    ])
    expect(await tree.where({ gt: 5, lt: 10 })).toEqual([
      { key: 'f', value: 6 },
      { key: 'ㅌ', value: 6 },
      { key: 'g', value: 7 },
      { key: 'h', value: 8 },
      { key: 'ㅋ', value: 8 },
      { key: 'i', value: 9 },
    ])
    expect(await tree.where({ gte: 5, lte: 10 })).toEqual([
      { key: 'e', value: 5 },
      { key: 'f', value: 6 },
      { key: 'ㅌ', value: 6 },
      { key: 'g', value: 7 },
      { key: 'h', value: 8 },
      { key: 'ㅋ', value: 8 },
      { key: 'i', value: 9 },
      { key: 'j', value: 10 },
      { key: 'ㅊ', value: 10 },
    ])
    expect(await tree.where({ gte: 5, lt: 10 })).toEqual([
      { key: 'e', value: 5 },
      { key: 'f', value: 6 },
      { key: 'ㅌ', value: 6 },
      { key: 'g', value: 7 },
      { key: 'h', value: 8 },
      { key: 'ㅋ', value: 8 },
      { key: 'i', value: 9 },
    ])
    expect(await tree.where({ gte: 5, lte: 10, equal: 6 })).toEqual([
      { key: 'f', value: 6 },
      { key: 'ㅌ', value: 6 },
    ])

    console.log(await tree.keys({ gt: 0, lt: 10 }))
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
    expect(tree.where({ like: '%h%' })).toEqual([
      { key: 'f', value: 'the' },
      { key: 'g', value: 'things' },
      { key: 'a', value: 'why' },
    ])
    expect(tree.where({ like: '%_s' })).toEqual([
      { key: 'c', value: 'cats' },
      { key: 'g', value: 'things' },
    ])
    expect(tree.where({ like: 'th%' })).toEqual([
      { key: 'f', value: 'the' },
      { key: 'g', value: 'things' },
    ])
  })

  test('insert:string:async', async () => {
    const tree = new BPTreeAsync(
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

    expect(await tree.where({ equal: 'cats' })).toEqual([
      { key: 'c', value: 'cats' }
    ])
    expect(await tree.where({ gt: 'p' })).toEqual([
      { key: 'd', value: 'sit' },
      { key: 'f', value: 'the' },
      { key: 'g', value: 'things' },
      { key: 'i', value: 'use' },
      { key: 'h', value: 'we' },
      { key: 'a', value: 'why' },
    ])
    expect(await tree.where({ lt: 'p' })).toEqual([
      { key: 'c', value: 'cats' },
      { key: 'b', value: 'do' },
      { key: 'e', value: 'on' },
    ])
    expect(await tree.where({ gt: 'p', lt: 'u' })).toEqual([
      { key: 'd', value: 'sit' },
      { key: 'f', value: 'the' },
      { key: 'g', value: 'things' },
    ])
    expect(await tree.where({ like: '%h%' })).toEqual([
      { key: 'f', value: 'the' },
      { key: 'g', value: 'things' },
      { key: 'a', value: 'why' },
    ])
    expect(await tree.where({ like: '%_s' })).toEqual([
      { key: 'c', value: 'cats' },
      { key: 'g', value: 'things' },
    ])
    expect(await tree.where({ like: 'th%' })).toEqual([
      { key: 'f', value: 'the' },
      { key: 'g', value: 'things' },
    ])
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

    expect(tree.where({ notEqual: 2 })).toEqual([
      { key: 'a', value: 1 },
      { key: 'c', value: 3 },
    ])
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

    expect(await tree.where({ notEqual: 2 })).toEqual([
      { key: 'a', value: 1 },
      { key: 'c', value: 3 },
    ])
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

    expect(await tree.where({ equal: 4 })).toEqual([])
    expect(await tree.where({ gt: 3 })).toEqual([
      { key: 'e', value: 5 },
      { key: 'f', value: 6 },
      { key: 'g', value: 7 },
      { key: 'h', value: 8 },
      { key: 'i', value: 9 },
      { key: 'j', value: 10 },
    ])
    expect(await tree.where({ lt: 6 })).toEqual([
      { key: 'a', value: 1 },
      { key: 'b', value: 2 },
      { key: 'c', value: 3 },
      { key: 'e', value: 5 },
    ])
    expect(await tree.where({ gt: 3, lt: 8 })).toEqual([
      { key: 'e', value: 5 },
      { key: 'f', value: 6 },
      { key: 'g', value: 7 },
    ])
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

    expect(tree.where({ notEqual: 2 })).toEqual([
      { key: 'a', value: 1 },
      { key: 'd', value: 4 },
    ])
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

    expect(await tree.where({ notEqual: 2 })).toEqual([
      { key: 'a', value: 1 },
      { key: 'd', value: 4 },
    ])
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

  private _filePath(name: number|string): string {
    return join(this.dir, name.toString())
  }

  id(): number {
    return this.autoIncrement('index', 1)
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

  private _filePath(name: number|string): string {
    return join(this.dir, name.toString())
  }

  async id(): Promise<number> {
    return await this.autoIncrement('index', 1)
  }

  async read(id: number): Promise<BPTreeNode<string, number>> {
    const raw = await readFile(this._filePath(id), 'utf8')
    return JSON.parse(raw)
  }

  async write(id: number, node: BPTreeNode<string, number>): Promise<void> {
    const stringify = JSON.stringify(node, null, 2)
    await writeFile(this._filePath(id), stringify, 'utf8')
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
        expect(r).toEqual([])
      }
    }
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
        expect(r).toEqual([])
      }
    }
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

    expect(tree.where({ like: { name: 'J%' } })).toEqual([
      { key: 6, value: { name: 'Japan', capital: 'Tokyo' } },
    ])
    expect(tree.where({ like: { name: 'C%' } })).toEqual([
      { key: 3, value: { name: 'China', capital: 'Beijing' } },
      { key: 4, value: { name: 'Colombia', capital: 'Bogota' } },
    ])
    expect(tree.where({ like: { name: '%or%' } })).toEqual([
      { key: 9, value: { name: 'Korea', capital: 'Seoul' } },
      { key: 10, value: { name: 'Portugal', capital: 'Lisbon' } },
    ])
    expect(tree.where({ like: { name: '_r%' } })).toEqual([
      { key: 1, value: { name: 'Argentina', capital: 'Buenos Aires' } },
      { key: 2, value: { name: 'Brazil', capital: 'Brasilia' } },
      { key: 5, value: { name: 'France', capital: 'Paris' } },
    ])
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

    expect(await tree.where({ like: { name: 'J%' } })).toEqual([
      { key: 6, value: { name: 'Japan', capital: 'Tokyo' } },
    ])
    expect(await tree.where({ like: { name: 'C%' } })).toEqual([
      { key: 3, value: { name: 'China', capital: 'Beijing' } },
      { key: 4, value: { name: 'Colombia', capital: 'Bogota' } },
    ])
    expect(await tree.where({ like: { name: '%or%' } })).toEqual([
      { key: 9, value: { name: 'Korea', capital: 'Seoul' } },
      { key: 10, value: { name: 'Portugal', capital: 'Lisbon' } },
    ])
    expect(await tree.where({ like: { name: '_r%' } })).toEqual([
      { key: 1, value: { name: 'Argentina', capital: 'Buenos Aires' } },
      { key: 2, value: { name: 'Brazil', capital: 'Brasilia' } },
      { key: 5, value: { name: 'France', capital: 'Paris' } },
    ])
  })
})