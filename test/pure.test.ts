import { BPTreePureSync, BPTreePureAsync, InMemoryStoreStrategySync, InMemoryStoreStrategyAsync, NumericComparator, StringComparator } from '../src'

describe('pure-test', () => {
  test('insert:number', () => {
    const strategy = new InMemoryStoreStrategySync<string, number>(5)
    const tree = new BPTreePureSync<string, number>(strategy, new NumericComparator())
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

    const gt5 = tree.where({ gt: 5 })
    expect(gt5.size).toBe(5)
    expect(gt5.has('f')).toBe(true)
    expect(gt5.has('j')).toBe(true)

    const lt5 = tree.where({ lt: 5 })
    expect(lt5.size).toBe(4)
    expect(lt5.has('a')).toBe(true)

    const eq5 = tree.where({ equal: 5 })
    expect(eq5.size).toBe(1)
    expect(eq5.has('e')).toBe(true)

    const gte5 = tree.where({ gte: 5 })
    expect(gte5.size).toBe(6)

    const lte5 = tree.where({ lte: 5 })
    expect(lte5.size).toBe(5)

    const neq5 = tree.where({ notEqual: 5 })
    expect(neq5.size).toBe(9)
  })

  test('insert:string', () => {
    const strategy = new InMemoryStoreStrategySync<string, string>(5)
    const tree = new BPTreePureSync<string, string>(strategy, new StringComparator())
    tree.init()

    tree.insert('a', 'apple')
    tree.insert('b', 'banana')
    tree.insert('c', 'cherry')
    tree.insert('d', 'date')
    tree.insert('e', 'elderberry')

    const result = tree.where({ equal: 'cherry' })
    expect(result.size).toBe(1)
    expect(result.has('c')).toBe(true)
  })

  test('or condition', () => {
    const strategy = new InMemoryStoreStrategySync<string, number>(5)
    const tree = new BPTreePureSync<string, number>(strategy, new NumericComparator())
    tree.init()

    for (let i = 1; i <= 10; i++) {
      tree.insert(`k${i}`, i)
    }

    const result = tree.where({ or: [3, 7] })
    expect(result.size).toBe(2)
    expect(result.has('k3')).toBe(true)
    expect(result.has('k7')).toBe(true)
  })

  test('delete', () => {
    const strategy = new InMemoryStoreStrategySync<string, number>(5)
    const tree = new BPTreePureSync<string, number>(strategy, new NumericComparator())
    tree.init()

    tree.insert('a', 1)
    tree.insert('b', 2)
    tree.insert('c', 3)
    tree.insert('d', 4)
    tree.insert('e', 5)

    tree.delete('c', 3)

    const all = tree.where({ gte: 1 })
    expect(all.size).toBe(4)
    expect(all.has('c')).toBe(false)
  })

  test('delete:notEqual', () => {
    const strategy = new InMemoryStoreStrategySync<string, number>(5)
    const tree = new BPTreePureSync<string, number>(strategy, new NumericComparator())
    tree.init()

    tree.insert('a', 1)
    tree.insert('b', 2)
    tree.insert('c', 3)
    tree.insert('d', 4)
    tree.insert('e', 5)

    tree.delete('c', 3)

    const neq = tree.where({ notEqual: 2 })
    expect(neq.size).toBe(3)
    expect(neq.has('b')).toBe(false)
  })

  test('batchInsert', () => {
    const strategy = new InMemoryStoreStrategySync<string, number>(5)
    const tree = new BPTreePureSync<string, number>(strategy, new NumericComparator())
    tree.init()

    const entries: [string, number][] = []
    for (let i = 1; i <= 100; i++) {
      entries.push([`k${i}`, i])
    }
    tree.batchInsert(entries)

    const gt50 = tree.where({ gt: 50 })
    expect(gt50.size).toBe(50)

    const all = tree.where({ gte: 1 })
    expect(all.size).toBe(100)
  })

  test('bulkLoad', () => {
    const strategy = new InMemoryStoreStrategySync<string, number>(5)
    const tree = new BPTreePureSync<string, number>(strategy, new NumericComparator())
    tree.init()

    const entries: [string, number][] = []
    for (let i = 1; i <= 100; i++) {
      entries.push([`k${i}`, i])
    }
    tree.bulkLoad(entries)

    const gt50 = tree.where({ gt: 50 })
    expect(gt50.size).toBe(50)

    const all = tree.where({ gte: 1 })
    expect(all.size).toBe(100)
  })

  test('exists and get', () => {
    const strategy = new InMemoryStoreStrategySync<string, number>(5)
    const tree = new BPTreePureSync<string, number>(strategy, new NumericComparator())
    tree.init()

    tree.insert('a', 1)
    tree.insert('b', 2)
    tree.insert('c', 3)

    expect(tree.exists('a', 1)).toBe(true)
    expect(tree.exists('a', 2)).toBe(false)
    expect(tree.exists('z', 99)).toBe(false)

    expect(tree.get('a')).toBe(1)
    expect(tree.get('b')).toBe(2)
    expect(tree.get('z')).toBe(undefined)
  })

  test('keys', () => {
    const strategy = new InMemoryStoreStrategySync<string, number>(5)
    const tree = new BPTreePureSync<string, number>(strategy, new NumericComparator())
    tree.init()

    tree.insert('a', 1)
    tree.insert('b', 2)
    tree.insert('c', 3)
    tree.insert('d', 4)
    tree.insert('e', 5)

    const keys = tree.keys({ gt: 3 })
    expect(keys.size).toBe(2)
    expect(keys.has('d')).toBe(true)
    expect(keys.has('e')).toBe(true)
  })

  test('whereStream and keysStream', () => {
    const strategy = new InMemoryStoreStrategySync<string, number>(5)
    const tree = new BPTreePureSync<string, number>(strategy, new NumericComparator())
    tree.init()

    for (let i = 1; i <= 10; i++) {
      tree.insert(`k${i}`, i)
    }

    const pairs: [string, number][] = []
    for (const pair of tree.whereStream({ lte: 3 })) {
      pairs.push(pair)
    }
    expect(pairs.length).toBe(3)

    const keys: string[] = []
    for (const key of tree.keysStream({ gte: 8 })) {
      keys.push(key)
    }
    expect(keys.length).toBe(3)
  })

  test('headData', () => {
    const strategy = new InMemoryStoreStrategySync<string, number>(5)
    const tree = new BPTreePureSync<string, number>(strategy, new NumericComparator())
    tree.init()

    tree.setHeadData({ myKey: 'myValue' })
    const data = tree.getHeadData()
    expect(data).toEqual({ myKey: 'myValue' })
  })

  test('getRootId and getOrder', () => {
    const strategy = new InMemoryStoreStrategySync<string, number>(5)
    const tree = new BPTreePureSync<string, number>(strategy, new NumericComparator())
    tree.init()

    expect(tree.getOrder()).toBe(5)
    expect(typeof tree.getRootId()).toBe('string')
  })

  test('verify', () => {
    const tree = new BPTreePureSync<string, number>(
      new InMemoryStoreStrategySync<string, number>(5),
      new NumericComparator(),
    )
    tree.init()

    expect(tree.verify(5, { gt: 3 })).toBe(true)
    expect(tree.verify(5, { gt: 7 })).toBe(false)
    expect(tree.verify(5, { equal: 5 })).toBe(true)
    expect(tree.verify(5, { notEqual: 5 })).toBe(false)
  })

  test('strategy-direct: no internal node caching', () => {
    const strategy = new InMemoryStoreStrategySync<string, number>(5)
    const tree = new BPTreePureSync<string, number>(strategy, new NumericComparator())
    tree.init()

    tree.insert('a', 1)
    tree.insert('b', 2)
    tree.insert('c', 3)

    const rootId = tree.getRootId()
    const rootNode = strategy.read(rootId) as any
    expect(rootNode).toBeDefined()
    expect(rootNode.leaf).toBe(true)
    expect(rootNode.values).toEqual([1, 2, 3])
  })

  test('like', () => {
    const strategy = new InMemoryStoreStrategySync<string, string>(5)
    const tree = new BPTreePureSync<string, string>(strategy, new StringComparator())
    tree.init()

    tree.insert('a', 'apple')
    tree.insert('b', 'banana')
    tree.insert('c', 'avocado')
    tree.insert('d', 'apricot')
    tree.insert('e', 'blueberry')

    const result = tree.where({ like: 'a%' })
    expect(result.size).toBe(3)
    expect(result.has('a')).toBe(true)
    expect(result.has('c')).toBe(true)
    expect(result.has('d')).toBe(true)
  })
})

describe('pure-async-test', () => {
  test('insert:number:async', async () => {
    const strategy = new InMemoryStoreStrategyAsync<string, number>(5)
    const tree = new BPTreePureAsync<string, number>(strategy, new NumericComparator())
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

    const gt5 = await tree.where({ gt: 5 })
    expect(gt5.size).toBe(5)

    const lt5 = await tree.where({ lt: 5 })
    expect(lt5.size).toBe(4)

    const eq5 = await tree.where({ equal: 5 })
    expect(eq5.size).toBe(1)
    expect(eq5.has('e')).toBe(true)
  })

  test('delete:async', async () => {
    const strategy = new InMemoryStoreStrategyAsync<string, number>(5)
    const tree = new BPTreePureAsync<string, number>(strategy, new NumericComparator())
    await tree.init()

    await tree.insert('a', 1)
    await tree.insert('b', 2)
    await tree.insert('c', 3)
    await tree.insert('d', 4)
    await tree.insert('e', 5)

    await tree.delete('c', 3)

    const all = await tree.where({ gte: 1 })
    expect(all.size).toBe(4)
    expect(all.has('c')).toBe(false)
  })

  test('batchInsert:async', async () => {
    const strategy = new InMemoryStoreStrategyAsync<string, number>(5)
    const tree = new BPTreePureAsync<string, number>(strategy, new NumericComparator())
    await tree.init()

    const entries: [string, number][] = []
    for (let i = 1; i <= 100; i++) entries.push([`k${i}`, i])
    await tree.batchInsert(entries)

    const all = await tree.where({ gte: 1 })
    expect(all.size).toBe(100)
  })

  test('bulkLoad:async', async () => {
    const strategy = new InMemoryStoreStrategyAsync<string, number>(5)
    const tree = new BPTreePureAsync<string, number>(strategy, new NumericComparator())
    await tree.init()

    const entries: [string, number][] = []
    for (let i = 1; i <= 100; i++) entries.push([`k${i}`, i])
    await tree.bulkLoad(entries)

    const all = await tree.where({ gte: 1 })
    expect(all.size).toBe(100)
  })

  test('exists and get:async', async () => {
    const strategy = new InMemoryStoreStrategyAsync<string, number>(5)
    const tree = new BPTreePureAsync<string, number>(strategy, new NumericComparator())
    await tree.init()

    await tree.insert('a', 1)
    await tree.insert('b', 2)

    expect(await tree.exists('a', 1)).toBe(true)
    expect(await tree.exists('a', 2)).toBe(false)
    expect(await tree.get('a')).toBe(1)
    expect(await tree.get('z')).toBe(undefined)
  })

  test('like:async', async () => {
    const strategy = new InMemoryStoreStrategyAsync<string, string>(5)
    const tree = new BPTreePureAsync<string, string>(strategy, new StringComparator())
    await tree.init()

    await tree.insert('a', 'apple')
    await tree.insert('b', 'banana')
    await tree.insert('c', 'avocado')

    const result = await tree.where({ like: 'a%' })
    expect(result.size).toBe(2)
    expect(result.has('a')).toBe(true)
    expect(result.has('c')).toBe(true)
  })
})
