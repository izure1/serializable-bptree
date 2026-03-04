import {
  BPTreeSync,
  SerializeStrategySync,
  ValueComparator,
  InMemoryStoreStrategySync
} from '../src'

interface Item {
  primary: number
  secondary: string
}

class ItemComparator extends ValueComparator<Item> {
  asc(a: Item, b: Item): number {
    const diff = a.primary - b.primary
    if (diff !== 0) return diff
    return a.secondary.localeCompare(b.secondary)
  }

  primaryAsc(a: Item, b: Item): number {
    return a.primary - b.primary
  }

  match(v: Item): string {
    return v.primary.toString() + v.secondary
  }
}

describe('Other primary conditions', () => {
  const order = 4
  let tree: BPTreeSync<string, Item>
  let strategy: SerializeStrategySync<string, Item>
  let comparator: ValueComparator<Item>

  beforeEach(() => {
    strategy = new InMemoryStoreStrategySync(order)
    comparator = new ItemComparator()
    tree = new BPTreeSync(strategy, comparator)
    tree.init()
  })

  const insertData = () => {
    const inputs: [string, Item][] = [
      ['k1', { primary: 1, secondary: 'a' }],
      ['k2', { primary: 2, secondary: 'b' }],
      ['k3', { primary: 2, secondary: 'c' }],
      ['k4', { primary: 3, secondary: 'd' }],
      ['k5', { primary: 4, secondary: 'e' }],
      ['k6', { primary: 4, secondary: 'f' }],
      ['k7', { primary: 5, secondary: 'g' }],
    ]
    inputs.forEach(([k, v]) => tree.insert(k, v))
  }

  const getKeys = (stream: Iterable<[string, Item]>) => {
    const res: string[] = []
    for (const [k] of stream) {
      res.push(k)
    }
    return res
  }

  describe('primaryGt', () => {
    test('asc', () => {
      insertData()
      // > 2 ==> 3 (d), 4 (e, f), 5 (g)
      const keys = getKeys(tree.whereStream({ primaryGt: { primary: 2 } as any }))
      expect(keys).toEqual(['k4', 'k5', 'k6', 'k7'])
    })

    test('desc', () => {
      insertData()
      // > 2 ==> 5 (g), 4 (f, e), 3 (d)
      const keys = getKeys(tree.whereStream({ primaryGt: { primary: 2 } as any }, { order: 'desc' }))
      expect(keys).toEqual(['k7', 'k6', 'k5', 'k4'])
    })

    test('boundary and duplicates', () => {
      insertData()
      // > 1 ==> 2 (b, c), 3 (d), 4 (e, f), 5 (g)
      const keys = getKeys(tree.whereStream({ primaryGt: { primary: 1 } as any }))
      expect(keys).toEqual(['k2', 'k3', 'k4', 'k5', 'k6', 'k7'])
    })

    test('value not in tree', () => {
      const inputs: [string, Item][] = [
        ['k1', { primary: 1, secondary: 'a' }],
        ['k2', { primary: 3, secondary: 'b' }],
        ['k3', { primary: 5, secondary: 'c' }],
      ]
      inputs.forEach(([k, v]) => tree.insert(k, v))
      // > 2 ==> 3 (b), 5 (c)
      const keys = getKeys(tree.whereStream({ primaryGt: { primary: 2 } as any }))
      expect(keys).toEqual(['k2', 'k3'])
    })

    test('where all values are greater', () => {
      insertData()
      // > 0 ==> all
      const keys = getKeys(tree.whereStream({ primaryGt: { primary: 0 } as any }))
      expect(keys).toEqual(['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7'])
    })

    test('where no values match', () => {
      insertData()
      const keys = getKeys(tree.whereStream({ primaryGt: { primary: 10 } as any }))
      expect(keys).toEqual([])
    })
  })

  describe('primaryGte', () => {
    test('asc', () => {
      insertData()
      // >= 2 ==> 2 (b, c), 3 (d), 4 (e, f), 5 (g)
      // Expect: k2, k3, k4, k5, k6, k7
      const keys = getKeys(tree.whereStream({ primaryGte: { primary: 2 } as any }))
      expect(keys).toEqual(['k2', 'k3', 'k4', 'k5', 'k6', 'k7'])
    })
    test('desc', () => {
      insertData()
      const keys = getKeys(tree.whereStream({ primaryGte: { primary: 3 } as any }, { order: 'desc' }))
      expect(keys).toEqual(['k7', 'k6', 'k5', 'k4'])
    })
  })

  describe('primaryLt', () => {
    test('asc', () => {
      insertData()
      // < 4 ==> 1 (a), 2 (b, c), 3 (d)
      const keys = getKeys(tree.whereStream({ primaryLt: { primary: 4 } as any }))
      expect(keys).toEqual(['k1', 'k2', 'k3', 'k4'])
    })
    test('desc', () => {
      insertData()
      const keys = getKeys(tree.whereStream({ primaryLt: { primary: 3 } as any }, { order: 'desc' }))
      expect(keys).toEqual(['k3', 'k2', 'k1'])
    })
  })

  describe('primaryLte', () => {
    test('asc', () => {
      insertData()
      // <= 3 ==> 1 (a), 2 (b, c), 3 (d)
      const keys = getKeys(tree.whereStream({ primaryLte: { primary: 3 } as any }))
      expect(keys).toEqual(['k1', 'k2', 'k3', 'k4'])
    })
    test('desc', () => {
      insertData()
      // <= 4 ==> 4 (f, e), 3 (d), 2 (c, b), 1 (a)
      const keys = getKeys(tree.whereStream({ primaryLte: { primary: 4 } as any }, { order: 'desc' }))
      expect(keys).toEqual(['k6', 'k5', 'k4', 'k3', 'k2', 'k1'])
    })
  })

  describe('primaryEqual', () => {
    test('asc', () => {
      insertData()
      // == 2 ==> 2 (b, c)
      const keys = getKeys(tree.whereStream({ primaryEqual: { primary: 2 } as any }))
      expect(keys).toEqual(['k2', 'k3'])
    })
    test('desc', () => {
      insertData()
      // == 4 ==> 4 (f, e)
      const keys = getKeys(tree.whereStream({ primaryEqual: { primary: 4 } as any }, { order: 'desc' }))
      expect(keys).toEqual(['k6', 'k5'])
    })
    test('not exists', () => {
      insertData()
      const keys = getKeys(tree.whereStream({ primaryEqual: { primary: 10 } as any }))
      expect(keys).toEqual([])
    })
  })

  describe('primaryNotEqual', () => {
    test('asc', () => {
      insertData()
      // != 2 ==> 1, 3, 4, 5
      const keys = getKeys(tree.whereStream({ primaryNotEqual: { primary: 2 } as any }))
      expect(keys).toEqual(['k1', 'k4', 'k5', 'k6', 'k7'])
    })
    test('desc', () => {
      insertData()
      // != 4 ==> 5, 3, 2, 1
      const keys = getKeys(tree.whereStream({ primaryNotEqual: { primary: 4 } as any }, { order: 'desc' }))
      expect(keys).toEqual(['k7', 'k4', 'k3', 'k2', 'k1'])
    })
  })

  describe('primaryOr', () => {
    test('asc', () => {
      insertData()
      const keys = getKeys(tree.whereStream({ primaryOr: [{ primary: 2 }, { primary: 5 }] as any }))
      expect(keys).toEqual(['k2', 'k3', 'k7'])
    })
    test('desc', () => {
      insertData()
      const keys = getKeys(tree.whereStream({ primaryOr: [{ primary: 4 }, { primary: 1 }] as any }, { order: 'desc' }))
      expect(keys).toEqual(['k6', 'k5', 'k1'])
    })
  })
})
