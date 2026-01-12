import { BPTreeSync, InMemoryStoreStrategySync, ValueComparator } from '../src'

interface CompositeValue {
  group: number
  id: number
}

class CompositeComparator extends ValueComparator<CompositeValue> {
  // Strict sorting for uniqueness
  asc(a: CompositeValue, b: CompositeValue): number {
    const diff = a.group - b.group
    return diff === 0 ? (a.id - b.id) : diff
  }

  // Grouping by primary key only
  primaryAsc(a: CompositeValue, b: CompositeValue): number {
    return a.group - b.group
  }

  match(v: CompositeValue): string {
    return v.group.toString()
  }
}

describe('Primary Operators Strict Test', () => {
  let tree: BPTreeSync<string, CompositeValue>
  let data: CompositeValue[] = []
  const order = 4 // Small order to trigger frequent splits

  beforeAll(async () => {
    tree = new BPTreeSync(
      new InMemoryStoreStrategySync(order),
      new CompositeComparator()
    )
    tree.init()

    // 1. Prepare Data
    // 10 groups, 20 items per group => 200 items in total
    for (let g = 1; g <= 10; g++) {
      for (let i = 1; i <= 20; i++) {
        const value = { group: g, id: i }
        const key = `k-${g}-${i}`
        data.push(value)
        tree.insert(key, value)
      }
    }
  })

  // Helper to extract values from BPTree result map
  const getValues = (map: Map<string, CompositeValue>) => Array.from(map.values())

  // Helper to verify results
  const verify = (
    opName: string,
    actualMap: Map<string, CompositeValue>,
    expectedFilter: (v: CompositeValue) => boolean
  ) => {
    const actual = getValues(actualMap)
    const expected = data.filter(expectedFilter)

    // Sort both for comparison
    const sorter = (a: CompositeValue, b: CompositeValue) =>
      (a.group - b.group) || (a.id - b.id)

    actual.sort(sorter)
    expected.sort(sorter)

    expect(actual.length).toBe(expected.length)
    expect(actual).toEqual(expected)
    if (actual.length !== expected.length) {
      console.log(`[${opName}] Failed: Expected ${expected.length}, got ${actual.length}`)
    }
  }

  test('primaryGt: groups > 5', async () => {
    const result = tree.where({ primaryGt: { group: 5 } as any })
    verify('primaryGt', result, v => v.group > 5)
  })

  test('primaryGte: groups >= 5', async () => {
    const result = tree.where({ primaryGte: { group: 5 } as any })
    verify('primaryGte', result, v => v.group >= 5)
  })

  test('primaryLt: groups < 5', async () => {
    const result = tree.where({ primaryLt: { group: 5 } as any })
    verify('primaryLt', result, v => v.group < 5)
  })

  test('primaryLte: groups <= 5', async () => {
    const result = tree.where({ primaryLte: { group: 5 } as any })
    verify('primaryLte', result, v => v.group <= 5)
  })

  test('primaryEqual: groups == 5', async () => {
    const result = tree.where({ primaryEqual: { group: 5 } as any })
    verify('primaryEqual', result, v => v.group === 5)
  })

  test('primaryNotEqual: groups != 5', async () => {
    const result = tree.where({ primaryNotEqual: { group: 5 } as any })
    verify('primaryNotEqual', result, v => v.group !== 5)
  })

  test('primaryOr: groups 2, 5, 8', async () => {
    const targets = [2, 5, 8]
    const result = tree.where({
      primaryOr: targets.map(t => ({ group: t })) as any
    })
    verify('primaryOr', result, v => targets.includes(v.group))
  })
})
