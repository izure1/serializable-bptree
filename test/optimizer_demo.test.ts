import { BPTreeSync } from '../src/index'
import { InMemoryStoreStrategySync } from '../src/index'
import { StringComparator, NumericComparator } from '../src/index'
import { BPTreeCondition } from '../src/index'

/**
 * Simulator for a simple Database Engine with Rule-based Optimizer
 */
describe('Rule-based Optimizer Demo', () => {
  // We simulate a table "Users" with two columns: 'id' (Unique) and 'age' (Non-unique)
  // Indexes:
  let idxId: BPTreeSync<number, number> // Key: ID (number), Value: RowPointer (number)
  let idxAge: BPTreeSync<number, number> // Key: Age (number), Value: RowPointer (number)

  // Actual Row Data (Heap)
  const rows: Record<number, { id: number, age: number, name: string }> = {}

  beforeEach(() => {
    idxId = new BPTreeSync<number, number>(new InMemoryStoreStrategySync<number, number>(4), new NumericComparator())
    idxAge = new BPTreeSync<number, number>(new InMemoryStoreStrategySync<number, number>(4), new NumericComparator())
    idxId.init()
    idxAge.init()

    // Insert dummy data
    const data = [
      { pk: 1, id: 100, age: 20, name: 'Alice' },
      { pk: 2, id: 200, age: 20, name: 'Bob' },
      { pk: 3, id: 300, age: 30, name: 'Charlie' },
      { pk: 4, id: 400, age: 25, name: 'David' },
      { pk: 5, id: 500, age: 20, name: 'Eve' }, // user_e is age 20
    ]

    data.forEach(row => {
      rows[row.pk] = { id: row.id, age: row.age, name: row.name }
      idxId.insert(row.pk, row.id)   // Key=PK, Value=ID (search by ID)
      idxAge.insert(row.pk, row.age) // Key=PK, Value=Age (search by Age)
    })
  })

  // 1. Define Query Interface
  interface Query {
    id?: BPTreeCondition<number>
    age?: BPTreeCondition<number>
  }

  // 2. Rule-based Optimizer
  // Decides which index to use as the "Driver"
  function chooseDriverIndex(query: Query): 'idxId' | 'idxAge' | 'none' {
    // Rule 1: 'Equal' on Unique Index is the best (Highest Selectivity)
    if (query.id && query.id.equal !== undefined) {
      return 'idxId'
    }

    // Rule 2: 'Equal' on Non-Unique Index is better than Range
    if (query.age && query.age.equal !== undefined) {
      return 'idxAge'
    }

    // Rule 3: Range Query on Unique Index (ID)
    if (query.id && (query.id.gt || query.id.lt || query.id.gte || query.id.lte)) {
      return 'idxId'
    }

    // Rule 4: Range Query on Non-Unique Index (Age)
    if (query.age && (query.age.gt || query.age.lt || query.age.gte || query.age.lte)) {
      return 'idxAge'
    }

    return 'none'
  }

  // 3. Execution Engine
  function executeQuery(query: Query): any[] {
    const driverName = chooseDriverIndex(query)
    const results: any[] = []
    let pointerStream: Generator<[number, number]> | null = null

    console.log(`[Optimizer] Selected Driver: ${driverName}`)

    if (driverName === 'idxId') {
      // Driver: idxId
      pointerStream = idxId.whereStream(query.id! as any)
    } else if (driverName === 'idxAge') {
      // Driver: idxAge
      // Note: Cast to any for demo simplicity as types differ
      pointerStream = idxAge.whereStream(query.age! as any)
    } else {
      // Full Scan (Not implemented for this demo)
      return []
    }

    // Iterate Stream (Driver)
    for (const [pk, val] of pointerStream!) {
      console.log(`[Stream Yield] PK: ${pk}, Value: ${val}`)
      const row = rows[pk]

      // Filter: Check other conditions against the Row
      // (This simulates checking other indexes or the generic filter step)
      let match = true

      // Check ID condition if it wasn't the driver
      if (driverName !== 'idxId' && query.id) {
        // Simple check for 'equal' for demo. Real engine would use verifierMap.
        if (query.id.equal && row.id !== query.id.equal) match = false
        // ... add range checks ...
      }

      // Check Age condition if it wasn't the driver
      if (driverName !== 'idxAge' && query.age) {
        if (query.age.equal && row.age !== query.age.equal) match = false
        if (query.age.gt && !(row.age > query.age.gt)) match = false
        // ... add other range checks ...
      }

      if (match) {
        console.log(`[Match] Row: ${JSON.stringify(row)}`)
        results.push(row)
      } else {
        console.log(`[No Match] Row: ${JSON.stringify(row)}`)
      }
    }

    return results
  }

  test('Optimization: Should select ID Index for ID Equal query', () => {
    // Query: ID = 100 AND Age > 10
    // Expected: Driver = idxId (Rule 1)
    const query: Query = {
      id: { equal: 100 },
      age: { gt: 10 }
    }

    // Mock console.log to verify selection
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => { })

    const res = executeQuery(query)

    expect(logSpy).toHaveBeenCalledWith('[Optimizer] Selected Driver: idxId')
    expect(res.length).toBe(1)
    expect(res[0].name).toBe('Alice')

    logSpy.mockRestore()
  })

  test('Optimization: Should select Age Index when ID query is absent', () => {
    // Query: Age = 20
    // Expected: Driver = idxAge (Rule 2)
    const query: Query = {
      age: { equal: 20 }
    }

    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => { })

    const res = executeQuery(query)

    expect(logSpy).toHaveBeenCalledWith('[Optimizer] Selected Driver: idxAge')
    // Alice(20), Bob(20), Eve(20)
    expect(res.length).toBe(3)

    logSpy.mockRestore()
  })

  test('ChooseDriver: Should return the candidate with highest priority condition', () => {
    // BPTreeSync.chooseDriver uses conditionPriority to select the best driver
    // Priority: equal(100) > or(80) > gt/lt(50) > like(30) > notEqual(10)

    const driver = BPTreeSync.ChooseDriver([
      { tree: idxAge, condition: { gt: 10 } },      // priority 50
      { tree: idxId, condition: { equal: 100 } },   // priority 100
    ])

    expect(driver).not.toBeNull()
    expect(driver!.tree).toBe(idxId)
    expect(driver!.condition).toEqual({ equal: 100 })
  })
})
