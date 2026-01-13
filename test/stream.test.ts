import {
  BPTreeSync,
  BPTreeAsync,
  SerializeStrategySync,
  SerializeStrategyAsync,
  ValueComparator,
  InMemoryStoreStrategySync,
  InMemoryStoreStrategyAsync,
  StringComparator
} from '../src'

describe('BPTree Stream Tests', () => {
  const order = 4

  describe('Sync Stream', () => {
    let tree: BPTreeSync<string, string>
    let strategy: SerializeStrategySync<string, string>
    let comparator: ValueComparator<string>

    beforeEach(() => {
      strategy = new InMemoryStoreStrategySync(order)
      comparator = new StringComparator()
      tree = new BPTreeSync(strategy, comparator)
      tree.init()
    })

    test('should stream keys and values correctly', () => {
      const inputs = [
        ['k1', 'v1'],
        ['k2', 'v2'],
        ['k3', 'v3'],
        ['k4', 'v4'],
        ['k5', 'v5'],
      ]
      inputs.forEach(([k, v]) => tree.insert(k, v))

      const stream = tree.whereStream({ gte: 'v1' })
      const result: [string, string][] = []
      for (const pair of stream) {
        result.push(pair)
      }

      expect(result.length).toBe(5)
      expect(result).toEqual(inputs)
    })

    test('should handle limit correctly in whereStream', () => {
      const inputs = [
        ['k1', 'a'],
        ['k2', 'a'],
        ['k3', 'b'],
        ['k4', 'b'],
        ['k5', 'c'],
      ]
      inputs.forEach(([k, v]) => tree.insert(k, v))

      const stream = tree.whereStream({ gte: 'a' }, 3)
      const result: [string, string][] = []
      for (const pair of stream) {
        result.push(pair)
      }

      expect(result.length).toBe(3)
      expect(result).toEqual([['k1', 'a'], ['k2', 'a'], ['k3', 'b']])
    })

    test('should handle limit with filtering in whereStream', () => {
      // Insert data such that we have matches and non-matches for secondary condition
      // We want to verify that limit applies to the *yielded* results, not just scanned ones.
      // Here "driver" is GTE 'a'. Secondary filter is NOT Equal 'b'.

      const inputs = [
        ['k1', 'a'], // match
        ['k2', 'b'], // filter out
        ['k3', 'c'], // match
        ['k4', 'b'], // filter out
        ['k5', 'd'], // match
        ['k6', 'e'], // match
      ]
      inputs.forEach(([k, v]) => tree.insert(k, v))

      // condition: val >= 'a' AND val != 'b'
      // limit: 3
      // Expected result: k1, k3, k5
      const stream = tree.whereStream({ gte: 'a', notEqual: 'b' }, 3)
      const result: [string, string][] = []
      for (const pair of stream) {
        result.push(pair)
      }

      expect(result.length).toBe(3)
      expect(result).toEqual([['k1', 'a'], ['k3', 'c'], ['k5', 'd']])
    })

    test('should allow external loop control (break)', () => {
      const inputs = [
        ['k1', 'v1'],
        ['k2', 'v2'],
        ['k3', 'v3'],
        ['k4', 'v4'],
      ]
      inputs.forEach(([k, v]) => tree.insert(k, v))

      const stream = tree.whereStream({ gte: 'v1' })
      const result: [string, string][] = []
      for (const pair of stream) {
        result.push(pair)
        if (result.length === 2) {
          break
        }
      }

      expect(result.length).toBe(2)
      expect(result).toEqual([['k1', 'v1'], ['k2', 'v2']])
    })

    test('keysStream should yield keys', () => {
      const inputs = [
        ['k1', 'v1'],
        ['k2', 'v2'],
      ]
      inputs.forEach(([k, v]) => tree.insert(k, v))

      const stream = tree.keysStream({ gte: 'v1' })
      const result: string[] = []
      for (const key of stream) {
        result.push(key)
      }
      expect(result).toEqual(['k1', 'k2'])
    })

    test('should stream with "like" condition', () => {
      const inputs = [
        ['k1', 'apple'],
        ['k2', 'banana'],
        ['k3', 'apricot'],
        ['k4', 'date'],
      ]
      inputs.forEach(([k, v]) => tree.insert(k, v))

      const stream = tree.whereStream({ like: 'ap%' })
      const result: [string, string][] = []
      for (const pair of stream) {
        result.push(pair)
      }

      expect(result.length).toBe(2)
      expect(result).toEqual([['k1', 'apple'], ['k3', 'apricot']])
    })

    test('should stream with "or" condition', () => {
      const inputs = [
        ['k1', 'apple'],
        ['k2', 'banana'],
        ['k3', 'cherry'],
        ['k4', 'date'],
      ]
      inputs.forEach(([k, v]) => tree.insert(k, v))

      const stream = tree.whereStream({ or: ['apple', 'cherry'] })
      const result: [string, string][] = []
      for (const pair of stream) {
        result.push(pair)
      }

      expect(result.length).toBe(2)
      expect(result).toEqual([['k1', 'apple'], ['k3', 'cherry']])
    })

    test('should stream with complex combined conditions', () => {
      const inputs = [
        ['k1', 'apple'],
        ['k2', 'banana'],
        ['k3', 'apricot'],
        ['k4', 'blueberry'],
        ['k5', 'cherry'],
      ]
      inputs.forEach(([k, v]) => tree.insert(k, v))
    })
  })

  describe('Async Stream', () => {
    let tree: BPTreeAsync<string, string>
    let strategy: SerializeStrategyAsync<string, string>
    let comparator: ValueComparator<string>

    beforeEach(async () => {
      strategy = new InMemoryStoreStrategyAsync(order)
      comparator = new StringComparator()
      tree = new BPTreeAsync(strategy, comparator)
      await tree.init()
    })

    test('should stream keys and values correctly', async () => {
      const inputs = [
        ['k1', 'v1'],
        ['k2', 'v2'],
        ['k3', 'v3'],
        ['k4', 'v4'],
        ['k5', 'v5'],
      ]
      for (const [k, v] of inputs) {
        await tree.insert(k, v)
      }

      const stream = tree.whereStream({ gte: 'v1' })
      const result: [string, string][] = []
      for await (const pair of stream) {
        result.push(pair)
      }

      expect(result.length).toBe(5)
      expect(result).toEqual(inputs)
    })

    test('should handle limit with filtering in whereStream', async () => {
      const inputs = [
        ['k1', 'a'],
        ['k2', 'b'],
        ['k3', 'c'],
        ['k4', 'b'],
        ['k5', 'd'],
        ['k6', 'e'],
      ]
      for (const [k, v] of inputs) {
        await tree.insert(k, v)
      }

      const stream = tree.whereStream({ gte: 'a', notEqual: 'b' }, 3)
      const result: [string, string][] = []
      for await (const pair of stream) {
        result.push(pair)
      }

      expect(result).toEqual([['k1', 'a'], ['k3', 'c'], ['k5', 'd']])
    })

    test('should stream with "like" condition', async () => {
      const inputs = [
        ['k1', 'apple'],
        ['k2', 'banana'],
        ['k3', 'apricot'],
        ['k4', 'date'],
      ]
      for (const [k, v] of inputs) {
        await tree.insert(k, v)
      }

      const stream = tree.whereStream({ like: 'ap%' })
      const result: [string, string][] = []
      for await (const pair of stream) {
        result.push(pair)
      }

      expect(result.length).toBe(2)
      expect(result).toEqual([['k1', 'apple'], ['k3', 'apricot']])
    })

    test('should stream with "or" condition', async () => {
      const inputs = [
        ['k1', 'apple'],
        ['k2', 'banana'],
        ['k3', 'cherry'],
        ['k4', 'date'],
      ]
      for (const [k, v] of inputs) {
        await tree.insert(k, v)
      }

      const stream = tree.whereStream({ or: ['apple', 'cherry'] })
      const result: [string, string][] = []
      for await (const pair of stream) {
        result.push(pair)
      }

      expect(result.length).toBe(2)
      expect(result).toEqual([['k1', 'apple'], ['k3', 'cherry']])
    })

    test('should stream with complex combined conditions', async () => {
      const inputs = [
        ['k1', 'apple'],
        ['k2', 'banana'],
        ['k3', 'apricot'],
        ['k4', 'blueberry'],
        ['k5', 'cherry'],
      ]
      for (const [k, v] of inputs) {
        await tree.insert(k, v)
      }
    })
    test('should stream with "like" condition leading wildcard', async () => {
      const inputs = [
        ['k1', 'John Doe'],
        ['k2', 'Jane Doe'],
        ['k3', 'Alice Smith'],
      ]
      for (const [k, v] of inputs) {
        await tree.insert(k, v)
      }

      const stream = tree.whereStream({ like: '% Doe' })
      const result: [string, string][] = []
      for await (const pair of stream) {
        result.push(pair)
      }

      expect(result.length).toBe(2)
      expect(result).toEqual([['k2', 'Jane Doe'], ['k1', 'John Doe']])
    })
  })
})
