import * as fs from 'fs'
import {
  SerializeStrategySync,
  SerializeStrategyAsync,
  BPTreeAsync,
  BPTreeSync,
  BPTreePureSync,
  BPTreePureAsync,
  ValueComparator,
  NumericComparator,
  StringComparator,
  InMemoryStoreStrategyAsync,
  InMemoryStoreStrategySync
} from '../src'
import { findLowerBoundLeaf } from '../src/base/BPTreeAlgorithmSync'

class ComplexComparator extends ValueComparator<{ id: number, data: string }> {
  asc(a: { id: number, data: string }, b: { id: number, data: string }): number {
    return a.id - b.id
  }
  match(value: { id: number, data: string }): string {
    return value.id.toString()
  }
}

interface BenchmarkResult {
  name: string
  unit: string
  value: number
}

async function runAsyncStreamBenchmark(): Promise<BenchmarkResult[]> {
  console.log('\n--- Async Stream Benchmark (Order 3, Large Items) ---')
  const order = 3
  const tree = new BPTreeAsync(
    new InMemoryStoreStrategyAsync(order),
    new ComplexComparator()
  )
  await tree.init()

  const totalItems = 10000
  const largeData = 'a'.repeat(2000)
  console.log(`Inserting ${totalItems} items...`)

  for (let i = 0; i < totalItems; i += 100) {
    const tx = await tree.createTransaction()
    for (let j = 0; j < 100; j++) {
      const val = i + j
      await tx.insert({ id: val, data: largeData }, { id: val, data: largeData })
    }
    await tx.commit()
  }

  console.log('Starting whereStream scan...')
  const readTx = await tree.createTransaction()
  let count = 0
  const startTime = Date.now()
  let lastTime = startTime

  for await (const pair of readTx.whereStream({ gte: { id: 0, data: '' } })) {
    count++
    if (count % 2000 === 0) {
      const now = Date.now()
      console.log(`Processed ${count} items.Last 2000 took ${now - lastTime} ms.`)
      lastTime = now
    }
  }
  const totalTime = Date.now() - startTime
  console.log(`Total time: ${totalTime} ms`)
  return [{ name: 'Async Stream Scan', unit: 'ms', value: totalTime }]
}

async function runPointQueryBenchmark(): Promise<BenchmarkResult[]> {
  console.log('\n--- Point Query Benchmark (No Cache) ---')
  const tree = new BPTreeAsync(
    new InMemoryStoreStrategyAsync(3),
    new NumericComparator()
  )
  await tree.init()

  const totalItems = 5000
  for (let i = 0; i < totalItems; i += 100) {
    const tx = await tree.createTransaction()
    for (let j = 0; j < 100; j++) {
      const val = i + j
      await tx.insert(val, val)
    }
    await tx.commit()
  }

  const readTx = await tree.createTransaction()
  const startTime = Date.now()
  const numQueries = 200
  for (let i = 0; i < numQueries; i++) {
    const target = (i * 17) % totalItems
    for await (const p of readTx.whereStream({ equal: target })) { }
  }
  const totalTime = Date.now() - startTime
  console.log(`Executed ${numQueries} point queries in ${totalTime} ms`)
  return [{ name: 'Point Query latency', unit: 'ms', value: totalTime }]
}

function runSyncBenchmark(): BenchmarkResult[] {
  console.log('\n--- Sync Where Benchmark ---')
  const tree = new BPTreeSync(
    new InMemoryStoreStrategySync(10),
    new NumericComparator()
  )
  tree.init()

  const totalItems = 10000
  const tx = tree.createTransaction()
  for (let i = 0; i < totalItems; i++) {
    tx.insert(i, i)
  }
  tx.commit()

  const readTx = tree.createTransaction()
  const startTime = Date.now()
  const result = readTx.where({ gte: 0 })
  const totalTime = Date.now() - startTime
  console.log(`Sync where(10k items) took ${totalTime} ms.Result size: ${result.size} `)
  return [{ name: 'Sync Where latency', unit: 'ms', value: totalTime }]
}

async function runLeakTest(): Promise<BenchmarkResult[]> {
  console.log('\n--- MVCC Transaction Leak Test ---')
  const tree = new BPTreeAsync(new InMemoryStoreStrategyAsync(100), new NumericComparator())
  await tree.init()

  const txWinner = await tree.createTransaction()
  await txWinner.insert(1, 100)
  await txWinner.commit()

  let conflictCount = 0
  const startTime = Date.now()
  for (let i = 0; i < 1000; i++) {
    const tx = await tree.createTransaction()
    await tx.insert(1, 200)
    const res = await tx.commit()
    if (!res.success) conflictCount++
  }
  const totalTime = Date.now() - startTime
  console.log(`Triggered ${conflictCount} conflicts in ${totalTime} ms.`)
  return [{ name: 'MVCC Conflict overhead', unit: 'ms', value: totalTime }]
}

async function runBatchBenchmark(): Promise<BenchmarkResult[]> {
  console.log('\n--- batchInsert Benchmark (5000 items) ---')
  const n = 5000
  const entries: [number, number][] = []
  for (let i = 0; i < n; i++) {
    entries.push([i, i])
  }

  // Individual Insert (Single Transaction)
  const treeIndiv = new BPTreeAsync(new InMemoryStoreStrategyAsync(50), new NumericComparator())
  await treeIndiv.init()
  const startIndiv = Date.now()
  const txIndiv = await treeIndiv.createTransaction()
  for (const [key, value] of entries) {
    await txIndiv.insert(key, value)
  }
  await txIndiv.commit()
  const timeIndiv = Date.now() - startIndiv
  console.log(`Individual Insert via one Transaction(${n} items): ${timeIndiv} ms`)

  // Batch Insert (Internal optimization)
  const treeBatch = new BPTreeAsync(new InMemoryStoreStrategyAsync(50), new NumericComparator())
  await treeBatch.init()
  const startBatch = Date.now()
  await treeBatch.batchInsert(entries)
  const timeBatch = Date.now() - startBatch
  console.log(`Batch Insert(${n} items): ${timeBatch} ms`)

  console.log(`Performance Improvement: ${((1 - timeBatch / timeIndiv) * 100).toFixed(1)}% `)

  return [
    { name: 'Individual Insert', unit: 'ms', value: timeIndiv },
    { name: 'Batch Insert', unit: 'ms', value: timeBatch }
  ]
}

function getRandomString(length: number): string {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
  let result = ''
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length))
  }
  return result
}

async function runBulkLoadBenchmark(): Promise<BenchmarkResult[]> {
  console.log('\n--- bulkLoad vs batchInsert Benchmark (2.5 Million Items) ---')
  const order = 100
  const totalItems = 2_500_000
  const batchSize = 100_000

  console.log(`Preparing data for ${totalItems} items...`)
  const allEntries: [string, string][][] = []
  for (let i = 0; i < totalItems; i += batchSize) {
    const entries: [string, string][] = []
    for (let j = 0; j < batchSize; j++) {
      const idx = i + j
      const val = getRandomString(2)
      const key = `${val}:${idx} `
      entries.push([key, val])
    }
    allEntries.push(entries)
  }

  const results: BenchmarkResult[] = []

  // bulkLoad
  {
    const tree = new BPTreeAsync(
      new InMemoryStoreStrategyAsync<string, string>(order),
      new StringComparator()
    )
    await tree.init()
    const flatEntries = allEntries.flat()

    console.log('Starting bulkLoad...')
    const startTime = Date.now()
    await tree.bulkLoad(flatEntries)
    const totalTime = Date.now() - startTime

    console.log(`BulkLoad completed in ${totalTime} ms.`)
    console.log(`Average time per item: ${(totalTime / totalItems).toFixed(4)} ms`)
    console.log(`Insertions per second: ${(totalItems / (totalTime / 1000)).toFixed(2)} `)

    results.push({ name: 'bulkLoad (2.5M)', unit: 'ms', value: totalTime })
  }

  await new Promise(resolve => setTimeout(resolve, 2000))

  // batchInsert
  {
    const tree = new BPTreeAsync(
      new InMemoryStoreStrategyAsync<string, string>(order),
      new StringComparator()
    )
    await tree.init()

    console.log('\nStarting batchInsert...')
    const startTime = Date.now()
    for (let i = 0; i < allEntries.length; i++) {
      const entries = allEntries[i]
      const batchStart = Date.now()
      await tree.batchInsert(entries)
      const batchTime = Date.now() - batchStart
      console.log(`[Batch ${i + 1} / ${allEntries.length}] Inserted ${entries.length} items.Batch took ${batchTime} ms.`)
    }
    const totalTime = Date.now() - startTime

    console.log(`\nbatchInsert completed in ${totalTime} ms.`)
    console.log(`Average time per item: ${(totalTime / totalItems).toFixed(4)} ms`)
    console.log(`Insertions per second: ${(totalItems / (totalTime / 1000)).toFixed(2)} `)

    results.push({ name: 'batchInsert (2.5M)', unit: 'ms', value: totalTime })
  }

  return results
}

import * as crypto from 'crypto'

class NoCopyStoreStrategySync<K, V> extends SerializeStrategySync<K, V> {
  private readonly store = new Map<string, any>()

  constructor(public order: number) {
    super(order)
  }

  id(): string {
    return crypto.randomUUID()
  }
  read(id: string): any {
    return this.store.get(id) // No deep copy
  }
  write(id: string, node: any): void {
    this.store.set(id, node)
  }
  delete(id: string): void {
    this.store.delete(id)
  }
  readHead(): any {
    if (this.head.root === null) {
      return null
    }
    return this.head
  }
  writeHead(head: any): void {
    this.head = head
  }
}

class NoCopyStoreStrategyAsync<K, V> extends SerializeStrategyAsync<K, V> {
  private readonly store = new Map<string, any>()

  constructor(public order: number) {
    super(order)
  }

  async id(): Promise<string> {
    return crypto.randomUUID()
  }
  async read(id: string): Promise<any> {
    return this.store.get(id) // No deep copy
  }
  async write(id: string, node: any): Promise<void> {
    this.store.set(id, node)
  }
  async delete(id: string): Promise<void> {
    this.store.delete(id)
  }
  async readHead(): Promise<any> {
    if (this.head.root === null) {
      return null
    }
    return this.head
  }
  async writeHead(head: any): Promise<void> {
    this.head = head
  }
}

async function runPureBenchmark(): Promise<BenchmarkResult[]> {
  console.log('\n--- MVCC vs Pure Tree Benchmark (100,000 items bulkLoad + point queries) ---')
  const order = 100
  const totalItems = 100_000
  const entries: [number, number][] = []

  for (let i = 0; i < totalItems; i++) {
    entries.push([i, i])
  }

  const results: BenchmarkResult[] = []

  // MVCC Sync Benchmark
  {
    const startMvccInsert = Date.now()
    const tree = new BPTreeSync(new InMemoryStoreStrategySync<number, number>(order), new NumericComparator())
    tree.init()
    tree.bulkLoad(entries)
    const timeMvccInsert = Date.now() - startMvccInsert

    const startMvccQuery = Date.now()
    const tx = tree.createTransaction()
    // random point queries
    for (let i = 0; i < 5000; i++) {
      const target = (i * 17) % totalItems
      tx.get(target)
    }
    const timeMvccQuery = Date.now() - startMvccQuery

    console.log(`MVCC Sync - bulkLoad(${totalItems}): ${timeMvccInsert} ms, 5K Point Queries: ${timeMvccQuery} ms`)
    results.push({ name: 'Sync MVCC bulkLoad', unit: 'ms', value: timeMvccInsert })
    results.push({ name: 'Sync MVCC queries', unit: 'ms', value: timeMvccQuery })
  }

  // Pure Sync Benchmark
  {
    const startPureInsert = Date.now()
    const strategy = new NoCopyStoreStrategySync<number, number>(order)
    const tree = new BPTreePureSync<number, number>(strategy, new NumericComparator())
    tree.init()
    tree.bulkLoad(entries)
    const timePureInsert = Date.now() - startPureInsert

    const startPureQuery = Date.now()
    for (let i = 0; i < 5000; i++) {
      const target = (i * 17) % totalItems
      tree.get(target)
    }
    const timePureQuery = Date.now() - startPureQuery

    console.log(`Pure Sync(No Copy) - bulkLoad(${totalItems}): ${timePureInsert} ms, 5K Point Queries: ${timePureQuery} ms`)
    results.push({ name: 'Sync Pure bulkLoad', unit: 'ms', value: timePureInsert })
    results.push({ name: 'Sync Pure queries', unit: 'ms', value: timePureQuery })
  }

  // Pure Async Benchmark
  {
    const startPureAsyncInsert = Date.now()
    const strategy = new NoCopyStoreStrategyAsync<number, number>(order)
    const tree = new BPTreePureAsync<number, number>(strategy, new NumericComparator())
    await tree.init()
    await tree.bulkLoad(entries)
    const timePureAsyncInsert = Date.now() - startPureAsyncInsert

    const startPureAsyncQuery = Date.now()
    for (let i = 0; i < 5000; i++) {
      const target = (i * 17) % totalItems
      await tree.get(target)
    }
    const timePureAsyncQuery = Date.now() - startPureAsyncQuery

    console.log(`Pure Async(No Copy) - bulkLoad(${totalItems}): ${timePureAsyncInsert} ms, 5K Point Queries: ${timePureAsyncQuery} ms`)
    results.push({ name: 'Async Pure bulkLoad', unit: 'ms', value: timePureAsyncInsert })
    results.push({ name: 'Async Pure queries', unit: 'ms', value: timePureAsyncQuery })
  }

  return results
}

async function runBatchDeletePureBenchmark(): Promise<BenchmarkResult[]> {
  console.log('\n--- BPTreePure batchDelete Benchmark (10,000 items) ---')
  const order = 100
  const totalItems = 10_000
  const entries: [number, number][] = []

  for (let i = 0; i < totalItems; i++) {
    entries.push([i, i])
  }

  const toDelete = entries.slice(0, 5000)
  const results: BenchmarkResult[] = []

  // Pure Sync Benchmark
  {
    const strategyIndiv = new NoCopyStoreStrategySync<number, number>(order)
    const treeIndiv = new BPTreePureSync<number, number>(strategyIndiv, new NumericComparator())
    treeIndiv.init()
    treeIndiv.bulkLoad(entries)

    const startIndiv = Date.now()
    for (const [key, value] of toDelete) {
      try {
        treeIndiv.delete(key, value)
      } catch (e: any) {
        console.log('\n--- ERROR DURING DELETE ---')
        console.log('Error at key:', key, 'value:', value)
        console.log('Exception:', e.message)

        // Let's locate the leaf node to see what's wrong with its keys
        const ctx = (treeIndiv as any)._createCtx()
        const node = findLowerBoundLeaf((treeIndiv as any)._createBufferedOps().ops, ctx.rootId, value, new NumericComparator())
        console.log('Leaf Node ID:', node.id)
        console.dir(node.keys, { depth: null })
        throw e
      }
    }
    const timeIndiv = Date.now() - startIndiv

    const strategyBatch = new NoCopyStoreStrategySync<number, number>(order)
    const treeBatch = new BPTreePureSync<number, number>(strategyBatch, new NumericComparator())
    treeBatch.init()
    treeBatch.bulkLoad(entries)

    const startBatch = Date.now()
    treeBatch.batchDelete(toDelete)
    const timeBatch = Date.now() - startBatch

    console.log(`Pure Sync - Individual Delete: ${timeIndiv} ms, batchDelete: ${timeBatch} ms`)
    console.log(`Performance Improvement: ${((1 - timeBatch / timeIndiv) * 100).toFixed(1)}% `)

    results.push({ name: 'Sync Pure Individual Delete', unit: 'ms', value: timeIndiv })
    results.push({ name: 'Sync Pure Batch Delete', unit: 'ms', value: timeBatch })
  }

  // Pure Async Benchmark
  {
    const strategyIndiv = new NoCopyStoreStrategyAsync<number, number>(order)
    const treeIndiv = new BPTreePureAsync<number, number>(strategyIndiv, new NumericComparator())
    await treeIndiv.init()
    await treeIndiv.bulkLoad(entries)

    const startIndiv = Date.now()
    for (const [key, value] of toDelete) {
      await treeIndiv.delete(key, value)
    }
    const timeIndiv = Date.now() - startIndiv

    const strategyBatch = new NoCopyStoreStrategyAsync<number, number>(order)
    const treeBatch = new BPTreePureAsync<number, number>(strategyBatch, new NumericComparator())
    await treeBatch.init()
    await treeBatch.bulkLoad(entries)

    const startBatch = Date.now()
    await treeBatch.batchDelete(toDelete)
    const timeBatch = Date.now() - startBatch

    console.log(`Pure Async - Individual Delete: ${timeIndiv} ms, batchDelete: ${timeBatch} ms`)
    console.log(`Performance Improvement: ${((1 - timeBatch / timeIndiv) * 100).toFixed(1)}% `)

    results.push({ name: 'Async Pure Individual Delete', unit: 'ms', value: timeIndiv })
    results.push({ name: 'Async Pure Batch Delete', unit: 'ms', value: timeBatch })
  }

  return results
}

async function main() {
  const args = process.argv.slice(2)
  const isJson = args.includes('json')
  const type = args.filter(a => a !== 'json')[0]

  const allResults: BenchmarkResult[] = []

  const tasks: Record<string, () => Promise<BenchmarkResult[]> | BenchmarkResult[]> = {
    stream: runAsyncStreamBenchmark,
    point: runPointQueryBenchmark,
    sync: runSyncBenchmark,
    leak: runLeakTest,
    batch: runBatchBenchmark,
    bulkload: runBulkLoadBenchmark,
    pure: runPureBenchmark,
    batchdelete: runBatchDeletePureBenchmark,
  }

  if (!type) {
    for (const key in tasks) {
      allResults.push(...await tasks[key]())
    }
  } else if (tasks[type]) {
    allResults.push(...await tasks[type]())
  } else {
    console.error(`Unknown benchmark type: ${type} `)
    process.exit(1)
  }

  if (isJson) {
    fs.writeFileSync('./benchmark/benchmark-result.json', JSON.stringify(allResults, null, 2))
    console.log('\nResults saved to ./benchmark/benchmark-result.json')
  }
}

main().catch(console.error)
