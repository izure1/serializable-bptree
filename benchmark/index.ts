import * as fs from 'fs'
import { BPTreeAsync, BPTreeSync, ValueComparator, NumericComparator, InMemoryStoreStrategyAsync, InMemoryStoreStrategySync } from '../src'

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
      console.log(`Processed ${count} items. Last 2000 took ${now - lastTime}ms.`)
      lastTime = now
    }
  }
  const totalTime = Date.now() - startTime
  console.log(`Total time: ${totalTime}ms`)
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
  console.log(`Executed ${numQueries} point queries in ${totalTime}ms`)
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
  console.log(`Sync where (10k items) took ${totalTime}ms. Result size: ${result.size}`)
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
  console.log(`Triggered ${conflictCount} conflicts in ${totalTime}ms.`)
  return [{ name: 'MVCC Conflict overhead', unit: 'ms', value: totalTime }]
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
  }

  if (!type) {
    for (const key in tasks) {
      allResults.push(...await tasks[key]())
    }
  } else if (tasks[type]) {
    allResults.push(...await tasks[type]())
  } else {
    console.error(`Unknown benchmark type: ${type}`)
    process.exit(1)
  }

  if (isJson) {
    fs.writeFileSync('./benchmark/benchmark-result.json', JSON.stringify(allResults, null, 2))
    console.log('\nResults saved to ./benchmark/benchmark-result.json')
  }
}

main().catch(console.error)
