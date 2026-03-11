import { BPTreeAsync, StringComparator, InMemoryStoreStrategyAsync } from '../src'

function getRandomString(length: number): string {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
  let result = ''
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length))
  }
  return result
}

async function runInputTest() {
  console.log('\n--- 2.5 Million Items Insert Test ---')
  const order = 100 // Reasonable order size
  const tree = new BPTreeAsync(
    new InMemoryStoreStrategyAsync<string, string>(order),
    new StringComparator()
  )
  await tree.init()

  const totalItems = 2_500_000
  const batchSize = 100_000 
  console.log(`Inserting ${totalItems} items in batches of ${batchSize}...`)

  const startTime = Date.now()
  let lastTime = startTime

  for (let i = 0; i < totalItems; i += batchSize) {
    const entries: [string, string][] = []
    for (let j = 0; j < batchSize; j++) {
      const idx = i + j
      const val = getRandomString(2)
      const key = `${val}:${idx}`
      entries.push([key, val])
    }
    
    const batchStart = Date.now()
    await tree.batchInsert(entries)
    const batchTime = Date.now() - batchStart
    
    const now = Date.now()
    console.log(`[Batch ${i / batchSize + 1} / ${totalItems / batchSize}] Inserted ${batchSize} items. Batch took ${batchTime}ms.`)
    lastTime = now
  }

  const totalTime = Date.now() - startTime
  console.log(`\nTotal time for ${totalItems} insertions: ${totalTime}ms`)
  console.log(`Average time per item: ${(totalTime / totalItems).toFixed(4)}ms`)
  console.log(`Insertions per second: ${(totalItems / (totalTime / 1000)).toFixed(2)}`)
}

runInputTest().catch(console.error)
