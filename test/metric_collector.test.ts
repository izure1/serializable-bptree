import { BPTreeSync } from '../src/BPTreeSync'
import { InMemoryStoreStrategySync } from '../src/SerializeStrategySync'
import { NumericComparator } from '../src/base/ValueComparator'
import { BPTreeSyncTransaction } from '../src/transaction/BPTreeSyncTransaction'
import * as fs from 'fs'
import * as path from 'path'

class MetricCollectorSync<K, V> extends BPTreeSync<K, V> {
  public cloneCount = 0
  public updateCount = 0

  constructor(strategy: any, comparator: any, option?: any) {
    super(strategy, comparator, option)
  }

  public override _cloneNode(node: any): any {
    this.cloneCount++
    return super._cloneNode(node)
  }

  public override _updateNode(node: any): void {
    this.updateCount++
    super._updateNode(node)
  }

  public createTransactionMetric(): MetricCollectorSyncTransaction<K, V> {
    const nestedTx = this.mvcc.createNested()
    const tx = new MetricCollectorSyncTransaction(
      this as any,
      this.mvcc,
      nestedTx,
      this.strategy,
      this.comparator,
      this.option,
      this
    );
    (tx as any)._initInternal()
    return tx
  }
}

class MetricCollectorSyncTransaction<K, V> extends BPTreeSyncTransaction<K, V> {
  constructor(
    rootTx: any,
    mvccRoot: any,
    mvcc: any,
    strategy: any,
    comparator: any,
    option: any,
    private collector: MetricCollectorSync<K, V>
  ) {
    super(rootTx, mvccRoot, mvcc, strategy, comparator, option)
  }

  public override _cloneNode(node: any): any {
    this.collector.cloneCount++
    return super._cloneNode(node)
  }

  public override _updateNode(node: any): void {
    this.collector.updateCount++
    super._updateNode(node)
  }
}

describe('Metric Collector (Optimization Verification)', () => {
  test('Detailed Metrics', () => {
    const strategy = new InMemoryStoreStrategySync<number, number>(3)
    const collector = new MetricCollectorSync<number, number>(strategy, new NumericComparator())
    collector.init()

    const results: any = {
      optimized: {}
    }

    // Scenario 1: Redundant Inserts (Key already exists)
    const tx1 = collector.createTransactionMetric()
    tx1.insert(1, 1) // First insert
    const midClone = collector.cloneCount
    tx1.insert(1, 1) // Redundant insert
    results.optimized.redundantInsert = { clone: collector.cloneCount - midClone }

    // Scenario 2: Delete non-existent key
    const midClone2 = collector.cloneCount
    tx1.delete(999, 999) // Non-existent
    results.optimized.nonExistentDelete = { clone: collector.cloneCount - midClone2 }

    // Scenario 3: Standard 10 inserts
    const startClone = collector.cloneCount
    for (let i = 2; i <= 10; i++) {
      tx1.insert(i, i)
    }
    tx1.commit()
    results.optimized.insert10 = { clone: collector.cloneCount - midClone } // Including the first two

    const resultPath = path.join(__dirname, 'metric_results.json')
    const existing = JSON.parse(fs.readFileSync(resultPath, 'utf8'))
    existing.optimized = results.optimized
    fs.writeFileSync(resultPath, JSON.stringify(existing, null, 2))
  })
})
