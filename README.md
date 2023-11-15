# serializable-bptree

[![](https://data.jsdelivr.com/v1/package/npm/serializable-bptree/badge)](https://www.jsdelivr.com/package/npm/serializable-bptree)
![Node.js workflow](https://github.com/izure1/serializable-bptree/actions/workflows/node.js.yml/badge.svg)

This is a B+tree that's totally okay with duplicate values. If you need to keep track of the B+ tree's state, don't just leave it in memory - make sure you write it down.

```typescript
import { readFileSync, writeFileSync, existsSync } from 'fs'
import { BPTree, SerializeStrategy, NumericComparator } from 'serializable-bptree'

class FileStoreStrategy extends SerializeStrategy<K, V> {
  id(): number {
    const random = Math.ceil(Math.random()*1000000)
    return random
  }

  read(id: number): BPTreeNode<K, V> {
    const raw = readFileSync(id.toString(), 'utf8')
    return JSON.parse(raw)
  }

  write(id: number, node: BPTreeNode<K, V>): void {
    const stringify = JSON.stringify(node)
    writeFileSync(id.toString(), stringify, 'utf8')
  }

  readHead(): SerializeStrategyHead|null {
    if (!existsSync('head')) {
      return null
    }
    const raw = readFileSync('head', 'utf8')
    return JSON.parse(raw)
  }

  writeHead(head: SerializeStrategyHead): void {
    const stringify = JSON.stringify(head)
    writeFileSync('head', stringify, 'utf8')
  }
}

const order = 5
const tree = new BPTree(
  new SerializeStrategy(order),
  new NumericComparator()
)

tree.insert('a', 1)
tree.insert('b', 2)
tree.insert('c', 3)

tree.delete('b', 2)

tree.where({ equal: 1 }) // [{ key: 'a', value: 1 }]
tree.where({ gt: 1 }) // [{ key: 'c', value: 3 }]
tree.where({ lt: 2 }) // [{ key: 'a', value: 1 }]
tree.where({ gt: 0, lt: 4 }) // [{ key: 'a', value: 1 }, { key: 'c', value: 3 }]
```

## Why use a `serializable-bptree`?

Firstly, in most cases, there is no need to use a B+tree in JavaScript. This is because there is a great alternative, the Map object. Nonetheless, if you need to retrieve values in a sorted order, a B+tree can be a good solution. These cases are often related to databases, and you may want to store this state not just in memory, but on a remote server or in a file. In this case, `serializable-bptree` can help you.

## How to use

### Node.js (cjs)

```bash
npm i serializable-bptree
```

```typescript
import { BPTree } from 'serializable-bptree'
```

### Browser (esm)

```html
<script type="module">
  import { BPTree } from 'https://cdn.jsdelivr.net/npm/serializable-bptree@1.x.x/dist/esm/index.min.js'
</script>
```

## Conceptualization

### Value comparator

B+tree needs to keep values in sorted order. Therefore, a process to compare the sizes of values is needed, and that role is played by the `ValueComparator`.

Commonly used numerical and string comparisons are natively supported by the `serializable-bptree` library. Use it as follows:

```typescript
import { NumericComparator, StringComparator } from 'serializable-bptree'
```

However, you may want to sort complex objects other than numbers and strings. For example, if you want to sort by the `age` property order of an object, you need to create a new class that inherits from the `ValueComparator` class. Use it as follows:

```typescript
import { ValueComparator } from 'serializable-bptree'

interface MyObject {
  age: number
  name: string
}

class AgeComparator {
  asc(a: MyObject, b: MyObject): number {
    return a.age - b.age
  }
}
```

### Serialize strategy

A B+tree instance is made up of numerous nodes. You would want to store this value when such nodes are created or updated. Let's assume you want to save it to a file.

You need to construct a logic for input/output from the file by inheriting the SerializeStrategy class. Look at the class structure below:

```typescript
import { SerializeStrategy } from 'serializable-bptree'

class MyFileIOStrategy extends SerializeStrategy {
  id(): number
  read(id: number): BPTreeNode<K, V>
  write(id: number, node: BPTreeNode<K, V>): void
  readHead(): SerializeStrategyHead|null
  writeHead(head: SerializeStrategyHead): void
}
```

What does this method mean? And why do we need to construct such a method?

#### id(): `number`

When a node is created in the B+tree, the node needs a unique value to represent itself. This is the `node.id` attribute, and you can specify this attribute yourself. For example, it could be implemented like this.

```typescript
id(): number {
  const current = before + 1
  before = current
  return current
}
```

Or, you could use file input/output to save and load the value of the `before` variable.

#### read(id: `number`): `BPTreeNode<K, V>`

This is a method to load the saved value as a tree instance. If you have previously saved the node as a file, you should use this method to convert it back to JavaScript JSON format and return it.

Please refer to the example below:

```typescript
read(id: number): BPTreeNode<K, V> {
  const filePath = `./my-store/${id.toString()}`
  const raw = fs.readFileSync(filePath, 'utf8')
  return JSON.parse(raw)
}
```

This method is called only once when loading a node from a tree instance.

#### write(id: `number`, node: `BPTreeNode<K, V>`): `void`

This method is called when there are changes in the internal nodes due to the insert or delete operations of the tree instance. In other words, it's a necessary method for synchronizing the in-memory nodes into a file.

Since this method is called frequently, be mindful of performance. There are ways to optimize it using a write-back caching technique.

Please refer to the example below:

```typescript
let queue = 0
function writeBack(id: number, node: BPTreeNode<K, V>, timer: number) {
  clearTimeout(queue)
  queue = setTimeout(() => {
    const filePath = `./my-store/${id.toString()}`
    const stringify = JSON.stringify(node)
    writeFileSync(filePath, stringify, 'utf8')
  }, timer)
}

...
write(id: number, node: BPTreeNode<K, V>): void {
  const writeBackInterval = 100
  writeBack(id, node, writeBackInterval)
}
```

This kind of delay writing should ideally occur within a few milliseconds. If this is not feasible, consider other approaches.

#### readHead(): `SerializeStrategyHead`|`null`

This method is called only once when the tree is created. It's a method to restore the saved tree information. If it is the initial creation and there is no stored root node, it should return `null`.

This method should return the value stored in the `writeHead` method.

#### writeHead(head: `SerializeStrategyHead`): `void`

This method is called whenever the head information of the tree changes, typically when the root node changes.

As a parameter, it receives the header information of the tree. This value should be serialized and stored. Later, the `readHead` method should convert this serialized value into a json format and return it.

### The Default `ValueComparator` and `SerializeStrategy`

To utilize `serializable-bptree`, you need to implement certain functions. However, a few basic helper classes are provided by default.

#### ValueComparator

* `NumericComparator`
* `StringComparator`

If the values being inserted into the tree are numeric, please use the `NumericComparator` class.

```typescript
import { NumericComparator } from 'serializable-bptree'
```

If the values being inserted into the tree can be strings, you can use the `StringComparator` class in this case.

```typescript
import { StringComparator } from 'serializable-bptree'
```

#### SerializeStrategy

* `InMemoryStoreStrategy`

As of now, the only class supported by default is the `InMemoryStoreStrategy`. This class is suitable for use when you prefer to operate the tree solely in-memory, similar to a typical B+ tree.

```typescript
import { InMemoryStoreStrategy } from 'serializable-bptree'
```

## LICENSE

MIT LICENSE
