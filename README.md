# serializable-bptree

[![](https://data.jsdelivr.com/v1/package/npm/serializable-bptree/badge)](https://www.jsdelivr.com/package/npm/serializable-bptree)
![Node.js workflow](https://github.com/izure1/serializable-bptree/actions/workflows/node.js.yml/badge.svg)

This is a B+tree that's totally okay with duplicate values. If you need to keep track of the B+ tree's state, don't just leave it in memory - make sure you write it down.

```typescript
import { readFileSync, writeFileSync, unlinkSync, existsSync } from 'fs'
import {
  BPTreeSync,
  SerializeStrategySync,
  NumericComparator
} from 'serializable-bptree'

class FileStoreStrategySync extends SerializeStrategySync<K, V> {
  id(): string {
    return this.autoIncrement('index', 1).toString()
  }

  read(id: string): BPTreeNode<K, V> {
    const raw = readFileSync(id, 'utf8')
    return JSON.parse(raw)
  }

  write(id: string, node: BPTreeNode<K, V>): void {
    const stringify = JSON.stringify(node)
    writeFileSync(id, stringify, 'utf8')
  }

  delete(id: string): void {
    unlinkSync(id)
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
const tree = new BPTreeSync(
  new FileStoreStrategySync(order),
  new NumericComparator()
)

tree.init()
tree.insert('a', 1)
tree.insert('b', 2)
tree.insert('c', 3)

tree.delete('b', 2)

tree.where({ equal: 1 }) // Map([{ key: 'a', value: 1 }])
tree.where({ gt: 1 }) // Map([{ key: 'c', value: 3 }])
tree.where({ lt: 2 }) // Map([{ key: 'a', value: 1 }])
tree.where({ gt: 0, lt: 4 }) // Map([{ key: 'a', value: 1 }, { key: 'c', value: 3 }])
tree.where({ or: [3, 1] }) // Map([{ key: 'a', value: 1 }, { key: 'c', value: 3 }])

tree.clear()
```

## Why use a `serializable-bptree`?

Firstly, in most cases, there is no need to use a B+tree in JavaScript. This is because there is a great alternative, the Map object. Nonetheless, if you need to retrieve values in a sorted order, a B+tree can be a good solution. These cases are often related to databases, and you may want to store this state not just in memory, but on a remote server or in a file. In this case, **serializable-bptree** can help you.

Additionally, this library supports asynchronous operations. Please refer to the section below for instructions on using it asynchronously.

## How to use

### Node.js (cjs)

```bash
npm i serializable-bptree
```

```typescript
import {
  BPTreeSync,
  BPTreeAsync,
  SerializeStrategySync,
  SerializeStrategyAsync,
  NumericComparator,
  StringComparator
} from 'serializable-bptree'
```

### Browser (esm)

```html
<script type="module">
  import {
    BPTreeSync,
    BPTreeAsync,
    InMemoryStoreStrategySync,
    InMemoryStoreStrategyAsync,
    ValueComparator,
    NumericComparator,
    StringComparator
  } from 'https://cdn.jsdelivr.net/npm/serializable-bptree@5/+esm'
</script>
```

## Conceptualization

### Value comparator

B+tree needs to keep values in sorted order. Therefore, a process to compare the sizes of values is needed, and that role is played by the **ValueComparator**.

Commonly used numerical and string comparisons are natively supported by the **serializable-bptree** library. Use it as follows:

```typescript
import { NumericComparator, StringComparator } from 'serializable-bptree'
```

However, you may want to sort complex objects other than numbers and strings. For example, if you want to sort by the **age** property order of an object, you need to create a new class that inherits from the **ValueComparator** class. Use it as follows:

```typescript
import { ValueComparator } from 'serializable-bptree'

interface MyObject {
  age: number
  name: string
}

class AgeComparator extends ValueComparator<MyObject> {
  asc(a: MyObject, b: MyObject): number {
    return a.age - b.age
  }

  match(value: MyObject): string {
    return value.age.toString()
  }
}
```

#### asc

The **asc** method should return values in ascending order. If the return value is negative, it means that the parameter **a** is smaller than **b**. If the return value is positive, it means that **a** is greater than **b**. If the return value is **0**, it indicates that **a** and **b** are of the same size.

#### match

The `match` method is used for the **LIKE** operator. This method specifies which value to test against a regular expression. For example, if you have a tree with values of the structure `{ country: string, capital: string }`, and you want to perform a **LIKE** operation based on the **capital** value, the method should return **value.capital**. In this case, you **CANNOT** perform a **LIKE** operation based on the **country** attribute. The returned value must be a string.

```typescript
interface MyObject {
  country: string
  capital: string
}

class CompositeComparator extends ValueComparator<MyObject> {
  ...
  match(value: MyObject): string {
    return value.capital
  }
}
```

For a tree with simple structure, without complex nesting, returning the value directly would be sufficient.

```typescript
class StringComparator extends ValueComparator<string> {
  match(value: string): string {
    return value
  }
}
```

### Serialize strategy

A B+tree instance is made up of numerous nodes. You would want to store this value when such nodes are created or updated. Let's assume you want to save it to a file.

You need to construct a logic for input/output from the file by inheriting the SerializeStrategy class. Look at the class structure below:

```typescript
import { SerializeStrategySync } from 'serializable-bptree'

class MyFileIOStrategySync extends SerializeStrategySync {
  id(): string
  read(id: string): BPTreeNode<K, V>
  write(id: string, node: BPTreeNode<K, V>): void
  delete(id: string): void
  readHead(): SerializeStrategyHead|null
  writeHead(head: SerializeStrategyHead): void
}
```

What does this method mean? And why do we need to construct such a method?

#### id(isLeaf: `boolean`): `string`

When a node is created in the B+tree, the node needs a unique value to represent itself. This is the **node.id** attribute, and you can specify this attribute yourself.

Typically, such an **id** value increases sequentially, and it would be beneficial to store such a value separately within the tree. For that purpose, the **setHeadData** and **getHeadData** methods are available. These methods are responsible for storing arbitrary data in the tree's header or retrieving stored data. Below is an example of usage:

```typescript
id(isLeaf: boolean): string {
  const current = this.getHeadData('index', 1) as number
  this.setHeadData('index', current+1)
  return current.toString()
}
```

Additionally, there is a more dev-friendly usage of this code.

```typescript
id(isLeaf: boolean): string {
  return this.autoIncrement('index', 1).toString()
}
```

The **id** method is called before a node is created in the tree. Therefore, it can also be used to allocate space for storing the node.

#### read(id: `string`): `BPTreeNode<K, V>`

This is a method to load the saved value as a tree instance. If you have previously saved the node as a file, you should use this method to convert it back to JavaScript JSON format and return it.

Please refer to the example below:

```typescript
read(id: string): BPTreeNode<K, V> {
  const filePath = `./my-store/${id}`
  const raw = fs.readFileSync(filePath, 'utf8')
  return JSON.parse(raw)
}
```

This method is called only once when loading a node from a tree instance. The loaded node is loaded into memory, and subsequently, when the tree references the node, it operates based on the values in memory **without** re-invoking this method.

#### write(id: `string`, node: `BPTreeNode<K, V>`): `void`

This method is called when there are changes in the internal nodes due to the insert or delete operations of the tree instance. In other words, it's a necessary method for synchronizing the in-memory nodes into a file.

Since this method is called frequently, be mindful of performance. There are ways to optimize it using a write-back caching technique.

Please refer to the example below:

```typescript
let queue = 0
function writeBack(id: string, node: BPTreeNode<K, V>, timer: number) {
  clearTimeout(queue)
  queue = setTimeout(() => {
    const filePath = `./my-store/${id}`
    const stringify = JSON.stringify(node)
    writeFileSync(filePath, stringify, 'utf8')
  }, timer)
}

...
write(id: string, node: BPTreeNode<K, V>): void {
  const writeBackInterval = 10
  writeBack(id, node, writeBackInterval)
}
```

This kind of delay writing should ideally occur within a few milliseconds. If this is not feasible, consider other approaches.

#### delete(id: `string`): `void`

This method is called when previously created nodes become no longer needed due to deletion or other processes. It can be used to free up space by deleting existing stored nodes.

```typescript
delete(id: string): void {
  const filePath = `./my-store/${id}`
  fs.unlinkSync(filePath)
}
```

#### readHead(): `SerializeStrategyHead`|`null`

This method is called only once when the tree is created. It's a method to restore the saved tree information. If it is the initial creation and there is no stored root node, it should return **null**.

This method should return the value stored in the **writeHead** method.

#### writeHead(head: `SerializeStrategyHead`): `void`

This method is called whenever the head information of the tree changes, typically when the root node changes. This method also works when the tree's **setHeadData** method is called. This is because the method attempts to store head data in the root node.

As a parameter, it receives the header information of the tree. This value should be serialized and stored. Later, the **readHead** method should convert this serialized value into a json format and return it.

### The Default `ValueComparator` and `SerializeStrategy`

To utilize **serializable-bptree**, you need to implement certain functions. However, a few basic helper classes are provided by default.

#### ValueComparator

* `NumericComparator`
* `StringComparator`

If the values being inserted into the tree are numeric, please use the **NumericComparator** class.

```typescript
import { NumericComparator } from 'serializable-bptree'
```

If the values being inserted into the tree can be strings, you can use the **StringComparator** class in this case.

```typescript
import { StringComparator } from 'serializable-bptree'
```

#### SerializeStrategy

* `InMemoryStoreStrategySync`
* `InMemoryStoreStrategyAsync`

As of now, the only class supported by default is the **InMemoryStoreStrategy**. This class is suitable for use when you prefer to operate the tree solely in-memory, similar to a typical B+ tree.

```typescript
import {
  InMemoryStoreStrategySync,
  InMemoryStoreStrategyAsync
} from 'serializable-bptree'
```

## Data Query Condition Clause

This library supports various conditional clauses. Currently, it supports **gte**, **gt**, **lte**, **lt**, **equal**, **notEqual**, **or**, and **like** conditions. Each condition is as follows:

### `gte`

Queries values that are greater than or equal to the given value.

```typescript
tree.where({ gte: 1 })
```

### `gt`

Queries values that are greater than the given value.

```typescript
tree.where({ gt: 1 })
```

### `lte`

Queries values that are less than or equal to the given value.

```typescript
tree.where({ lte: 5 })
```

### `lt`

Queries values that are less than the given value.

```typescript
tree.where({ lt: 5 })
```

### `equal`

Queries values that match the given value.

```typescript
tree.where({ equal: 3 })
```

### `notEqual`

Queries values that do not match the given value.

```typescript
tree.where({ notEqual: 3 })
```

### `or`

Queries values that satisfy at least one of the given conditions. It accepts an array of conditions, and if any of these conditions are met, the data is included in the result.

```typescript
tree.where({ or:  [1, 2, 3] })
```

### `like`

Queries values that contain the given value in a manner similar to regular expressions. Special characters such as % and _ can be used.

**%** matches zero or more characters. For example, **%ada%** means all strings that contain "ada" anywhere in the string. **%ada** means strings that end with "ada". **ada%** means strings that start with **"ada"**.

**_** matches exactly one character.
Using **p_t**, it can match any string where the underscore is replaced by any character, such as "pit", "put", etc.

You can obtain matching data by combining these condition clauses. If there are multiple conditions, an **AND** operation is used to retrieve only the data that satisfies all conditions.

```typescript
tree.where({ like: 'hello%' })
tree.where({ like: 'he__o%' })
tree.where({ like: '%world!' })
tree.where({ like: '%lo, wor%' })
```

## Using Asynchronously

Support for asynchronous trees has been available since version 3.0.0. Asynchronous is useful for operations with delays, such as file input/output and remote storage. Here is an example of how to use it:

```typescript
import { existsSync } from 'fs'
import { readFile, writeFile, unlink } from 'fs/promises'
import {
  BPTreeAsync,
  SerializeStrategyAsync,
  NumericComparator,
  StringComparator
} from 'serializable-bptree'

class FileStoreStrategyAsync extends SerializeStrategyAsync<K, V> {
  async id(isLeaf: boolean): Promise<string> {
    return await this.autoIncrement('index', 1).toString()
  }

  async read(id: string): Promise<BPTreeNode<K, V>> {
    const raw = await readFile(id, 'utf8')
    return JSON.parse(raw)
  }

  async write(id: string, node: BPTreeNode<K, V>): Promise<void> {
    const stringify = JSON.stringify(node)
    await writeFile(id, stringify, 'utf8')
  }

  async delete(id: string): Promise<void> {
    await unlink(id)
  }

  async readHead(): Promise<SerializeStrategyHead|null> {
    if (!existsSync('head')) {
      return null
    }
    const raw = await readFile('head', 'utf8')
    return JSON.parse(raw)
  }

  async writeHead(head: SerializeStrategyHead): Promise<void> {
    const stringify = JSON.stringify(head)
    await writeFile('head', stringify, 'utf8')
  }
}

const order = 5
const tree = new BPTreeAsync(
  new FileStoreStrategyAsync(order),
  new NumericComparator()
)

await tree.init()
await tree.insert('a', 1)
await tree.insert('b', 2)
await tree.insert('c', 3)

await tree.delete('b', 2)

await tree.where({ equal: 1 }) // Map([{ key: 'a', value: 1 }])
await tree.where({ gt: 1 }) // Map([{ key: 'c', value: 3 }])
await tree.where({ lt: 2 }) // Map([{ key: 'a', value: 1 }])
await tree.where({ gt: 0, lt: 4 }) // Map([{ key: 'a', value: 1 }, { key: 'c', value: 3 }])

tree.clear()
```

The implementation method for asynchronous operations is not significantly different. The **-Async** suffix is used instead of the **-Sync** suffix in the **BPTree** and **SerializeStrategy** classes. The only difference is that the methods become asynchronous. The **ValueComparator** class and similar value comparators do not use asynchronous operations.

## Precautions for Use

### Synchronization Issue

The serializable-bptree minimizes file I/O by storing loaded nodes in-memory (caching). This approach works perfectly when a single tree instance is used for a given storage.

However, if **multiple BPTree instances** (e.g., across different processes or servers) read from and write to a **single shared storage**, data inconsistency can occur. This is because each instance maintains its own independent in-memory cache, and changes made by one instance are not automatically reflected in the others.

To solve this problem, you must synchronize the cached nodes across all instances. The `forceUpdate` method can be used to refresh the nodes cached in a tree instance. When one instance saves data to the shared storage, you should implement a signaling mechanism (e.g., via Pub/Sub or WebSockets) to notify other instances that a node has been updated. Upon receiving this signal, the other instances should call the `forceUpdate` method to ensure they are working with the latest data.

### Concurrency Issue in Asynchronous Trees

This issue occurs only in asynchronous trees and can also occur in a 1:1 relationship between remote storage and client.

Since version 5.x.x, **serializable-bptree** provides a built-in read/write lock for the `BPTreeAsync` class to prevent data inconsistency during concurrent operations. Calling public methods like `insert`, `delete`, `where`, `exists`, `keys`, `setHeadData`, and `forceUpdate` will automatically acquire the appropriate lock.

However, please be aware of the following technical limitations:
- **Locking is only applied to public methods**: The internal `protected` methods (e.g., `_insertInParent`, `_deleteEntry`, etc.) do not automatically acquire locks.
- **Inheritance Caution**: If you extend the `BPTreeAsync` class and call `protected` methods directly, you must manually manage the locks using `readLock` or `writeLock` to ensure data integrity.

Despite these safeguards, it is still recommended to avoid unnecessary concurrent operations whenever possible to maintain optimal performance and predictability.

## LICENSE

MIT LICENSE
