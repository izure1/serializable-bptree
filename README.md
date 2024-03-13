# serializable-bptree

[![](https://data.jsdelivr.com/v1/package/npm/serializable-bptree/badge)](https://www.jsdelivr.com/package/npm/serializable-bptree)
![Node.js workflow](https://github.com/izure1/serializable-bptree/actions/workflows/node.js.yml/badge.svg)

This is a B+tree that's totally okay with duplicate values. If you need to keep track of the B+ tree's state, don't just leave it in memory - make sure you write it down.

```typescript
import { readFileSync, writeFileSync, existsSync } from 'fs'
import {
  BPTreeSync,
  SerializeStrategySync,
  NumericComparator
} from 'serializable-bptree'

class FileStoreStrategySync extends SerializeStrategySync<K, V> {
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
const tree = new BPTreeSync(
  new FileStoreStrategySync(order),
  new NumericComparator()
)

tree.init()
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
  } from 'https://cdn.jsdelivr.net/npm/serializable-bptree@3.x.x/dist/esm/index.min.js'
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
    return value.age
  }
}
```

#### asc

The **asc** method should return values in ascending order. If the return value is negative, it means that the parameter **a** is smaller than **b**. If the return value is positive, it means that **a** is greater than **b**. If the return value is **0**, it indicates that **a** and **b** are of the same size.

#### match

The `match` method is used for the **LIKE** operator. This method specifies which value to test against a regular expression. For example, if you have a tree with values of the structure `{ country: string, capital: number }`, and you want to perform a **LIKE** operation based on the **capital** value, the method should return **value.capital**. In this case, you **CANNOT** perform a **LIKE** operation based on the **country** attribute. The returned value must be a string.

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
  id(): number
  read(id: number): BPTreeNode<K, V>
  write(id: number, node: BPTreeNode<K, V>): void
  readHead(): SerializeStrategyHead|null
  writeHead(head: SerializeStrategyHead): void
}
```

What does this method mean? And why do we need to construct such a method?

#### id(): `number`

When a node is created in the B+tree, the node needs a unique value to represent itself. This is the **node.id** attribute, and you can specify this attribute yourself. For example, it could be implemented like this.

```typescript
id(): number {
  const current = before + 1
  before = current
  return current
}
```

Or, you could use file input/output to save and load the value of the **before** variable.

This method is called before a node is created in the tree. Therefore, it can also be used to allocate space for storing the node.

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

This method is called only once when loading a node from a tree instance. The loaded node is loaded into memory, and subsequently, when the tree references the node, it operates based on the values in memory **without** re-invoking this method.

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
  const writeBackInterval = 10
  writeBack(id, node, writeBackInterval)
}
```

This kind of delay writing should ideally occur within a few milliseconds. If this is not feasible, consider other approaches.

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

This library supports various conditional clauses. Currently, it supports **gte**, **gt**, **lte**, **lt**, **equal**, **notEqual**, and **like** conditions. Each condition is as follows:

### `gte`

Queries values that are greater than or equal to the given value.

### `gt`

Queries values that are greater than the given value.

### `lte`

Queries values that are less than or equal to the given value.

### `lt`

Queries values that are less than the given value.

### `equal`

Queries values that match the given value.

### `notEqual`

Queries values that do not match the given value.

### `like`

Queries values that contain the given value in a manner similar to regular expressions. Special characters such as % and _ can be used.

**%** matches zero or more characters. For example, **%ada%** means all strings that contain "ada" anywhere in the string. **%ada** means strings that end with "ada". **ada%** means strings that start with **"ada"**.

**_** matches exactly one character.
Using **p_t**, it can match any string where the underscore is replaced by any character, such as "pit", "put", etc.

You can obtain matching data by combining these condition clauses. If there are multiple conditions, an **AND** operation is used to retrieve only the data that satisfies all conditions.

## Using Asynchronously

Support for asynchronous trees has been available since version 3.0.0. Asynchronous is useful for operations with delays, such as file input/output and remote storage. Here is an example of how to use it:

```typescript
import { existsSync } from 'fs'
import { readFile, writeFile } from 'fs/promises'
import {
  BPTreeAsync,
  SerializeStrategyAsync,
  NumericComparator,
  StringComparator
} from 'serializable-bptree'

class FileStoreStrategyAsync extends SerializeStrategyAsync<K, V> {
  async id(): Promise<number> {
    const random = Math.ceil(Math.random()*1000000)
    return random
  }

  async read(id: number): Promise<BPTreeNode<K, V>> {
    const raw = await readFile(id.toString(), 'utf8')
    return JSON.parse(raw)
  }

  async write(id: number, node: BPTreeNode<K, V>): Promise<void> {
    const stringify = JSON.stringify(node)
    await writeFile(id.toString(), stringify, 'utf8')
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

await tree.where({ equal: 1 }) // [{ key: 'a', value: 1 }]
await tree.where({ gt: 1 }) // [{ key: 'c', value: 3 }]
await tree.where({ lt: 2 }) // [{ key: 'a', value: 1 }]
await tree.where({ gt: 0, lt: 4 }) // [{ key: 'a', value: 1 }, { key: 'c', value: 3 }]
```

The implementation method for asynchronous operations is not significantly different. The **-Async** suffix is used instead of the **-Sync** suffix in the **BPTree** and **SerializeStrategy** classes. The only difference is that the methods become asynchronous. The **ValueComparator** class and similar value comparators do not use asynchronous operations.

## Precautions for Use

### Synchronization Issue

The serializable-bptree minimizes file I/O by storing loaded nodes in-memory. This approach works well in situations where there is a 1:1 relationship between the remote storage and the client. However, in a 1:n scenario, where multiple clients read from and write to a single remote storage, data inconsistency between the remote storage and the clients can occur.

To solve this problem, it's necessary to update the cached nodes. The forceUpdate method was created for this purpose. It fetches the node data cached in the tree instance again. To use this feature, when you save data to the remote storage, you must send a signal to all clients connected to that remote storage indicating that the node has been updated. Clients must receive this signal and configure logic to call the **forceUpdate** method; however, this goes beyond the scope of the library, so you must implement it yourself.

### Concurrency Issue in Asynchronous Trees

This issue occurs only in asynchronous trees and can also occur in a 1:1 relationship between remote storage and client. During the process of inserting/removing data asynchronously, querying the data can result in inconsistent data. To prevent concurrency issues, do not query data while inserting/removing it.

## LICENSE

MIT LICENSE
