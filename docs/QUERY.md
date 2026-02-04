# Query Conditions

`serializable-bptree` supports various operators for querying data via the `where()` method.

## Supported Operators

### Comparison Operators

- **`gte`**: Greater than or equal to.
- **`gt`**: Greater than.
- **`lte`**: Less than or equal to.
- **`lt`**: Less than.
- **`equal`**: Exact match.
- **`notEqual`**: Does not match.

Example:
```typescript
tree.where({ gte: 10, lt: 20 }) // 10 <= x < 20
```

### Logical Operators

- **`or`**: Matches if any of the provided conditions are met. Accepts an array of values.

Example:
```typescript
tree.where({ or: [1, 3, 5] })
```

### String Matching

- **`like`**: SQL-like string matching using regular expressions. Accepts a string pattern directly.
  - `%`: Matches zero or more characters.
  - `_`: Matches exactly one character.

Example:
```typescript
tree.where({ like: 'user_%' }) // Matches 'user_1', 'user_admin', etc.
```

### Primary Operators

If you are using composite values and have defined `primaryAsc` in your `ValueComparator`, you can use these operators to query based on the primary sorting group.

- **`primaryEqual`**: Matches if the primary group is equal.
- **`primaryNotEqual`**: Matches if the primary group is not equal.
- **`primaryGt`**: Primary group is greater than.
- **`primaryGte`**: Primary group is greater than or equal to.
- **`primaryLt`**: Primary group is less than.
- **`primaryLte`**: Primary group is less than or equal to.
- **`primaryOr`**: Matches if the primary group matches any of the provided values.

Example:
```typescript
// Assuming value is { group: number, id: number } 
// and primaryAsc compares the 'group' field.
tree.where({ primaryGte: { group: 5 } })
```

## Combining Conditions

Multiple conditions in a single query object are treated as an **AND** operation.

```typescript
tree.where({ 
  gt: 10, 
  lt: 50, 
  notEqual: 25 
})
```

## Performance & Optimization

`serializable-bptree` provides tools for efficient querying, especially when dealing with multiple indexes.

### Query Optimizer (`ChooseDriver`)

The `ChooseDriver` static method helps select the best index (driver) based on the query conditions. It evaluates the complexity of conditions and assigns priorities:
- Higher priority: `equal`, `primaryEqual` (High selectivity)
- Medium priority: `or`, `primaryOr`, `gt`, `lt`, `gte`, `lte`
- Lower priority: `like`, `notEqual`

```typescript
const driver = BPTreeSync.ChooseDriver([
  { tree: indexA, condition: { equal: 10 } },
  { tree: indexB, condition: { gt: 100 } }
])
// returns the candidate with indexA because 'equal' has higher priority.
```

### Multi-Index Filtering (`keys()`)

When querying with multiple indexes, you should use `ChooseDriver` to select the most efficient starting index and then refine the results using `keys()` on other indexes. This is much more efficient than manual filtering.

```typescript
const query = { id: { equal: 100 }, age: { gt: 20 } }

// 1. Select the best index based on condition priority
const candidates = [
  { tree: idxId, condition: query.id },
  { tree: idxAge, condition: query.age }
]
const driver = BPTreeSync.ChooseDriver(candidates)
const others = candidates.filter((c) => driver.tree !== c.tree)

// 2. Execute query using the selected driver
let keys = driver.tree.keys(driver.condition)
for (const { tree, condition } of others) {
  // Refine the key set using other indexes
  keys = tree.keys(condition, keys)
}

console.log('Found result keys: ', keys)
```

### Performance Note

Primary operators (`primaryEqual`, `primaryGt`, etc.) are highly optimized. They utilize the B+Tree's internal structure to perform range scans, significantly reducing the number of nodes visited compared to generic operators.
