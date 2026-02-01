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

### Manual Filtering (`get()` and `verify()`)

For conditions that are not part of the primary index search (the driver), you can use `get()` to retrieve supplemental data and `verify()` to check conditions without manual value comparison.

- **`get(key)`**: Performs a full scan to find a value by its key. Useful for secondary index lookups during a stream.
- **`verify(value, condition)`**: Checks if a given value satisfies a B+Tree condition.

```typescript
for (const [pk, val] of driver.tree.whereStream(driver.condition)) {
  const otherValue = otherIndex.get(pk)
  if (otherValue !== undefined && otherIndex.verify(otherValue, otherCondition)) {
    // Result matches both conditions
  }
}
```

### Performance Note

Primary operators (`primaryEqual`, `primaryGt`, etc.) are highly optimized. They utilize the B+Tree's internal structure to perform range scans, significantly reducing the number of nodes visited compared to generic operators.
