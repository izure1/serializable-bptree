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

- **`or`**: Matches if any of the provided conditions are met. Accepts an array of values or nested conditions.

Example:
```typescript
tree.where({ or: [1, 3, 5] })
```

### String Matching

- **`like`**: SQL-like string matching using regular expressions.
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

### Performance Note

Primary operators are highly optimized. Operators like `primaryEqual`, `primaryGt`, `primaryGte`, `primaryLt`, `primaryLte`, and `primaryOr` utilize the B+Tree's structure to perform range scans, minimizing the number of nodes processed.
