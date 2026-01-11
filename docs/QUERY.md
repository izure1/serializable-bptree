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

## Combining Conditions

Multiple conditions in a single query object are treated as an **AND** operation.

```typescript
tree.where({ 
  gt: 10, 
  lt: 50, 
  notEqual: 25 
})
```
