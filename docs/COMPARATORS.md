# Value Comparator

In a B+tree, values must be kept in a sorted order. The process of comparing value sizes is handled by the **ValueComparator**.

## Built-in Comparators

`serializable-bptree` provides native support for common data types:

```typescript
import { NumericComparator, StringComparator } from 'serializable-bptree'
```

- **NumericComparator**: For comparing numbers.
- **StringComparator**: For comparing strings.

## Custom Comparators

To sort complex objects, you can create a custom class that inherits from `ValueComparator`.

### Example: Sorting by Object Property

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

### Methods to Implement

#### `asc(a: T, b: T): number`

The `asc` method defines the sorting order:
- **Negative value**: `a` < `b`
- **Positive value**: `a` > `b`
- **Zero (0)**: `a` == `b`

#### `match(value: T): string`

The `match` method is used for the **LIKE** operator and regular expression testing. It specifies which property of the object should be used for string-based matching.

```typescript
interface MyObject {
  country: string
  capital: string
}

class CompositeComparator extends ValueComparator<MyObject> {
  // ... asc implementation ...
  
  match(value: MyObject): string {
    return value.capital // Only the 'capital' property will be tested for LIKE queries
  }
}
```

> [!NOTE]
> The value returned by `match` must always be a **string**.

#### `primaryAsc(a: T, b: T): number` (Optional)

`primaryAsc` is used to define a **primary sorting group**. This is particularly useful for **Composite Values** where you want to group entries by a specific field while maintaining uniqueness with another field.

- **Purpose**: Enables the `primaryEqual` query operator.
- **Behavior**: If `primaryAsc` is not implemented, it defaults to the `asc` method.
- **Requirement**: The comparison logic in `primaryAsc` must be **consistent** with `asc`. If `primaryAsc(a, b)` returns a non-zero value, `asc(a, b)` must return the same sign.

```typescript
interface MyValue {
  category: string
  id: number
}

class MyComparator extends ValueComparator<MyValue> {
  // Strict sorting for uniqueness
  asc(a: MyValue, b: MyValue): number {
    const diff = a.category.localeCompare(b.category)
    return diff === 0 ? (a.id - b.id) : diff
  }

  // Grouping by category only
  primaryAsc(a: MyValue, b: MyValue): number {
    return a.category.localeCompare(b.category)
  }
}
```

By defining `primaryAsc`, you can query all items in a category using `primaryEqual`:
```typescript
tree.where({ primaryEqual: { category: 'electronics' } })
```

