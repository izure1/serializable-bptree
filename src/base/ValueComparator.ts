export abstract class ValueComparator<V> {
  /**
   * Implement an algorithm that sorts values in ascending order.  
   * If it returns a negative number, a is less than b. If it returns 0, the two values are equal. If it returns a positive number, a is greater than b.
   * @param a Value a.
   * @param b Value b.
   */
  abstract asc(a: V, b: V): number

  /**
   * The `match` method is used for the **LIKE** operator.
   * This method specifies which value to test against a regular expression.
   * 
   * For example, if you have a tree with values of the structure `{ country: string, capital: number }`,
   * and you want to perform a **LIKE** operation based on the **capital** value, the method should return **value.capital**.
   * In this case, you **CANNOT** perform a **LIKE** operation based on the **country** attribute.
   * The returned value must be a string.
   * 
   * ```
   * interface MyObject {
   *   country: string
   *   capital: string
   * }
   *
   * class CompositeComparator extends ValueComparator<MyObject> {
   *   match(value: MyObject): string {
   *     return value.capital
   *   }
   * }
   * ```
   * 
   * For a tree with simple structure, without complex nesting, returning the value directly would be sufficient.
   * 
   * ```
   * class StringComparator extends ValueComparator<string> {
   *   match(value: string): string {
   *     return value
   *   }
   * }
   * ```
   * 
   * @param value The inserted value.
   * @returns The value to test against a regular expression.
   */
  abstract match(value: V): string

  isLower(value: V, than: V): boolean {
    return this.asc(value, than) < 0
  }

  isSame(value: V, than: V): boolean {
    return this.asc(value, than) === 0
  }

  isHigher(value: V, than: V): boolean {
    return this.asc(value, than) > 0
  }
}

export class NumericComparator extends ValueComparator<number> {
  asc(a: number, b: number): number {
    return a-b
  }

  match(value: number): string {
    return value.toString()
  }
}

export class StringComparator extends ValueComparator<string> {
  asc(a: string, b: string): number {
    return a.localeCompare(b)
  }

  match(value: string): string {
    return value
  }
}
