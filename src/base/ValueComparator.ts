export abstract class ValueComparator<V> {
  /**
   * Implement an algorithm that sorts values in ascending order.  
   * If it returns a negative number, a is less than b. If it returns 0, the two values are equal. If it returns a positive number, a is greater than b.
   * @param a Value a.
   * @param b Value b.
   */
  abstract asc(a: V, b: V): number
  
  /**
   * This method is used for the `like` operator.
   * It should return the value that the regular expression needs to examine.
   * For example, if comparing with the like operator on the `value.name` attribute, it should return the value of `value.name`.
   * @param value The inserted value.
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
