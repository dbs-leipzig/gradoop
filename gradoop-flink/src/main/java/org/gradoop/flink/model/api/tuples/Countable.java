
package org.gradoop.flink.model.api.tuples;

/**
 * Something countable.
 */
public interface Countable {

  /**
   * Getter.
   *
   * @return count
   */
  long getCount();

  /**
   * Setter.
   *
   * @param count count
   */
  void setCount(long count);
}
