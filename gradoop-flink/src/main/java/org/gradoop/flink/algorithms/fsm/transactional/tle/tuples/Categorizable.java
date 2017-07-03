
package org.gradoop.flink.algorithms.fsm.transactional.tle.tuples;

/**
 * Describes something categorizable.
 */
public interface Categorizable {

  /**
   * Getter.
   *
   * @return category
   */
  String getCategory();

  /**
   * Setter.
   *
   * @param category category
   */
  void setCategory(String category);
}
