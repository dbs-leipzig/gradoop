package org.gradoop.model;

/**
 * Created by martin on 05.11.14.
 */
public interface Labeled {
  /**
   * Returns all labels of that entity.
   *
   * @return all labels or an empty {@code Iterable}
   */
  Iterable<String> getLabels();

  /**
   * Adds a label to the set of labels.
   *
   * @param label label to be added (must be not null and non-empty)
   */
  void addLabel(String label);
}
