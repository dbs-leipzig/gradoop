package org.gradoop.model;

/**
 * A multi-labeled entity is tagged with zero or more labels.
 */
public interface MultiLabeled {
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
