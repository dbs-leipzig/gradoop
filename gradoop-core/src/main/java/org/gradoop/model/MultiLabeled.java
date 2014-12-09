package org.gradoop.model;

/**
 * A multi-labeled entity is tagged with zero or more labels.
 */
public interface MultiLabeled {
  /**
   * Returns all labels of that entity.
   *
   * @return all labels or {@code null} if entity has no labels.
   */
  Iterable<String> getLabels();

  /**
   * Adds a label to the set of labels.
   *
   * @param label label to be added (must be not null and non-empty)
   */
  void addLabel(String label);

  /**
   * Returns the number of labels that entity has.
   *
   * @return number of labels
   */
  int getLabelCount();
}
