package org.gradoop.model;

/**
 * A single labeled entity is tagged with zero or one label.
 */
public interface SingleLabeled {
  /**
   * Returns the single label of that entity.
   *
   * @return label
   */
  String getLabel();
}
