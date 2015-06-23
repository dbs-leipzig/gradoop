package org.gradoop.model;

/**
 * Entity is identifiable in a given context.
 */
public interface Identifiable {
  /**
   * Returns the identifier of that entity.
   *
   * @return identifier
   */
  Long getId();

  /**
   * Sets the identifier of that entity.
   *
   * @param id identifier
   */
  void setId(Long id);
}
