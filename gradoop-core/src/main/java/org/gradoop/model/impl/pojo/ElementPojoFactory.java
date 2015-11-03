package org.gradoop.model.impl.pojo;

/**
 * Contains methods that are used by all factories.
 */
public abstract class ElementPojoFactory {
  /**
   * Checks if the given identifier is valid.
   *
   * @param id identifier
   */
  protected void checkId(final Long id) {
    if (id == null) {
      throw new IllegalArgumentException("id must not be null");
    }
  }

  /**
   * Checks if {@code label} is valid (not null or empty).
   *
   * @param label edge label
   */
  protected void checkLabel(String label) {
    if (label == null || "".equals(label)) {
      throw new IllegalArgumentException("label must not be null or empty");
    }
  }
}
