package org.gradoop.model.impl.pojo;

import org.gradoop.model.impl.id.GradoopId;

/**
 * Contains methods that are used by all factories.
 */
public abstract class ElementPojoFactory {
  /**
   * Checks if the given identifier is valid.
   *
   * @param id identifier
   */
  protected void checkId(final GradoopId id) {
    if (id == null) {
      throw new IllegalArgumentException("id must not be null");
    }
  }

  /**
   * Checks if {@code label} is valid (i.e. not {@code null}).
   *
   * @param label edge label
   */
  protected void checkLabel(String label) {
    if (label == null) {
      throw new IllegalArgumentException("label must not be null");
    }
  }
}
