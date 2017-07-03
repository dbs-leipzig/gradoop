
package org.gradoop.common.model.api.entities;

import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Describes an identifiable entity.
 */
public interface EPGMIdentifiable {
  /**
   * Returns the identifier of that entity.
   *
   * @return identifier
   */
  GradoopId getId();

  /**
   * Sets the identifier of that entity.
   *
   * @param id identifier
   */
  void setId(GradoopId id);
}
