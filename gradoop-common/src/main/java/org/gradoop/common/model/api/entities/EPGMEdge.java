
package org.gradoop.common.model.api.entities;

import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Describes data assigned to an edge in the EPGM.
 */
public interface EPGMEdge extends EPGMGraphElement {
  /**
   * Returns the source vertex identifier.
   *
   * @return source vertex id
   */
  GradoopId getSourceId();

  /**
   * Sets the source vertex identifier.
   *
   * @param sourceId source vertex id
   */
  void setSourceId(GradoopId sourceId);

  /**
   * Returns the target vertex identifier.
   *
   * @return target vertex id
   */
  GradoopId getTargetId();

  /**
   * Sets the target vertex identifier.
   *
   * @param targetId target vertex id.
   */
  void setTargetId(GradoopId targetId);
}
