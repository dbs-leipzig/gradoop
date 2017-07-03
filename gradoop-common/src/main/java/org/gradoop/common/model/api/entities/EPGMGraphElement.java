
package org.gradoop.common.model.api.entities;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * A graph element is part of a logical graph. An element can be part of more
 * than one logical graph. This applies to vertices and edges in the EPGM.
 */
public interface EPGMGraphElement extends EPGMElement {
  /**
   * Returns all graphs that element belongs to.
   *
   * @return all graphs of that element
   */
  GradoopIdList getGraphIds();

  /**
   * Adds that element to the given graphId. If the element is already an
   * element of the given graphId, nothing happens.
   *
   * @param graphId the graphId to be added to
   */
  void addGraphId(GradoopId graphId);

  /**
   * Adds the given graph set to the element.
   *
   * @param graphIds the graphIds to be added
   */
  void setGraphIds(GradoopIdList graphIds);

  /**
   * Resets all graph elements.
   */
  void resetGraphIds();

  /**
   * Returns the number of graphs this element belongs to.
   *
   * @return number of graphs containing that element
   */
  int getGraphCount();
}
