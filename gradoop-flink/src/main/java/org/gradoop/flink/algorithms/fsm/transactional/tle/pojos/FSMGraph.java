
package org.gradoop.flink.algorithms.fsm.transactional.tle.pojos;

import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Map;

/**
 * Describe a FSM-fitted graph representation.
 */
public interface FSMGraph {

  /**
   * Getter.
   *
   * @return id-vertex label map
   */
  Map<Integer, String> getVertices();

  /**
   * Setter.
   *
   * @return id-edge map
   */
  Map<Integer, FSMEdge> getEdges();

  /**
   * Getter.
   *
   * @return graph id
   */
  GradoopId getId();
}
