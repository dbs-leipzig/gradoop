
package org.gradoop.flink.algorithms.fsm.transactional.tle.pojos;

import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Map;

/**
 * Lightweight representation of a labeled graph transaction.
 */
public class CCSGraph extends TFSMGraph {

  /**
   * graph identifier
   */
  private final String category;

  /**
   * Constructor.
   *
   * @param category category
   * @param id graph identifier
   * @param vertices id-vertex map
   * @param edges id-edge map
   */
  public CCSGraph(String category, GradoopId id,
    Map<Integer, String> vertices, Map<Integer, FSMEdge> edges) {
    super(id, vertices, edges);
    this.category = category;
  }

  public String getCategory() {
    return category;
  }
}
