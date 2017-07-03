
package org.gradoop.flink.algorithms.fsm.transactional.tle.pojos;

import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Map;

/**
 * Lightweight representation of a labeled graph transaction.
 */
public class TFSMGraph extends Embedding implements FSMGraph {

  /**
   * graph identifier
   */
  private final GradoopId id;

  /**
   * Constructor.
   *
   * @param id graph identifier
   * @param vertices id-vertex map
   * @param edges id-edge map
   */
  public TFSMGraph(GradoopId id,
    Map<Integer, String> vertices, Map<Integer, FSMEdge> edges) {
    super(vertices, edges);
    this.id = id;
  }

  public GradoopId getId() {
    return id;
  }
}
