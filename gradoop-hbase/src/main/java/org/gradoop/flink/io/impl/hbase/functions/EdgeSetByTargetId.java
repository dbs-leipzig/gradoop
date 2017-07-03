
package org.gradoop.flink.io.impl.hbase.functions;

import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Takes grouped edges as input and outputs a tuple containing target vertex id
 * and the edges.
 *
 * edge+ -> (targetId, edge+)
 *
 * @param <E> EPGM edge type
 */
public class EdgeSetByTargetId<E extends Edge> extends EdgeSet<E> {

  /**
   * Constructor
   */
  public EdgeSetByTargetId() {
    extractBySourceId = false;
  }
}
