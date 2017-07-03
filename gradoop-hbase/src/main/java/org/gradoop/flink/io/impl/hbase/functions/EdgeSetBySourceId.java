
package org.gradoop.flink.io.impl.hbase.functions;

import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Takes grouped edges as input and outputs a tuple containing source vertex id
 * and the edges.
 *
 * edge+ -> (sourceId, edge+)
 *
 * @param <E> EPGM edge type
 */
public class EdgeSetBySourceId<E extends Edge> extends EdgeSet<E> {

  /**
   * Constructor
   */
  public EdgeSetBySourceId() {
    extractBySourceId = true;
  }
}
