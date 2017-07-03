
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.Subgraph;

/**
 * Filters a subgraph by minimum edge count.
 *
 * @param <S> subgraph type
 */
public class MinEdgeCount<S extends Subgraph> implements FilterFunction<S> {

  /**
   * Minimum number of edges
   */
  private final int minEdgeCount;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public MinEdgeCount(FSMConfig fsmConfig) {
    this.minEdgeCount = fsmConfig.getMinEdgeCount();
  }

  @Override
  public boolean filter(S subgraph) throws Exception {
    return subgraph.getEmbedding().getEdges().size() >= minEdgeCount;
  }
}
