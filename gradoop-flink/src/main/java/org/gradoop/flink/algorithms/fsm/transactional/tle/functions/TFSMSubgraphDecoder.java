
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.MapFunction;

import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.TFSMSubgraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * FSM subgraph -> Gradoop graph transaction.
 */
public class TFSMSubgraphDecoder extends SubgraphDecoder
  implements MapFunction<TFSMSubgraph, GraphTransaction> {

  /**
   * Label of frequent subgraphs.
   */
  private static final String SUBGRAPH_LABEL = "FrequentSubgraph";

  /**
   * Constructor.
   *
   * @param config Gradoop Flink configuration
   */
  public TFSMSubgraphDecoder(GradoopFlinkConfig config) {
    super(config);
  }

  @Override
  public GraphTransaction map(TFSMSubgraph value) throws Exception {
    return createTransaction(value, SUBGRAPH_LABEL);
  }

}
