
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.transactional.CategoryCharacteristicSubgraphs;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CCSSubgraph;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * FSM subgraph -> Gradoop graph transaction.
 */
public class CCSSubgraphDecoder extends SubgraphDecoder
  implements MapFunction<CCSSubgraph, GraphTransaction> {

  /**
   * Label of frequent subgraphs.
   */
  private static final String SUBGRAPH_LABEL = "CharacteristicSubgraph";

  /**
   * Constructor.
   *
   * @param config Gradoop Flink configuration
   */
  public CCSSubgraphDecoder(GradoopFlinkConfig config) {
    super(config);
  }

  @Override
  public GraphTransaction map(CCSSubgraph value) throws Exception {
    GraphTransaction transaction = createTransaction(value, SUBGRAPH_LABEL);

    transaction.getGraphHead().setProperty(
      CategoryCharacteristicSubgraphs.CATEGORY_KEY,
      value.getCategory()
    );

    return transaction;
  }

}
