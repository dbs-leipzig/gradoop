package org.gradoop.flink.algorithms.fsm.gspan;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm_old.common.config.Constants;
import org.gradoop.flink.algorithms.fsm.tuples.GraphEmbeddingPair;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

import java.util.Collection;

/**
 * Created by peet on 21.11.16.
 */
public class PatternGrowth
  extends RichMapFunction<GraphEmbeddingPair, GraphEmbeddingPair> {

  private Collection<TraversalCode<String>> frequentSubgraphs;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.frequentSubgraphs = getRuntimeContext().getBroadcastVariable(Constants.FREQUENT_SUBGRAPHS);
  }

  @Override
  public GraphEmbeddingPair map(GraphEmbeddingPair graphEmbeddingPair) throws Exception {

    if (graphEmbeddingPair.getAdjacencyList().getRows().isEmpty()) {
      for (TraversalCode<String> code : frequentSubgraphs) {
        graphEmbeddingPair.getCodeEmbeddings().put(code, null);
      }
    } else {
      GSpan.growChildren(graphEmbeddingPair, frequentSubgraphs);
    }

    return graphEmbeddingPair;
  }
}
