package org.gradoop.flink.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.tuples.FrequentSubgraph;
import org.gradoop.flink.algorithms.fsm.tuples.SubgraphEmbeddings;

/**
 * Created by peet on 09.09.16.
 */
public class CountableFrequentSubgraph
  implements MapFunction<SubgraphEmbeddings, FrequentSubgraph> {

  private final FrequentSubgraph reuseTuple =
    new FrequentSubgraph(null, 1L, null);

  @Override
  public FrequentSubgraph map(
    SubgraphEmbeddings subgraphEmbeddings) throws Exception {

    reuseTuple
      .setSubgraph(subgraphEmbeddings.getSubgraph());

    reuseTuple
      .setEmbedding(subgraphEmbeddings.getEmbeddings().iterator().next());

    return reuseTuple;
  }
}
