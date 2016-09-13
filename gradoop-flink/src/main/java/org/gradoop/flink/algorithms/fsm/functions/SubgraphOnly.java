package org.gradoop.flink.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.tuples.Subgraph;
import org.gradoop.flink.algorithms.fsm.tuples.SubgraphEmbeddings;

/**
 * Created by peet on 09.09.16.
 */
public class SubgraphOnly
  implements MapFunction<SubgraphEmbeddings, Subgraph> {

  private final Subgraph reuseTuple =
    new Subgraph(null, 1L, null);

  @Override
  public Subgraph map(
    SubgraphEmbeddings subgraphEmbeddings) throws Exception {

    reuseTuple
      .setSubgraph(subgraphEmbeddings.getSubgraph());

    reuseTuple
      .setEmbedding(subgraphEmbeddings.getEmbeddings().iterator().next());

    return reuseTuple;
  }
}
