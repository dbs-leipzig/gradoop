
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.TFSMSubgraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.TFSMSubgraphEmbeddings;

/**
 * (graphId, size, canonicalLabel, embeddings)
 *   => (canonicalLabel, frequency=1, sample embedding)
 */
public class TFSMSubgraphOnly
  implements MapFunction<TFSMSubgraphEmbeddings, TFSMSubgraph> {

  /**
   * reuse tuple to avoid instantiations
   */
  private final TFSMSubgraph reuseTuple = new TFSMSubgraph(null, 1L, null);

  @Override
  public TFSMSubgraph map(
    TFSMSubgraphEmbeddings subgraphEmbeddings) throws Exception {

    reuseTuple
      .setCanonicalLabel(subgraphEmbeddings.getCanonicalLabel());

    reuseTuple
      .setEmbedding(subgraphEmbeddings.getEmbeddings().iterator().next());

    return reuseTuple;
  }
}
