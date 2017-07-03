
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CCSSubgraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CCSSubgraphEmbeddings;

/**
 * (graphId, size, canonicalLabel, embeddings)
 *   => (canonicalLabel, frequency=1, sample embedding)
 */
public class CCSSubgraphOnly
  implements MapFunction<CCSSubgraphEmbeddings, CCSSubgraph> {

  /**
   * reuse tuple to avoid instantiations
   */
  private final CCSSubgraph reuseTuple =
    new CCSSubgraph(null, null, 1L, null, false);

  @Override
  public CCSSubgraph map(
    CCSSubgraphEmbeddings subgraphEmbeddings) throws Exception {

    reuseTuple.setCategory(subgraphEmbeddings.getCategory());

    reuseTuple
      .setCanonicalLabel(subgraphEmbeddings.getCanonicalLabel());

    reuseTuple
      .setEmbedding(subgraphEmbeddings.getEmbeddings().iterator().next());

    return reuseTuple;
  }
}
