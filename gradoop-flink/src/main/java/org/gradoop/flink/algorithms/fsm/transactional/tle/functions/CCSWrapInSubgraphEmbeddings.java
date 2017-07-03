
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CCSSubgraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CCSSubgraphEmbeddings;

/**
 * subgraphWithSampleEmbedding => subgraphWithEmbeddings
 */
public class CCSWrapInSubgraphEmbeddings implements
  MapFunction<CCSSubgraph, CCSSubgraphEmbeddings> {

  /**
   * reuse tuple to avoid instantiations
   */
  private final CCSSubgraphEmbeddings reuseTuple;

  /**
   * Constructor.
   */
  public CCSWrapInSubgraphEmbeddings() {
    this.reuseTuple = new CCSSubgraphEmbeddings();
    this.reuseTuple.setGraphId(GradoopId.NULL_VALUE);
  }

  @Override
  public CCSSubgraphEmbeddings map(CCSSubgraph subgraph) throws
    Exception {

    reuseTuple.setCanonicalLabel(subgraph.getCanonicalLabel());
    reuseTuple.setCategory(subgraph.getCategory());
    reuseTuple.setSize(subgraph.getEmbedding().getEdges().size());
    reuseTuple.setEmbeddings(Lists.newArrayList(subgraph.getEmbedding()));

    return reuseTuple;
  }
}
