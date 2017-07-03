
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.TFSMSubgraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.TFSMSubgraphEmbeddings;

/**
 * subgraphWithSampleEmbedding => subgraphWithEmbeddings
 */
public class TFSMWrapInSubgraphEmbeddings implements
  MapFunction<TFSMSubgraph, TFSMSubgraphEmbeddings> {

  /**
   * reuse tuple to avoid instantiations
   */
  private final TFSMSubgraphEmbeddings reuseTuple;

  /**
   * Constructor.
   */
  public TFSMWrapInSubgraphEmbeddings() {
    this.reuseTuple = new TFSMSubgraphEmbeddings();
    this.reuseTuple.setGraphId(GradoopId.NULL_VALUE);
    this.reuseTuple.setSize(0);
  }

  @Override
  public TFSMSubgraphEmbeddings map(TFSMSubgraph tfsmSubgraph) throws
    Exception {

    reuseTuple.setCanonicalLabel(tfsmSubgraph.getCanonicalLabel());
    reuseTuple.setEmbeddings(Lists.newArrayList(tfsmSubgraph.getEmbedding()));

    return reuseTuple;
  }
}
