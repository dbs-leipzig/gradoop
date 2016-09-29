package org.gradoop.flink.algorithms.fsm.ccs.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.ccs.tuples.CCSSubgraph;
import org.gradoop.flink.algorithms.fsm.ccs.tuples.CCSSubgraphEmbeddings;

public class CCSWrapInSubgraphEmbeddings implements
  MapFunction<CCSSubgraph, CCSSubgraphEmbeddings> {

  private final CCSSubgraphEmbeddings reuseTuple;

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
