package org.gradoop.flink.algorithms.fsm.tfsm.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.tfsm.tuples.TFSMSubgraph;
import org.gradoop.flink.algorithms.fsm.tfsm.tuples.TFSMSubgraphEmbeddings;

public class TFSMWrapInSubgraphEmbeddings implements
  MapFunction<TFSMSubgraph, TFSMSubgraphEmbeddings> {

  private final TFSMSubgraphEmbeddings reuseTuple;

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
