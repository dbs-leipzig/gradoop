package org.gradoop.flink.algorithms.fsm.tfsm.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.tfsm.tuples.TFSMSubgraphEmbeddings;

public class IsResult implements FilterFunction<TFSMSubgraphEmbeddings> {

  private final boolean shouldBeCollector;

  public IsResult(boolean shouldBeCollector) {
    this.shouldBeCollector = shouldBeCollector;
  }

  @Override
  public boolean filter(TFSMSubgraphEmbeddings embeddings) throws Exception {
    boolean isCollector = embeddings.getGraphId().equals(GradoopId.NULL_VALUE);

    return shouldBeCollector == isCollector;
  }

}
