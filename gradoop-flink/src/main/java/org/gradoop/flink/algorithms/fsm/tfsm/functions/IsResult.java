package org.gradoop.flink.algorithms.fsm.tfsm.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.common.tuples.SubgraphEmbeddings;

public class IsResult<SE extends SubgraphEmbeddings> 
  implements FilterFunction<SE> {

  private final boolean shouldBeCollector;

  public IsResult(boolean shouldBeCollector) {
    this.shouldBeCollector = shouldBeCollector;
  }

  @Override
  public boolean filter(SE embeddings) throws Exception {
    boolean isCollector = embeddings.getGraphId().equals(GradoopId.NULL_VALUE);

    return shouldBeCollector == isCollector;
  }

}
