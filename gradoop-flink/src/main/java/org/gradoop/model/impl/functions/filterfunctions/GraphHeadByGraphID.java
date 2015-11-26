package org.gradoop.model.impl.functions.filterfunctions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 26.11.15.
 */
public class GraphHeadByGraphID<G extends EPGMGraphHead> implements FilterFunction<G> {
  private final GradoopId graphId;

  public GraphHeadByGraphID(GradoopId graphID) {
    this.graphId = graphID;
  }

  @Override
  public boolean filter(G graph) throws Exception {
    return graph.getId().equals(graphId);
  }
}
