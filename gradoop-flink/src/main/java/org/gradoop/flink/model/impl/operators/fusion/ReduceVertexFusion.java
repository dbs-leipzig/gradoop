package org.gradoop.flink.model.impl.operators.fusion;

import org.gradoop.flink.model.api.operators.GraphGraphGraphCollectionToGraph;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;

/**
 * Created by Giacomo Bergami on 21/02/17.
 */
public class ReduceVertexFusion implements GraphGraphGraphCollectionToGraph {
  @Override
  public LogicalGraph execute(LogicalGraph left, LogicalGraph right,
    GraphCollection hypervertices) {
    return null;
  }

  @Override
  public String getName() {
    return ReduceVertexFusion.class.getName();
  }
}
