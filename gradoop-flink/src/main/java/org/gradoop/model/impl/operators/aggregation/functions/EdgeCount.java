package org.gradoop.model.impl.operators.aggregation.functions;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.AggregateFunction;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.tuple.ValueOfTuple1;
import org.gradoop.model.impl.functions.counting.Tuple1With1L;

public class EdgeCount
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements AggregateFunction<Long, G, V, E> {

  @Override
  public DataSet<Long> execute(LogicalGraph<G, V, E> graph) {
    return graph
      .getEdges()
      .map(new Tuple1With1L<E>())
      .sum(0)
      .map(new ValueOfTuple1<Long>());
  }
}
