package org.gradoop.model.impl.operators.aggregation.functions;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.functions.AggregateFunction;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.count.Count;

/**
 * Aggregate function returning the vertex count of a graph.
 *
 * @param <G> graph head type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class VertexCount
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements AggregateFunction<Long, G, V, E> {

  @Override
  public DataSet<Long> execute(LogicalGraph<G, V, E> graph) {
    return Count.count(graph.getVertices());
  }
}
