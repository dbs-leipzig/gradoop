package org.gradoop.model.api.operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;

/**
 * Describes an aggregate function as input for the
 * {@link org.gradoop.model.impl.operators.aggregation.Aggregation} operator.
 *
 * @param <N> result type of aggregated numeric values
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public interface AggregateFunction<N extends Number,
  G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {

  /**
   * Defines the aggregate function.
   *
   * @param graph input graph
   * @return aggregated value as 1-element dataset
   */
  DataSet<N> execute(LogicalGraph<G, V, E> graph);

}
