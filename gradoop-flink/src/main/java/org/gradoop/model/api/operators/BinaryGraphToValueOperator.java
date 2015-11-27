package org.gradoop.model.api.operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.model.LogicalGraph;

/**
 * Creates a (usually 1-element) Boolean dataset based on two input graphs.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <G> EPGM graph head type
 * @param <T> value type
 */
public interface BinaryGraphToValueOperator
  <V extends EPGMVertex, E extends EPGMEdge, G extends EPGMGraphHead, T>
  extends Operator {

  /**
   * Executes the operator.
   *
   * @param firstGraph  first input graph
   * @param secondGraph second input graph
   * @return operator result
   */
  DataSet<T> execute(
    LogicalGraph<G, V, E> firstGraph,
    LogicalGraph<G, V, E> secondGraph
  );
}
