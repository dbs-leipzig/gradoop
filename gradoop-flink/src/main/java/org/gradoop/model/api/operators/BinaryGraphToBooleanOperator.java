package org.gradoop.model.api.operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;

/**
 * Creates a (usually 1-element) Boolean dataset based on two input graphs.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <G> EPGM graph head type
 */
public interface BinaryGraphToBooleanOperator
  <V extends EPGMVertex, E extends EPGMEdge, G extends EPGMGraphHead>
  extends Operator {

  /**
   * Executes the operator.
   *
   * @param firstGraph  first input graph
   * @param secondGraph second input graph
   * @return operator result
   */
  DataSet<Boolean> execute(
    LogicalGraph<V, E, G> firstGraph,
    LogicalGraph<V, E, G> secondGraph
  );
}
