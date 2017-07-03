
package org.gradoop.flink.model.impl.operators.combination;

import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.api.operators.ReducibleBinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.GraphCollection;

/**
 * Computes the combined graph from a collection of logical graphs.
 */
public class ReduceCombination implements ReducibleBinaryGraphToGraphOperator {

  /**
   * Creates a new logical graph by union the vertex and edge sets of all graph
   * contained in the given collection.
   *
   * @param collection input collection
   * @return combined graph
   */
  @Override
  public LogicalGraph execute(GraphCollection collection) {
    return LogicalGraph.fromDataSets(
      collection.getVertices(),
      collection.getEdges(),
      collection.getConfig());
  }

  @Override
  public String getName() {
    return ReduceCombination.class.getName();
  }
}
