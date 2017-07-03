
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.count.Count;

/**
 * Computes the number of edges in the given graph.
 */
public class EdgeCount implements UnaryGraphToValueOperator<DataSet<Long>> {

  @Override
  public DataSet<Long> execute(LogicalGraph graph) {
    return Count.count(graph.getEdges());
  }
}
