
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Label;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Convenience operator to compute the distribution of edge label.
 *
 * For each edge label, the output contains a tuple consisting of the label and the number of edges
 * with that label.
 */
public class EdgeLabelDistribution
  implements UnaryGraphToValueOperator<DataSet<WithCount<String>>> {

  @Override
  public DataSet<WithCount<String>> execute(LogicalGraph graph) {
    return new EdgeValueDistribution<>(new Label<>()).execute(graph);
  }
}
