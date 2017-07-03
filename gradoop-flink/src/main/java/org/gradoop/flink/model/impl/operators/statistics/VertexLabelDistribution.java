
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Label;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Convenience operator to compute the vertex label distribution.
 *
 * For each vertex label, the output contains a tuple consisting of the label and the number of
 * vertices with that label.
 */
public class VertexLabelDistribution
  implements UnaryGraphToValueOperator<DataSet<WithCount<String>>> {

  @Override
  public DataSet<WithCount<String>> execute(LogicalGraph graph) {
    return new VertexValueDistribution<>(new Label<>()).execute(graph);
  }
}
