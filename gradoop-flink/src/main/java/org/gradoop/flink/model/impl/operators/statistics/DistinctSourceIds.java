
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.operators.count.Count;

/**
 * Computes the number of distinct source vertex ids.
 */
public class DistinctSourceIds implements UnaryGraphToValueOperator<DataSet<Long>> {

  @Override
  public DataSet<Long> execute(LogicalGraph graph) {
    return Count.count(
      graph.getEdges()
        .map(new SourceId<>())
        .distinct()
    );
  }
}
