
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.Tuple2ToWithCount;
import org.gradoop.flink.model.impl.operators.count.functions.Tuple2FromTupleWithObjectAnd1L;
import org.gradoop.flink.model.impl.operators.statistics.functions.ToSourceIdWithLabel;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Computes the number of distinct source ids per edge label.
 */
public class DistinctSourceIdsByEdgeLabel
  implements UnaryGraphToValueOperator<DataSet<WithCount<String>>> {

  @Override
  public DataSet<WithCount<String>> execute(LogicalGraph graph) {
    return graph.getEdges()
      .map(new ToSourceIdWithLabel<>())
      .groupBy(0, 1)
      .first(1)
      .<Tuple1<String>>project(1)
      .map(new Tuple2FromTupleWithObjectAnd1L<>())
      .groupBy(0)
      .sum(1)
      .map(new Tuple2ToWithCount<>());
  }
}
