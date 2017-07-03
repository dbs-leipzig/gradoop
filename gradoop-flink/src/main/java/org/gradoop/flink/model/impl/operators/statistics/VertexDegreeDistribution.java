
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.Tuple2ToWithCount;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Computes the distribution of vertex degrees.
 */
public class VertexDegreeDistribution
  implements UnaryGraphToValueOperator<DataSet<WithCount<Long>>> {

  @Override
  public DataSet<WithCount<Long>> execute(LogicalGraph graph) {
    return Count.groupByFromTuple(
      new VertexDegrees()
        .execute(graph)
        .<Tuple1<Long>>project(1))
      .map(new Tuple2ToWithCount<>());
  }
}
