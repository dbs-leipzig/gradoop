
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.statistics.functions.SumCounts;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Computes the vertex degree for each vertex.
 */
public class VertexDegrees implements UnaryGraphToValueOperator<DataSet<WithCount<GradoopId>>> {

  @Override
  public DataSet<WithCount<GradoopId>> execute(LogicalGraph graph) {
    return new OutgoingVertexDegrees()
      .execute(graph)
      .join(new IncomingVertexDegrees().execute(graph))
      .where(0).equalTo(0)
      .with(new SumCounts<>());
  }
}
