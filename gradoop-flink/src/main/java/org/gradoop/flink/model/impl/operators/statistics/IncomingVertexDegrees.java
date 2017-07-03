
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.operators.statistics.functions.SetOrCreateWithCount;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Computes the incoming degree for each vertex.
 */
public class IncomingVertexDegrees
  implements UnaryGraphToValueOperator<DataSet<WithCount<GradoopId>>> {

  @Override
  public DataSet<WithCount<GradoopId>> execute(LogicalGraph graph) {
    return new EdgeValueDistribution<>(new TargetId<>()).execute(graph)
      .rightOuterJoin(graph.getVertices().map(new Id<>()))
      .where(0).equalTo("*")
      .with(new SetOrCreateWithCount());
  }
}
