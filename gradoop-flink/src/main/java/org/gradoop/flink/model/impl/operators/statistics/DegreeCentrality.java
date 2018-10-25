package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.operators.statistics.functions.CalculateDegreeCentrality;
import org.gradoop.flink.model.impl.operators.statistics.functions.DegreeDistanceFunction;
import org.gradoop.flink.model.impl.tuples.WithCount;

public class DegreeCentrality
  implements UnaryGraphToValueOperator<DataSet<Double>> {

  @Override
  public DataSet<Double> execute(LogicalGraph graph) {


    DataSet<WithCount<GradoopId>> degrees = new VertexDegrees().execute(graph);
    DataSet<Long> vertexCount = new VertexCount().execute(graph);

    // for broadcasting
    DataSet<WithCount<GradoopId>> maxDegree = degrees.max(1);
    String broadcastName = "degree_max";

    DataSet<Double> degreeCentrality = degrees
      .map(new DegreeDistanceFunction(broadcastName))
      .withBroadcastSet(maxDegree, broadcastName)
      .sum(0)
      .crossWithTiny(vertexCount).with(new CalculateDegreeCentrality());

    return degreeCentrality;
  }
}

