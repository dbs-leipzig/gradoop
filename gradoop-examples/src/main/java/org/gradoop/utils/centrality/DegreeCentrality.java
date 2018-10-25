package org.gradoop.utils.centrality;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.statistics.VertexCount;
import org.gradoop.flink.model.impl.operators.statistics.VertexDegrees;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.model.impl.operators.statistics.functions.CalculateDegreeCentrality;
import org.gradoop.flink.model.impl.operators.statistics.functions.DegreeDistanceFunction;

public class DegreeCentrality extends AbstractRunner implements ProgramDescription {

  private static final String DEGREE_KEY = "degree";

  public static void main(String[] args) throws Exception {

    // read logical Graph
    LogicalGraph graph = readLogicalGraph(args[0], args[1]);


    DataSet<WithCount<GradoopId>> degrees = new VertexDegrees().execute(graph);
    DataSet<Long> vertexCount = new VertexCount().execute(graph);


    // broadcasting
    DataSet<WithCount<GradoopId>> maxDegree = degrees.max(1);


    String broadcastName = "degree_max";
    DataSet<Double> degree = degrees
      .map(new DegreeDistanceFunction(broadcastName))
      .withBroadcastSet(maxDegree, broadcastName)
      .sum(0)
      .crossWithTiny(vertexCount).with(new CalculateDegreeCentrality());
    degree.print();

    // aggregiere Summe
    //Dataset<Double> finalDegree = distances.sum(0).crossWithTiny(maxDegree).with(new BigDegreeCrossFunction());




  }

  @Override
  public String getDescription() {
    return DegreeCentrality.class.getName();
  }

}
