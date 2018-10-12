package org.gradoop.utils.centrality;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.flink.model.impl.operators.statistics.VertexDegrees;
import org.gradoop.flink.model.impl.tuples.WithCount;

public class DegreeCentrality extends AbstractRunner implements ProgramDescription {

  private static final String DEGREE_KEY = "degree";

  public static void main(String[] args) throws Exception {

    // read logical Graph
    LogicalGraph graph = readLogicalGraph(args[0], args[1]);


    // DataSet<WithCount<GradoopId>> degrees = new VertexDegrees().execute(graph).join();




  }


  @Override
  public String getDescription() {
    return DegreeCentrality.class.getName();
  }
}
