package org.gradoop.utils.centrality;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.statistics.VertexDegrees;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.utils.centrality.functions.AddDegreeJoinFunction;

public class DegreeCentrality extends AbstractRunner implements ProgramDescription {

  private static final String DEGREE_KEY = "degree";

  public static void main(String[] args) throws Exception {

     // read logical Graph
     LogicalGraph graph = readLogicalGraph(args[0], args[1]);


     DataSet<WithCount<GradoopId>> degrees = new VertexDegrees().execute(graph);


     // broadcasting
     DataSet<WithCount<GradoopId>> maxDegree = degrees.max(1);



     DataSet<Vertex> vertices = graph.getVertices()
       .join(degrees)
       .where(new Id<>()).equalTo(0)
       .with(new AddDegreeJoinFunction(DEGREE_KEY));


     // aggregiere Summe



  }

  @Override
  public String getDescription() {
    return DegreeCentrality.class.getName();
  }
}
