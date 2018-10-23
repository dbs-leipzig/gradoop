package org.gradoop.utils.centrality;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.flink.model.impl.operators.statistics.VertexDegrees;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class DegreeCentrality extends AbstractRunner implements ProgramDescription {

  private static final String DEGREE_KEY = "degree";

  public static void main(String[] args) throws Exception {

    // read logical Graph
    // LogicalGraph graph = readLogicalGraph(args[0], args[1]);


    // DataSet<WithCount<GradoopId>> degrees = new VertexDegrees().execute(graph).join();


    String graphString = ""
        + "g0:graph["
        + "(p0:Person {name: \"Alice\"})"
        + "(p1:Person {name: \"Bob\"})"
        + "(p2:Person {name: \"Jacob\"})"
        + "(p3:Person {name: \"Mark\"})"
        + "(p0)-[]->(p1)"
        + "(p1)-[]->(p0)"
        + "(p1)-[]->(p2)"
        + "(p2)-[]->(p1)"
        + "(p1)-[]->(p3)"
        + "(p3)-[]->(p1)"
        + "(p2)-[]->(p3)"
        + "(p3)-[]->(p2)"
        + "]";

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(config);
    loader.initDatabaseFromString(graphString);


    LogicalGraph graph = loader.getLogicalGraph();
    graph.getVertices().print();

    DataSet<WithCount<GradoopId>> degrees = new VertexDegrees().execute(graph);
    degrees.print();

    TransformationFunction<Vertex> addDegree = new TransformationFunction<Vertex>() {
      @Override
      public Vertex apply(Vertex vertex, Vertex el1) {
        vertex.setProperty("university", "Leipzig University");
        return vertex;
      }
    };


    graph.print();
  }

  @Override
  public String getDescription() {
    return DegreeCentrality.class.getName();
  }
}
