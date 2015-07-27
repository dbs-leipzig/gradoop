package org.gradoop.model.impl;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.impl.operators.FlinkBTGAlgorithm;
import org.gradoop.model.impl.operators.io.formats.FlinkBTGVertexValue;
import org.junit.Test;

public class FlinkBTGAlgorithmTest {
  @Test
  public void testBTGAlgorithmGraph() throws Exception {
    int maxIteration = 100;
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<Vertex<Long, FlinkBTGVertexValue>> vertices =
      FlinkBTGAlgorithmTestHelper.getConnectedIIGVertices(env);
    DataSet<Edge<Long, Long>> edges =
      FlinkBTGAlgorithmTestHelper.getConnectedIIGEdges(env);
    Graph<Long, FlinkBTGVertexValue, Long> gellyGraph =
      Graph.fromDataSet(vertices, edges, env);
    DataSet<Vertex<Long, FlinkBTGVertexValue>> btgGraph =
      gellyGraph.run(new FlinkBTGAlgorithm(maxIteration)).getVertices();
    btgGraph.print();
  }
}
