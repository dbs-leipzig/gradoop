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
  private static String[] getConnectedIIG() {
    return new String[] {
      "0,1 0,1 4 9 10", "1,1 1,0 5 6 11 12", "2,1 2,8 13", "3,1 3,7 14 15",
      "4,0 4,0 5", "5,0 5,1 4 6", "6,0 6,1 5 7 8", "7,0 7,3 6", "8,0 8,2 6",
      "9,0 9,0 10", "10,0 10,0 9 11 12", "11,0 11,1 10 13 14",
      "12,0 12,1 10 15", "13,0 13,2 11", "14,0 14,3 11", "15,0 15,3 12"
    };
  }

  static String[] getDisconnectedIIG() {
    return new String[] {
      "0,1 0,6 7", "1,1 1,2 7", "2,1 2,1 8 9", "3,1 3,4 10", "4,1 4,3 5 11 12",
      "5,1 5,4 12 13", "6,0 6,0 7", "7,0 7,0 1 6 8", "8,0 8,2 7 9", "9,0 9,2 8",
      "10,0 10,3 11 12", "11,0 11,4 10", "12,0 12,4 5 10 13", "13,0 13,5"
    };
  }

  static String[] getDisconnectedIIGWithBTGIDs() {
    return new String[] {
      "0,1 0,6 7", "1,1 1,2 7", "2,1 2,1 8 9", "3,1 3,4 10", "4,1 4,3 5 11 12",
      "5,1 5,4 12 13", "6,0 6,0 7", "7,0 7,0 1 6 8", "8,0 8,2 7 9", "9,0 9,2 8",
      "10,0 10,3 11 12", "11,0 11,4 10", "12,0 12,4 5 10 13", "13,0 13,5"
    };
  }

  @Test
  public void testConnectedIIG() throws Exception {
    int maxIteration = 100;
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<Vertex<Long, FlinkBTGVertexValue>> vertices =
      FlinkBTGAlgorithmTestHelper
        .getConnectedIIGVertices(getConnectedIIG(), env);
    DataSet<Edge<Long, Long>> edges =
      FlinkBTGAlgorithmTestHelper.getConnectedIIGEdges(getConnectedIIG(), env);
    Graph<Long, FlinkBTGVertexValue, Long> gellyGraph =
      Graph.fromDataSet(vertices, edges, env);
    DataSet<Vertex<Long, FlinkBTGVertexValue>> btgGraph =
      gellyGraph.run(new FlinkBTGAlgorithm(maxIteration)).getVertices();
    btgGraph.print();
  }

  @Test
  public void testDisconnectedIIG() throws Exception {
    int maxIteration = 100;
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<Vertex<Long, FlinkBTGVertexValue>> vertices =
      FlinkBTGAlgorithmTestHelper
        .getConnectedIIGVertices(getDisconnectedIIG(), env);
    DataSet<Edge<Long, Long>> edges =
      FlinkBTGAlgorithmTestHelper.getConnectedIIGEdges(getDisconnectedIIG(), env);
    Graph<Long, FlinkBTGVertexValue, Long> gellyGraph =
      Graph.fromDataSet(vertices, edges, env);
    DataSet<Vertex<Long, FlinkBTGVertexValue>> btgGraph =
      gellyGraph.run(new FlinkBTGAlgorithm(maxIteration)).getVertices();
    btgGraph.print();
  }
}
