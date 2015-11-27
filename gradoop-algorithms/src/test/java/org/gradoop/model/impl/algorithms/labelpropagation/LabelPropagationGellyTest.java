package org.gradoop.model.impl.algorithms.labelpropagation;//package org.gradoop.model.impl.algorithms.labelpropagation;
//
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.graph.Graph;
//import org.apache.flink.graph.Vertex;
//import org.apache.flink.types.NullValue;
//import org.gradoop.model.FlinkTestBase;
//import org.gradoop.model.impl.algorithms.labelpropagation.pojos.LPVertexValue;
//import org.gradoop.model.impl.id.GradoopId;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.junit.runners.Parameterized;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import static org.junit.Assert.assertEquals;
//
///**
// * Tests the algorithm using the Gelly Graph.run() method.
// */
//@RunWith(Parameterized.class)
//public class LabelPropagationGellyTest extends FlinkTestBase {
//  public LabelPropagationGellyTest(TestExecutionMode mode) {
//    super(mode);
//  }
//
//  /**
//   * @return a connected graph where each vertex has its id as value
//   */
//  static String[] getConnectedGraphWithVertexValues() {
//    return new String[] {
//      "0 0 1 2 3", "1 1 0 2 3", "2 2 0 1 3 4", "3 3 0 1 2", "4 4 2 5 6 7",
//      "5 5 4 6 7 8", "6 6 4 5 7", "7 7 4 5 6", "8 8 5 9 10 11", "9 9 8 10 11",
//      "10 10 8 9 11", "11 11 8 9 10"
//    };
//  }
//
//  /**
//   * All vertices are connected.
//   *
//   * @return a complete bipartite graph where each vertex has its id as value
//   */
//  static String[] getCompleteBipartiteGraphWithVertexValue() {
//    return new String[] {
//      "0 0 4 5 6 7", "1 1 4 5 6 7", "2 2 4 5 6 7", "3 3 4 5 6 7", "4 4 0 1 2 3",
//      "5 5 0 1 2 3", "6 6 0 1 2 3", "7 7 0 1 2 3"
//    };
//  }
//
//  /**
//   * @return a graph containing a loop with a vertex value
//   */
//  static String[] getLoopGraphWithVertexValues() {
//    return new String[] {
//      "0 0 1 2", "1 1 0 3", "2 1 0 3", "3 0 1 2"
//    };
//  }
//
//  @Test
//  public void testConnectedGraphWithVertexValues() throws Exception {
//    int maxIteration = 100;
//    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//    Graph<GradoopId, LPVertexValue, NullValue> gellyGraph =
//      LabelPropagationTestHelper
//        .getGraph(getConnectedGraphWithVertexValues(), env);
//    DataSet<Vertex<GradoopId, LPVertexValue>> labeledGraph =
//      gellyGraph.run(new LabelPropagationAlgorithm(maxIteration)).getVertices();
//    validateConnectedGraphResult(parseResult(labeledGraph.collect()));
//  }
//
//  @Test
//  public void testBipartiteGraphWithVertexValues() throws Exception {
//    int maxIteration = 100;
//    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//    Graph<GradoopId, LPVertexValue, NullValue> gellyGraph =
//      LabelPropagationTestHelper
//        .getGraph(getCompleteBipartiteGraphWithVertexValue(), env);
//    DataSet<Vertex<GradoopId, LPVertexValue>> labeledGraph =
//      gellyGraph.run(new LabelPropagationAlgorithm(maxIteration)).getVertices();
//    validateCompleteBipartiteGraphResult(parseResult(labeledGraph.collect()));
//  }
//
//  @Test
//  public void testLoopGraphWithVertexValues() throws Exception {
//    int maxIteration = 100;
//    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//    Graph<GradoopId, LPVertexValue, NullValue> gellyGraph =
//      LabelPropagationTestHelper
//        .getGraph(getLoopGraphWithVertexValues(), env);
//    DataSet<Vertex<GradoopId, LPVertexValue>> labeledGraph =
//      gellyGraph.run(new LabelPropagationAlgorithm(maxIteration)).getVertices();
//    validateLoopGraphResult(parseResult(labeledGraph.collect()));
//  }
//
//  private Map<GradoopId, GradoopId> parseResult(
//    List<Vertex<GradoopId, LPVertexValue>> graph) {
//    Map<GradoopId, GradoopId> result = new HashMap<>();
//    for (Vertex<GradoopId, LPVertexValue> v : graph) {
//      result.put(v.getId(), v.getValue().getCurrentCommunity());
//    }
//    return result;
//  }
//
//  private void validateConnectedGraphResult(
//    Map<GradoopId, GradoopId> vertexIDwithValue) {
//    assertEquals(12, vertexIDwithValue.size());
//    assertEquals(GradoopId.fromLong(0L),
//      vertexIDwithValue.get(GradoopId.fromLong(0L)));
//    assertEquals(GradoopId.fromLong(0L),
//      vertexIDwithValue.get(GradoopId.fromLong(1L)));
//    assertEquals(GradoopId.fromLong(0L),
//      vertexIDwithValue.get(GradoopId.fromLong(2L)));
//    assertEquals(GradoopId.fromLong(0L),
//      vertexIDwithValue.get(GradoopId.fromLong(3L)));
//    assertEquals(GradoopId.fromLong(4L),
//      vertexIDwithValue.get(GradoopId.fromLong(4L)));
//    assertEquals(GradoopId.fromLong(4L),
//      vertexIDwithValue.get(GradoopId.fromLong(5L)));
//    assertEquals(GradoopId.fromLong(4L),
//      vertexIDwithValue.get(GradoopId.fromLong(6L)));
//    assertEquals(GradoopId.fromLong(4L),
//      vertexIDwithValue.get(GradoopId.fromLong(7L)));
//    assertEquals(GradoopId.fromLong(8L),
//      vertexIDwithValue.get(GradoopId.fromLong(8L)));
//    assertEquals(GradoopId.fromLong(8L),
//      vertexIDwithValue.get(GradoopId.fromLong(9L)));
//    assertEquals(GradoopId.fromLong(8L),
//      vertexIDwithValue.get(GradoopId.fromLong(10L)));
//    assertEquals(GradoopId.fromLong(8L),
//      vertexIDwithValue.get(GradoopId.fromLong(11L)));
//  }
//
//  private void validateCompleteBipartiteGraphResult(
//    Map<GradoopId, GradoopId> vertexIDwithValue) {
//    assertEquals(8, vertexIDwithValue.size());
//    assertEquals(GradoopId.fromLong(0L),
//      vertexIDwithValue.get(GradoopId.fromLong(0L)));
//    assertEquals(GradoopId.fromLong(0L),
//      vertexIDwithValue.get(GradoopId.fromLong(1L)));
//    assertEquals(GradoopId.fromLong(0L),
//      vertexIDwithValue.get(GradoopId.fromLong(2L)));
//    assertEquals(GradoopId.fromLong(0L),
//      vertexIDwithValue.get(GradoopId.fromLong(3L)));
//    assertEquals(GradoopId.fromLong(0L),
//      vertexIDwithValue.get(GradoopId.fromLong(4L)));
//    assertEquals(GradoopId.fromLong(0L),
//      vertexIDwithValue.get(GradoopId.fromLong(5L)));
//    assertEquals(GradoopId.fromLong(0L),
//      vertexIDwithValue.get(GradoopId.fromLong(6L)));
//    assertEquals(GradoopId.fromLong(0L),
//      vertexIDwithValue.get(GradoopId.fromLong(7L)));
//  }
//
//  private void validateLoopGraphResult(
//    Map<GradoopId, GradoopId> vertexIDwithValue) {
//    assertEquals(4, vertexIDwithValue.size());
//    assertEquals(GradoopId.fromLong(0L),
//      vertexIDwithValue.get(GradoopId.fromLong(0L)));
//    assertEquals(GradoopId.fromLong(0L),
//      vertexIDwithValue.get(GradoopId.fromLong(1L)));
//    assertEquals(GradoopId.fromLong(0L),
//      vertexIDwithValue.get(GradoopId.fromLong(2L)));
//    assertEquals(GradoopId.fromLong(0L),
//      vertexIDwithValue.get(GradoopId.fromLong(3L)));
//  }
//}
