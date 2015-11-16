package org.gradoop.model.impl.algorithms.epgmlabelpropagation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.algorithms.labelpropagation
  .LabelPropagationTestHelper;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests the algorithm using the Gelly Graph.run() method.
 */
@RunWith(Parameterized.class)
public class EPGMLabelPropagationGellyTest extends FlinkTestBase {
  public EPGMLabelPropagationGellyTest(TestExecutionMode mode) {
    super(mode);
  }

  /**
   * @return a connected graph where each vertex has its id as value
   */
  static String[] getConnectedGraphWithVertexValues() {
    return new String[] {
      "0 0 1 2 3", "1 1 0 2 3", "2 2 0 1 3 4", "3 3 0 1 2", "4 4 2 5 6 7",
      "5 5 4 6 7 8", "6 6 4 5 7", "7 7 4 5 6", "8 8 5 9 10 11", "9 9 8 10 11",
      "10 10 8 9 11", "11 11 8 9 10"
    };
  }

  /**
   * All vertices are connected.
   *
   * @return a complete bipartite graph where each vertex has its id as value
   */
  static String[] getCompleteBipartiteGraphWithVertexValue() {
    return new String[] {
      "0 0 4 5 6 7", "1 1 4 5 6 7", "2 2 4 5 6 7", "3 3 4 5 6 7", "4 4 0 1 2 3",
      "5 5 0 1 2 3", "6 6 0 1 2 3", "7 7 0 1 2 3"
    };
  }

  /**
   * @return a graph containing a loop with a vertex value
   */
  static String[] getLoopGraphWithVertexValues() {
    return new String[] {"0 0 1 2", "1 1 0 3", "2 1 0 3", "3 0 1 2"};
  }

  @Test
  public void testConnectedGraphWithVertexValues() throws Exception {
    int maxIteration = 100;
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    Graph<GradoopId, VertexPojo, EdgePojo> epGraph =
      LabelPropagationTestHelper
        .getEPGraph(getConnectedGraphWithVertexValues(), env);
    DataSet<Vertex<GradoopId, VertexPojo>> labeledGraph = epGraph.run(
      new EPGMLabelPropagationAlgorithm<VertexPojo, EdgePojo>(
        maxIteration)).getVertices();
    validateConnectedGraphResult(parseResult(labeledGraph.collect()));
  }

  @Test
  public void testBipartiteGraphWithVertexValues() throws Exception {
    int maxIteration = 100;
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    Graph<GradoopId, VertexPojo, EdgePojo> gellyGraph =
      LabelPropagationTestHelper
        .getEPGraph(getCompleteBipartiteGraphWithVertexValue(), env);
    DataSet<Vertex<GradoopId, VertexPojo>> labeledGraph = gellyGraph.run(
      new EPGMLabelPropagationAlgorithm<VertexPojo, EdgePojo>(
        maxIteration)).getVertices();
    validateCompleteBipartiteGraphResult(parseResult(labeledGraph.collect()));
  }

  @Test
  public void testLoopGraphWithVertexValues() throws Exception {
    int maxIteration = 100;
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    Graph<GradoopId, VertexPojo, EdgePojo> gellyGraph =
      LabelPropagationTestHelper
        .getEPGraph(getLoopGraphWithVertexValues(), env);
    DataSet<Vertex<GradoopId, VertexPojo>> labeledGraph = gellyGraph.run(
      new EPGMLabelPropagationAlgorithm<VertexPojo, EdgePojo>(
        maxIteration)).getVertices();
    validateLoopGraphResult(parseResult(labeledGraph.collect()));
  }

  private Map<GradoopId, GradoopId> parseResult(
    List<Vertex<GradoopId, VertexPojo>> graph) {
    Map<GradoopId, GradoopId> result = new HashMap<>();
    for (Vertex<GradoopId, VertexPojo> v : graph) {
      result.put(v.getId(), (GradoopId) v.getValue()
        .getProperty(EPGMLabelPropagationAlgorithm.CURRENT_VALUE));
    }
    return result;
  }

  private void validateConnectedGraphResult(Map<GradoopId, GradoopId> vertexIDWithValue) {
    assertEquals(12, vertexIDWithValue.size());
    assertEquals(GradoopId.fromLong(0L),
      vertexIDWithValue.get(GradoopId.fromLong(0L)));
    assertEquals(GradoopId.fromLong(0L),
      vertexIDWithValue.get(GradoopId.fromLong(1L)));
    assertEquals(GradoopId.fromLong(0L),
      vertexIDWithValue.get(GradoopId.fromLong(2L)));
    assertEquals(GradoopId.fromLong(0L),
      vertexIDWithValue.get(GradoopId.fromLong(3L)));
    assertEquals(GradoopId.fromLong(4L),
      vertexIDWithValue.get(GradoopId.fromLong(4L)));
    assertEquals(GradoopId.fromLong(4L),
      vertexIDWithValue.get(GradoopId.fromLong(5L)));
    assertEquals(GradoopId.fromLong(4L),
      vertexIDWithValue.get(GradoopId.fromLong(6L)));
    assertEquals(GradoopId.fromLong(4L),
      vertexIDWithValue.get(GradoopId.fromLong(7L)));
    assertEquals(GradoopId.fromLong(8L),
      vertexIDWithValue.get(GradoopId.fromLong(8L)));
    assertEquals(GradoopId.fromLong(8L),
      vertexIDWithValue.get(GradoopId.fromLong(9L)));
    assertEquals(GradoopId.fromLong(8L),
      vertexIDWithValue.get(GradoopId.fromLong(10L)));
    assertEquals(GradoopId.fromLong(8L),
      vertexIDWithValue.get(GradoopId.fromLong(11L)));
  }

  private void validateCompleteBipartiteGraphResult(
    Map<GradoopId, GradoopId> vertexIDWithValue) {
    assertEquals(8, vertexIDWithValue.size());
    assertEquals(GradoopId.fromLong(0L),
      vertexIDWithValue.get(GradoopId.fromLong(0L)));
    assertEquals(GradoopId.fromLong(0L),
      vertexIDWithValue.get(GradoopId.fromLong(1L)));
    assertEquals(GradoopId.fromLong(0L),
      vertexIDWithValue.get(GradoopId.fromLong(2L)));
    assertEquals(GradoopId.fromLong(0L),
      vertexIDWithValue.get(GradoopId.fromLong(3L)));
    assertEquals(GradoopId.fromLong(0L),
      vertexIDWithValue.get(GradoopId.fromLong(4L)));
    assertEquals(GradoopId.fromLong(0L),
      vertexIDWithValue.get(GradoopId.fromLong(5L)));
    assertEquals(GradoopId.fromLong(0L),
      vertexIDWithValue.get(GradoopId.fromLong(6L)));
    assertEquals(GradoopId.fromLong(0L),
      vertexIDWithValue.get(GradoopId.fromLong(7L)));
  }

  private void validateLoopGraphResult(Map<GradoopId, GradoopId> vertexIDWithValue) {
    assertEquals(4, vertexIDWithValue.size());
    assertEquals(GradoopId.fromLong(0L),
      vertexIDWithValue.get(GradoopId.fromLong(0L)));
    assertEquals(GradoopId.fromLong(0L),
      vertexIDWithValue.get(GradoopId.fromLong(1L)));
    assertEquals(GradoopId.fromLong(0L),
      vertexIDWithValue.get(GradoopId.fromLong(2L)));
    assertEquals(GradoopId.fromLong(0L),
      vertexIDWithValue.get(GradoopId.fromLong(3L)));
  }
}
