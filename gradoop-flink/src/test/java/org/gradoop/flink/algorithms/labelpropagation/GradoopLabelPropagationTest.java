package org.gradoop.flink.algorithms.labelpropagation;

import org.gradoop.flink.algorithms.labelpropagation.GradoopLabelPropagation;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class GradoopLabelPropagationTest extends GradoopFlinkTestBase {
  /**
   * Tests if the resulting graph contains vertices and edges with the same
   * associated data (i.e. the label propagation value).
   *
   * @throws Exception
   */
  @Test
  public void testByElementData() throws Exception {
    String graph = "input[" +
      "/* first community */" +
      "(v0 {id=0, value=\"A\"})" +
      "(v1 {id=1, value=\"A\"})" +
      "(v2 {id=2, value=\"B\"})" +
      "(v0)-[e0]->(v1);" +
      "(v1)-[e1]->(v2);" +
      "(v2)-[e2]->(v0);" +
      "/* second community */" +
      "(v3 {id=3, value=\"C\"})" +
      "(v4 {id=4, value=\"D\"})" +
      "(v5 {id=5, value=\"E\"})" +
      "(v6 {id=6, value=\"F\"})" +
      "(v3)-[e3]->(v1);" +
      "(v3)-[e4]->(v4);" +
      "(v3)-[e5]->(v5);" +
      "(v3)-[e6]->(v6);" +
      "(v4)-[e7]->(v3);" +
      "(v4)-[e8]->(v5);" +
      "(v4)-[e9]->(v6);" +
      "(v5)-[e10]->(v3);" +
      "(v5)-[e11]->(v4);" +
      "(v5)-[e12]->(v6);" +
      "(v6)-[e13]->(v3);" +
      "(v6)-[e14]->(v4);" +
      "(v6)-[e15]->(v5);" +
      "]" +
      "result[" +
      "(v7 {id=0, value=\"A\"})" +
      "(v8 {id=1, value=\"A\"})" +
      "(v9 {id=2, value=\"A\"})" +
      "(v7)-[e16]->(v8);" +
      "(v8)-[e17]->(v9);" +
      "(v9)-[e18]->(v7);" +
      "(v10 {id=3, value=\"C\"})" +
      "(v11 {id=4, value=\"C\"})" +
      "(v12 {id=5, value=\"C\"})" +
      "(v13 {id=6, value=\"C\"})" +
      "(v10)-[e19]->(v8);" +
      "(v10)-[e20]->(v11);" +
      "(v10)-[e21]->(v12);" +
      "(v10)-[e22]->(v13);" +
      "(v11)-[e23]->(v10);" +
      "(v11)-[e24]->(v12);" +
      "(v11)-[e25]->(v13);" +
      "(v12)-[e26]->(v10);" +
      "(v12)-[e27]->(v11);" +
      "(v12)-[e28]->(v13);" +
      "(v13)-[e29]->(v10);" +
      "(v13)-[e30]->(v11);" +
      "(v13)-[e31]->(v12);" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(graph);

    LogicalGraph outputGraph = loader.getLogicalGraphByVariable("input")
      .callForGraph(new GradoopLabelPropagation(10, "value"));

    collectAndAssertTrue(outputGraph.equalsByElementData(
      loader.getLogicalGraphByVariable("result")));
  }

  /**
   * Tests, if the resulting graph contains the same elements as the input
   * graph.
   *
   * @throws Exception
   */
  @Test
  public void testByElementIds() throws Exception {
    String graph = "input[" +
      "/* first community */" +
      "(v0 {id=0, value=\"A\"})" +
      "(v1 {id=1, value=\"A\"})" +
      "(v2 {id=2, value=\"B\"})" +
      "(v0)-[e0]->(v1);" +
      "(v1)-[e1]->(v2);" +
      "(v2)-[e2]->(v0);" +
      "/* second community */" +
      "(v3 {id=3, value=\"C\"})" +
      "(v4 {id=4, value=\"D\"})" +
      "(v5 {id=5, value=\"E\"})" +
      "(v6 {id=6, value=\"F\"})" +
      "(v3)-[e3]->(v1);" +
      "(v3)-[e4]->(v4);" +
      "(v3)-[e5]->(v5);" +
      "(v3)-[e6]->(v6);" +
      "(v4)-[e7]->(v3);" +
      "(v4)-[e8]->(v5);" +
      "(v4)-[e9]->(v6);" +
      "(v5)-[e10]->(v3);" +
      "(v5)-[e11]->(v4);" +
      "(v5)-[e12]->(v6);" +
      "(v6)-[e13]->(v3);" +
      "(v6)-[e14]->(v4);" +
      "(v6)-[e15]->(v5);" +
      "]" +
      "result[" +
      "(v0)-[e0]->(v1);" +
      "(v1)-[e1]->(v2);" +
      "(v2)-[e2]->(v0);" +
      "(v3)-[e3]->(v1);" +
      "(v3)-[e4]->(v4);" +
      "(v3)-[e5]->(v5);" +
      "(v3)-[e6]->(v6);" +
      "(v4)-[e7]->(v3);" +
      "(v4)-[e8]->(v5);" +
      "(v4)-[e9]->(v6);" +
      "(v5)-[e10]->(v3);" +
      "(v5)-[e11]->(v4);" +
      "(v5)-[e12]->(v6);" +
      "(v6)-[e13]->(v3);" +
      "(v6)-[e14]->(v4);" +
      "(v6)-[e15]->(v5);" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(graph);

    LogicalGraph outputGraph = loader.getLogicalGraphByVariable("input")
      .callForGraph(new GradoopLabelPropagation(10, "value"));

    collectAndAssertTrue(outputGraph.equalsByElementIds(
      loader.getLogicalGraphByVariable("result")));
  }
}
