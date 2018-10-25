package org.gradoop.flink.model.impl.operators.sampling.statistics;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.statistics.DegreeCentrality;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test class for degree centrality
 */
public class DegreeCentralityTest extends GradoopFlinkTestBase {

  /**
   * graph as String for testing
   * g0: a star graph
   * g1: a path
   * g2: a community graph
   */
  private String graphString =
    "g0[" +
      "(v0)-[]->(v1)"+
      "(v0)-[]->(v2)"+
      "(v0)-[]->(v3)"+
      "(v0)-[]->(v4)"+
      "(v0)-[]->(v5)"+
    "]" +
    "g1[" +
      "(v0)-[]->(v1)"+
      "(v1)-[]->(v2)"+
      "(v2)-[]->(v3)"+
      "(v3)-[]->(v4)"+
    "]" +
    "g2[" +
      "(v0)-[]->(v1)"+
      "(v1)-[]->(v2)"+
      "(v1)-[]->(v3)"+
      "(v2)-[]->(v3)"+
      "(v0)-[]->(v4)"+
      "(v4)-[]->(v5)"+
      "(v4)-[]->(v6)"+
      "(v5)-[]->(v6)"+
    "]";

  /**
   * test star graph for degree centrality
   * @throws Exception
   */
  @Test
  public void testStar() throws Exception {
    LogicalGraph graph = getLoaderFromString(graphString).getLogicalGraphByVariable("g0");
    DataSet<Double> dataSet = new DegreeCentrality().execute(graph);
    assertEquals(1.0, dataSet.collect().get(0).doubleValue(), 0.001);
  }

  /**
   * test star graph for degree centrality
   * @throws Exception
   */
  @Test
  public void testPath() throws Exception {
    LogicalGraph graph = getLoaderFromString(graphString).getLogicalGraphByVariable("g1");
    DataSet<Double> dataSet = new DegreeCentrality().execute(graph);
    assertEquals(0.167, dataSet.collect().get(0).doubleValue(), 0.001);
  }

  /**
   * test star graph for degree centrality
   * @throws Exception
   */
  @Test
  public void testCommunity() throws Exception {
    LogicalGraph graph = getLoaderFromString(graphString).getLogicalGraphByVariable("g2");
    DataSet<Double> dataSet = new DegreeCentrality().execute(graph);
    assertEquals(0.167, dataSet.collect().get(0).doubleValue(), 0.001);
  }

}
