package org.gradoop.flink.model.impl.layouts.gve;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class GVELayoutTest extends GradoopFlinkTestBase {

  private static GraphHead g0;
  private static GraphHead g1;

  private static Vertex v0;
  private static Vertex v1;
  private static Vertex v2;

  private static Edge e0;
  private static Edge e1;

  @BeforeClass
  public static void setup() {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    g0 = config.getGraphHeadFactory().createGraphHead("A");
    g1 = config.getGraphHeadFactory().createGraphHead("B");

    v0 = config.getVertexFactory().createVertex("A");
    v1 = config.getVertexFactory().createVertex("B");
    v2 = config.getVertexFactory().createVertex("C");

    e0 = config.getEdgeFactory().createEdge("a", v0.getId(), v1.getId());
    e1 = config.getEdgeFactory().createEdge("b", v1.getId(), v2.getId());

    v0.addGraphId(g0.getId());
    v1.addGraphId(g0.getId());
    v1.addGraphId(g1.getId());
    v2.addGraphId(g1.getId());

    e0.addGraphId(g0.getId());
    e1.addGraphId(g1.getId());
  }

  @Test
  public void hasTransactionalLayout() throws Exception {
    GVELayout layout = new GVELayout(
      getExecutionEnvironment().fromElements(g0, g1),
      getExecutionEnvironment().fromElements(v0, v1, v2),
      getExecutionEnvironment().fromElements(e0, e1));

    assertFalse(layout.hasTransactionalLayout());
  }

  @Test
  public void hasGVELayout() throws Exception {
    GVELayout layout = new GVELayout(
      getExecutionEnvironment().fromElements(g0, g1),
      getExecutionEnvironment().fromElements(v0, v1, v2),
      getExecutionEnvironment().fromElements(e0, e1));

    assertTrue(layout.hasGVELayout());
  }

  @Test
  public void getGraphTransactions() throws Exception {
    GraphTransaction tx0 = new GraphTransaction(g0, Sets.newHashSet(v0, v1), Sets.newHashSet(e0));

    GVELayout layout = new GVELayout(
      getExecutionEnvironment().fromElements(g0),
      getExecutionEnvironment().fromElements(v0, v1),
      getExecutionEnvironment().fromElements(e0));

    assertEquals(tx0, layout.getGraphTransactions().collect().get(0));
  }

  @Test
  public void getGraphHead() throws Exception {
    GVELayout layout = new GVELayout(
      getExecutionEnvironment().fromElements(g0),
      getExecutionEnvironment().fromElements(v0, v1),
      getExecutionEnvironment().fromElements(e0));

    GradoopTestUtils.validateEPGMElementCollections(Sets.newHashSet(g0),
      layout.getGraphHead().collect());
  }

  @Test
  public void getGraphHeads() throws Exception {
    GVELayout layout = new GVELayout(
      getExecutionEnvironment().fromElements(g0, g1),
      getExecutionEnvironment().fromElements(v0, v1, v2),
      getExecutionEnvironment().fromElements(e0, e1));

    GradoopTestUtils.validateEPGMElementCollections(Sets.newHashSet(g0, g1),
      layout.getGraphHeads().collect());
  }

  @Test
  public void getGraphHeadsByLabel() throws Exception {
    GVELayout layout = new GVELayout(
      getExecutionEnvironment().fromElements(g0, g1),
      getExecutionEnvironment().fromElements(v0, v1, v2),
      getExecutionEnvironment().fromElements(e0, e1));

    GradoopTestUtils.validateEPGMElementCollections(Sets.newHashSet(g0),
      layout.getGraphHeadsByLabel("A").collect());
  }

  @Test
  public void getVertices() throws Exception {
    GVELayout layout = new GVELayout(
      getExecutionEnvironment().fromElements(g0, g1),
      getExecutionEnvironment().fromElements(v0, v1, v2),
      getExecutionEnvironment().fromElements(e0, e1));

    GradoopTestUtils.validateEPGMGraphElementCollections(Sets.newHashSet(v0, v1, v2),
      layout.getVertices().collect());
  }

  @Test
  public void getVerticesByLabel() throws Exception {
    GVELayout layout = new GVELayout(
      getExecutionEnvironment().fromElements(g0, g1),
      getExecutionEnvironment().fromElements(v0, v1, v2),
      getExecutionEnvironment().fromElements(e0, e1));

    GradoopTestUtils.validateEPGMGraphElementCollections(Sets.newHashSet(v0),
      layout.getVerticesByLabel("A").collect());
  }

  @Test
  public void getEdges() throws Exception {
    GVELayout layout = new GVELayout(
      getExecutionEnvironment().fromElements(g0, g1),
      getExecutionEnvironment().fromElements(v0, v1, v2),
      getExecutionEnvironment().fromElements(e0, e1));

    GradoopTestUtils.validateEPGMGraphElementCollections(Sets.newHashSet(e0, e1),
      layout.getEdges().collect());
  }

  @Test
  public void getEdgesByLabel() throws Exception {
    GVELayout layout = new GVELayout(
      getExecutionEnvironment().fromElements(g0, g1),
      getExecutionEnvironment().fromElements(v0, v1, v2),
      getExecutionEnvironment().fromElements(e0, e1));

    GradoopTestUtils.validateEPGMGraphElementCollections(Sets.newHashSet(e0),
      layout.getEdgesByLabel("a").collect());
  }

  @Test
  public void getOutgoingEdges() throws Exception {
    GVELayout layout = new GVELayout(
      getExecutionEnvironment().fromElements(g0, g1),
      getExecutionEnvironment().fromElements(v0, v1, v2),
      getExecutionEnvironment().fromElements(e0, e1));

    GradoopTestUtils.validateEPGMGraphElementCollections(Sets.newHashSet(e0),
      layout.getOutgoingEdges(v0.getId()).collect());
  }

  @Test
  public void getIncomingEdges() throws Exception {
    GVELayout layout = new GVELayout(
      getExecutionEnvironment().fromElements(g0, g1),
      getExecutionEnvironment().fromElements(v0, v1, v2),
      getExecutionEnvironment().fromElements(e0, e1));

    GradoopTestUtils.validateEPGMGraphElementCollections(Sets.newHashSet(e0),
      layout.getIncomingEdges(v1.getId()).collect());
  }
}