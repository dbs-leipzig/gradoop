package org.gradoop.model.impl.operators.matching.simulation.dual;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.matching.TestData;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;

public class DualSimulationTest extends GradoopFlinkTestBase {

  @Test
  public void testGraph1PathPattern0() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TestData.GRAPH_1);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> db =
      loader.getLogicalGraphByVariable("db");

    // pattern
    String query = TestData.PATH_PATTERN_0;
    printPattern(query);

    loader.appendToDatabaseFromString("expected[" +
      "(v1)-[e2]->(v6)" +
      "(v5)-[e6]->(v4)" +
      "(v2)-[e3]->(v6)" +
      "]");

    // create operator
    DualSimulation<GraphHeadPojo, VertexPojo, EdgePojo> op =
      new DualSimulation<>(query);

    // execute and validate
    collectAndAssertTrue(op.execute(db)
      .equalsByElementIds(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testGraph3PathPattern1() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TestData.GRAPH_3);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> db =
      loader.getLogicalGraphByVariable("db");

    // pattern
    String query = TestData.PATH_PATTERN_1;
    printPattern(query);

    loader.appendToDatabaseFromString("expected[" +
      "(v0)-[e0]->(v1)" +
      "(v1)-[e1]->(v2)" +
      "(v2)-[e2]->(v3)" +
      "]");

    // create operator
    DualSimulation<GraphHeadPojo, VertexPojo, EdgePojo> op =
      new DualSimulation<>(query);

    // execute and validate
    collectAndAssertTrue(op.execute(db)
      .equalsByElementIds(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testGraph2LoopPattern0() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TestData.GRAPH_2);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> db =
      loader.getLogicalGraphByVariable("db");

    // pattern
    String query = TestData.LOOP_PATTERN_0;
    printPattern(query);

    loader.appendToDatabaseFromString("expected[" +
      "(v9)-[e15]->(v9)" +
      "]");

    // create operator
    DualSimulation<GraphHeadPojo, VertexPojo, EdgePojo> op =
      new DualSimulation<>(query);

    // execute and validate
    collectAndAssertTrue(op.execute(db)
      .equalsByElementIds(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testGraph1CyclePattern0() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TestData.GRAPH_1);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> db =
      loader.getLogicalGraphByVariable("db");

    // pattern
    String query = TestData.CYCLE_PATTERN_0;
    printPattern(query);

    loader.appendToDatabaseFromString("expected[" +
      "(v1)-[e2]->(v6)" +
      "(v6)-[e8]->(v5)" +
      "(v5)-[e6]->(v4)" +
      "(v4)-[e4]->(v1)" +
      "(v6)-[e7]->(v2)" +
      "(v2)-[e3]->(v6)" +
      "]");

    // create operator
    DualSimulation<GraphHeadPojo, VertexPojo, EdgePojo> op =
      new DualSimulation<>(query);

    // execute and validate
    collectAndAssertTrue(op.execute(db)
      .equalsByElementIds(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testGraph2CyclePattern1() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TestData.GRAPH_2);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> db =
      loader.getLogicalGraphByVariable("db");

    // pattern
    String query = TestData.CYCLE_PATTERN_1;
    printPattern(query);

    loader.appendToDatabaseFromString("expected[" +
      "(v9)-[e15]->(v9)" +
      "]");

    // create operator
    DualSimulation<GraphHeadPojo, VertexPojo, EdgePojo> op =
      new DualSimulation<>(query);

    // execute and validate
    collectAndAssertTrue(op.execute(db)
      .equalsByElementIds(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testGraph1CyclePattern2() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TestData.GRAPH_1);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> db =
      loader.getLogicalGraphByVariable("db");

    // pattern
    String query = TestData.CYCLE_PATTERN_2;
    printPattern(query);

    loader.appendToDatabaseFromString("expected[" +
      "(v1)-[e2]->(v6)" +
      "(v2)-[e3]->(v6)" +
      "(v4)-[e4]->(v1)" +
      "(v4)-[e5]->(v3)" +
      "(v5)-[e6]->(v4)" +
      "(v6)-[e7]->(v2)" +
      "(v6)-[e8]->(v5)" +
      "(v5)-[e9]->(v7)" +
      "]");

    // create operator
    DualSimulation<GraphHeadPojo, VertexPojo, EdgePojo> op =
      new DualSimulation<>(query);

    // execute and validate
    collectAndAssertTrue(op.execute(db)
      .equalsByElementIds(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testGraph2CyclePattern3() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TestData.GRAPH_2);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> db =
      loader.getLogicalGraphByVariable("db");

    // pattern
    String query = TestData.CYCLE_PATTERN_3;
    printPattern(query);

    // expected result
    loader.appendToDatabaseFromString("expected[" +
      "(v1)-[e0]->(v0)" +
      "(v0)-[e1]->(v4)" +
      "(v0)-[e2]->(v4)" +
      "(v0)-[e3]->(v3)" +
      "(v3)-[e4]->(v5)" +
      "(v5)-[e5]->(v1)" +
      "(v1)-[e6]->(v6)" +
      "(v6)-[e7]->(v2)" +
      "(v2)-[e8]->(v6)" +
      "(v5)-[e9]->(v4)" +
      "(v5)-[e10]->(v4)" +
      "(v6)-[e11]->(v7)" +
      "(v8)-[e12]->(v7)" +
      "(v10)-[e13]->(v5)" +
      "(v6)-[e14]->(v10)" +
      "]");

    // create operator
    DualSimulation<GraphHeadPojo, VertexPojo, EdgePojo> op =
      new DualSimulation<>(query);

    // execute and validate
    collectAndAssertTrue(op.execute(db)
      .equalsByElementIds(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testGraph1CyclePattern4() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TestData.GRAPH_1);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> db =
      loader.getLogicalGraphByVariable("db");

    // pattern
    String query = TestData.CYCLE_PATTERN_4;
    printPattern(query);

    loader.appendToDatabaseFromString("expected[" +
      "(v1)-[e2]->(v6)" +
      "(v6)-[e8]->(v5)" +
      "(v5)-[e6]->(v4)" +
      "(v4)-[e4]->(v1)" +
      "(v6)-[e7]->(v2)" +
      "(v2)-[e3]->(v6)" +
      "]");

    // create operator
    DualSimulation<GraphHeadPojo, VertexPojo, EdgePojo> op =
      new DualSimulation<>(query);

    // execute and validate
    collectAndAssertTrue(op.execute(db)
      .equalsByElementIds(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testGraph1TreePattern0() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TestData.GRAPH_1);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> db =
      loader.getLogicalGraphByVariable("db");

    // pattern
    String query = TestData.TREE_PATTERN_0;
    printPattern(query);

    loader.appendToDatabaseFromString("expected[" +
      "]");

    // create operator
    DualSimulation<GraphHeadPojo, VertexPojo, EdgePojo> op =
      new DualSimulation<>(query);

    // execute and validate
    collectAndAssertTrue(op.execute(db)
      .equalsByElementIds(loader.getLogicalGraphByVariable("expected")));
  }

  private void printPattern(String query) {
    GDLHandler gdlHandler = new GDLHandler.Builder().buildFromString(query);
    System.out.println("Pattern:");
    for (Vertex vertex : gdlHandler.getVertices()) {
      System.out.println(vertex);
    }
    for (Edge edge : gdlHandler.getEdges()) {
      System.out.println(edge);
    }
  }
}
