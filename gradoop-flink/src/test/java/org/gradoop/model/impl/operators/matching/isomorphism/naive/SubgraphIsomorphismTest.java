package org.gradoop.model.impl.operators.matching.isomorphism.naive;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.matching.TestData;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class SubgraphIsomorphismTest extends GradoopFlinkTestBase {

  @Test
  public void testGraph1CyclePattern2() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TestData.GRAPH_1);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> db =
      loader.getLogicalGraphByVariable("db");

    // pattern
    String query = TestData.CYCLE_PATTERN_2;

    loader.appendToDatabaseFromString("expected1[" +
      "(v2)-[e3]->(v6)" +
      "(v6)-[e7]->(v2)" +
      "(v6)-[e9]->(v7)" +
      "]");

    SubgraphIsomorphism<GraphHeadPojo, VertexPojo, EdgePojo> op =
      new SubgraphIsomorphism<>(query, false);

    // execute and validate
    collectAndAssertTrue(op.execute(db).equalsByGraphElementIds(
      loader.getGraphCollectionByVariables("expected1")));
  }

  @Test
  public void testGraph2CyclePattern5() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TestData.GRAPH_2);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> db =
      loader.getLogicalGraphByVariable("db");

    // pattern
    String query = TestData.CYCLE_PATTERN_5;

    loader.appendToDatabaseFromString("" +
      "expected1[" +
      "(v0)-[e1]->(v4)<-[e2]-(v0)" +
      "]" +
      "expected2[" +
      "(v5)-[e9]->(v4)<-[e10]-(v5)" +
      "]");

    SubgraphIsomorphism<GraphHeadPojo, VertexPojo, EdgePojo> op =
      new SubgraphIsomorphism<>(query, false);

    // execute and validate
    collectAndAssertTrue(op.execute(db).equalsByGraphElementIds(
      loader.getGraphCollectionByVariables("expected1", "expected2")));
  }
}