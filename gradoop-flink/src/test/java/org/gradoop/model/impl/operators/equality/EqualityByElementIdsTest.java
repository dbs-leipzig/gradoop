package org.gradoop.model.impl.operators.equality;

import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class EqualityByElementIdsTest extends EqualityTestBase {

  @Test
  public void testExecute(){

    // 4 graphs : 1-2 of same elements, 1-3 different vertex, 1-4 different edge
    String asciiGraphs = "" +
      "g1[(a)-[b]->(c)];" +
      "g2[(a)-[b]->(c)];" +
      "g3[(d)-[e]->(c)];" +
      "g4[(a)-[f]->(c)];" +
      "g5[(a)];" +
      "g6[(a)];" +
      "g7[(b)];";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g1 =
      loader.getLogicalGraphByVariable("g1");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g2 =
      loader.getLogicalGraphByVariable("g2");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g3 =
      loader.getLogicalGraphByVariable("g3");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g4 =
      loader.getLogicalGraphByVariable("g4");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g5 =
      loader.getLogicalGraphByVariable("g5");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g6 =
      loader.getLogicalGraphByVariable("g6");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g7 =
      loader.getLogicalGraphByVariable("g7");


    EqualityByElementIds<GraphHeadPojo, VertexPojo, EdgePojo> equals =
      new EqualityByElementIds<>();

    collectAndAssertEquals(equals.execute(g1, g1));
    collectAndAssertEquals(equals.execute(g1, g2));
    collectAndAssertNotEquals(equals.execute(g1, g3));
    collectAndAssertNotEquals(equals.execute(g1, g4));

    collectAndAssertEquals(equals.execute(g5, g5));
    collectAndAssertEquals(equals.execute(g5, g6));
    collectAndAssertNotEquals(equals.execute(g5, g7));

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> emptyGraph =
      LogicalGraph.createEmptyGraph(getConfig());

    collectAndAssertEquals(equals.execute(emptyGraph, emptyGraph));
    collectAndAssertNotEquals(equals.execute(g1, emptyGraph));
  }
}