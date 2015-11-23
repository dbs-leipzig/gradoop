package org.gradoop.model.impl.operators.equality;

import org.gradoop.model.impl.operators.EqualityTestBase;
import org.gradoop.model.impl.operators.equality.logicalgraph.EqualByElementIds;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class EqualByElementIdsTest extends EqualityTestBase {

  public EqualByElementIdsTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testExecute(){

    // 4 graphs : 1-2 of same elements, 1-3 different vertex, 1-4 different edge
    String asciiGraphs =
      "g1[(a)-[b]->(c)];g2[(a)-[b]->(c)];g3[(d)-[b]->(c)];g4[(a)-[e]->(c)]";

    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      getLoaderFromString(asciiGraphs);

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g1 =
      loader.getLogicalGraphByVariable("g1");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g2 =
      loader.getLogicalGraphByVariable("g2");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g3 =
      loader.getLogicalGraphByVariable("g3");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g4 =
      loader.getLogicalGraphByVariable("g4");


    EqualByElementIds<GraphHeadPojo, VertexPojo, EdgePojo> equals =
      new EqualByElementIds<>();

    collectAndAssertEquals(equals.execute(g1, g2));
    collectAndAssertNotEquals(equals.execute(g1, g3));
    collectAndAssertNotEquals(equals.execute(g1, g4));
  }
}