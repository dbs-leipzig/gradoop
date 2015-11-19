package org.gradoop.model.impl.operators.logicalgraph.binary.equality;

import org.gradoop.model.impl.operators.EqualityTestBase;
import org.gradoop.model.impl.operators.logicalgraph.binary.equality
  .EqualByElementIds;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;

/**
 * Created by peet on 17.11.15.
 */
public class EqualByElementIdsTest extends EqualityTestBase {

  @Test
  public void testExecute(){

    // 4 graphs : 1-2 of same elements, 1-3 different vertex, 1-4 different edge
    String asciiGraphs =
      "g1[(a)-b->(c)];g2[(a)-b->(c)];g3[(d)-b->(c)];g4[(a)-e->(c)]";

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader();
    loader.readDatabaseFromString(asciiGraphs);

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g1 =
      loader.getLogicalGraphByVariable("g1");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g2 =
      loader.getLogicalGraphByVariable("g2");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g3 =
      loader.getLogicalGraphByVariable("g3");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g4 =
      loader.getLogicalGraphByVariable("g4");


    collectAndAssertEquals(new EqualByElementIds().execute(g1, g2));
    collectAndAssertNotEquals(new EqualByElementIds().execute(g1, g3));
    collectAndAssertNotEquals(new EqualByElementIds().execute(g1, g4));
  }
}