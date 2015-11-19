package org.gradoop.model.impl.operators.collection.binary.equality;

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
  public void testExecute() throws Exception {

    String asciiGraphs = "g1[(a)-b->(c)];g2[(a)-b->(c)]";

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader();
    loader.readGraphs(asciiGraphs);

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g1 =
      loader.getLogicalGraphByVariable("g1");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g2 =
      loader.getLogicalGraphByVariable("g2");

    collectAndCheckAssertions(new EqualByElementIds().execute(g1, g2));
  }
}