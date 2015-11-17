package org.gradoop.model.impl.operators.collection.binary.equality;

import junit.framework.TestCase;
import org.gradoop.grala.api.AsciiGraphReader;
import org.gradoop.grala.impl.MockGraphReader;
import org.gradoop.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;

/**
 * Created by peet on 17.11.15.
 */
public class EqualByElementIdsTest extends TestCase {

  public void testExecute() throws Exception {

    String asciiGraphs = "g1[(a)-b->(c)];g2[(a)-b->(c)]";

    AsciiGraphReader reader = new MockGraphReader();
    reader.read(asciiGraphs);



    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g1 =
      helper.getLogicalGraphByVariable("g1");

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> g2 =
      helper.getLogicalGraphByVariable("g1");

    g1.callForGraph()

  }
}