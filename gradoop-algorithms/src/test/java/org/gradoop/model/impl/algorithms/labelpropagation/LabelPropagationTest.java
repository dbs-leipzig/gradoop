package org.gradoop.model.impl.algorithms.labelpropagation;


import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class LabelPropagationTest extends GradoopFlinkTestBase {

  @Test
  public void testDisconnectedGraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("" +
        "input[" +
        "(v0)-[e0]->(v1);" +
        "(v2)-[e1]->(v3);" +
        "]" +
        "c1[" +
        "(v0)-[e0]->(v1);" +
        "]" +
        "c2[" +
        "(v2)-[e1]->(v3);" +
        "]");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> outputCollection =
      loader.getLogicalGraphByVariable("input").callForCollection(
        new LabelPropagation<GraphHeadPojo, VertexPojo, EdgePojo>(10));

    collectAndAssertTrue(outputCollection.equalsByGraphElementIds(
      loader.getGraphCollectionByVariables("c1", "c2")));
  }
}
