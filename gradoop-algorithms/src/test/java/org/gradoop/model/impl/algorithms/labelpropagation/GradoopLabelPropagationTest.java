package org.gradoop.model.impl.algorithms.labelpropagation;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class GradoopLabelPropagationTest extends GradoopFlinkTestBase {
  /**
   * Tests if the resulting graph contains vertices and edges with the same
   * associated data (i.e. the label propagation value).
   *
   * @throws Exception
   */
  @Test
  public void testByElementData() throws Exception {

    String gdlFile = GradoopLabelPropagationTest.class.getResource
      ("/data/gdl/lp1.gdl").getFile();

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromFile(gdlFile);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> outputGraph =
      loader.getLogicalGraphByVariable("input").callForGraph(
        new GradoopLabelPropagation<GraphHeadPojo, VertexPojo, EdgePojo>(
          10, "value"));

    collectAndAssertTrue(outputGraph.equalsByElementData(
      loader.getLogicalGraphByVariable("result")));
  }

  /**
   * Tests, if the resulting graph contains the same elements as the input
   * graph.
   *
   * @throws Exception
   */
  @Test
  public void testByElementIds() throws Exception {

    String gdlFile = GradoopLabelPropagationTest.class.getResource
      ("/data/gdl/lp2.gdl").getFile();

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromFile(gdlFile);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> outputGraph =
      loader.getLogicalGraphByVariable("input").callForGraph(
        new GradoopLabelPropagation<GraphHeadPojo, VertexPojo, EdgePojo>(10,
          "value"));

    collectAndAssertTrue(outputGraph.equalsByElementIds(
      loader.getLogicalGraphByVariable("result")));
  }
}
