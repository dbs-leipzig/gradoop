package org.gradoop.model.impl.algorithms.communitydetection;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class CommunityDetectionTest extends GradoopFlinkTestBase {

  @Test
  public void testByElementData() throws Exception {
    String gdlFile = CommunityDetectionTest.class.getResource
      ("/data/gdl/cd.gdl").getFile();


    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromFile(gdlFile);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> outputGraph =
      loader.getLogicalGraphByVariable("input").callForGraph(
        new CommunityDetection<GraphHeadPojo, VertexPojo, EdgePojo>(
          10, 0.5, "value"));

    collectAndAssertTrue(outputGraph.equalsByElementData(
      loader.getLogicalGraphByVariable("result")));
  }

}
