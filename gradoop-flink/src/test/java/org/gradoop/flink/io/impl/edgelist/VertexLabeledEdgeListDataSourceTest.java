
package org.gradoop.flink.io.impl.edgelist;

import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class VertexLabeledEdgeListDataSourceTest extends GradoopFlinkTestBase {

  @Test
  public void testRead() throws Exception {
    String edgeListFile = VertexLabeledEdgeListDataSourceTest.class
      .getResource("/data/edgelist/vertexlabeled/input").getFile();
    String gdlFile = VertexLabeledEdgeListDataSourceTest.class
      .getResource("/data/edgelist/vertexlabeled/expected.gdl").getFile();

    DataSource dataSource = new VertexLabeledEdgeListDataSource(edgeListFile, " ", "lan", config);
    LogicalGraph result = dataSource.getLogicalGraph();
    FlinkAsciiGraphLoader loader = getLoaderFromFile(gdlFile);
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    collectAndAssertTrue(expected.equalsByElementData(result));
  }
}
