package org.gradoop.flink.io.impl.csv;

import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.edgelist.VertexLabeledEdgeListDataSourceTest;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.junit.Test;

import static org.gradoop.flink.model.impl.GradoopFlinkTestUtils.printLogicalGraph;

public class CSVDataSourceTest extends GradoopFlinkTestBase {
  @Test
  public void testRead() throws Exception {
    String csvPath = VertexLabeledEdgeListDataSourceTest.class
      .getResource("/data/csv/input")
      .getPath();

    String gdlPath = CSVDataSourceTest.class
      .getResource("/data/csv/expected/expected.gdl")
      .getPath();

    DataSource dataSource = new CSVDataSource(csvPath, getConfig());
    LogicalGraph input = dataSource.getLogicalGraph();
    LogicalGraph expected = getLoaderFromFile(gdlPath)
      .getLogicalGraphByVariable("expected");

    collectAndAssertTrue(input.equalsByElementData(expected));
  }
}
