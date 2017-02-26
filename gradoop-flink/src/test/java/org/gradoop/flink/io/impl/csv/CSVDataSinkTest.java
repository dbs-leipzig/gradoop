package org.gradoop.flink.io.impl.csv;

import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CSVDataSinkTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testWrite() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader()
      .getDatabase()
      .getDatabaseGraph(true);

    DataSink csvDataSink = new CSVDataSink(tmpPath, getConfig());
    csvDataSink.write(input, true);

    getExecutionEnvironment().execute();

    DataSource csvDataSource = new CSVDataSource(tmpPath, getConfig());
    LogicalGraph output = csvDataSource.getLogicalGraph();

    collectAndAssertTrue(input.equalsByElementData(output));
  }
}
