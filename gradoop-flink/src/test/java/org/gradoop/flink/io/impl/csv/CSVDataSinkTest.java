/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.io.impl.csv;

import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.edgelist.VertexLabeledEdgeListDataSourceTest;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
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

  @Test
  public void testWriteWithExistingMetaData() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    String csvPath = VertexLabeledEdgeListDataSourceTest.class
      .getResource("/data/csv/input")
      .getFile();

    String gdlPath = CSVDataSourceTest.class
      .getResource("/data/csv/expected/expected.gdl")
      .getFile();

    LogicalGraph input = getLoaderFromFile(gdlPath).getLogicalGraphByVariable("expected");

    DataSink csvDataSink = new CSVDataSink(tmpPath, csvPath + "/metadata.csv", getConfig());
    csvDataSink.write(input, true);

    getExecutionEnvironment().execute();

    DataSource csvDataSource = new CSVDataSource(tmpPath, getConfig());
    LogicalGraph output = csvDataSource.getLogicalGraph();

    collectAndAssertTrue(input.equalsByElementData(output));
  }
}
