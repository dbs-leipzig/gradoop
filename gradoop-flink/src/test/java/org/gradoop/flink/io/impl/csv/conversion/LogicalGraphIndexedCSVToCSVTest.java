/*
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
package org.gradoop.flink.io.impl.csv.conversion;

import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.deprecated.logicalgraphcsv.LogicalGraphIndexedCSVDataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test class for converting logical graph indexed csv to indexed csv
 */
public class LogicalGraphIndexedCSVToCSVTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  /**
   * Test converting logical graph indexed csv to indexed csv.
   *
   * @throws Exception on failure
   */
  @Test
  public void testConversion() throws Exception {
    String csvPath = getFilePath("/data/csv/input_indexed_deprecated");

    DataSource lgIndexedCSVDataSource = new LogicalGraphIndexedCSVDataSource(csvPath, getConfig());

    LogicalGraph oldGraph = lgIndexedCSVDataSource.getLogicalGraph();

    String tmpPath = temporaryFolder.getRoot().getPath();
    DataSink csvDataSink = new CSVDataSink(tmpPath, getConfig());
    csvDataSink.write(oldGraph, true);

    getExecutionEnvironment().execute();

    DataSource csvDataSource = new CSVDataSource(tmpPath, getConfig());
    LogicalGraph newGraph = csvDataSource.getLogicalGraph();

    collectAndAssertTrue(oldGraph.equalsByData(newGraph));
  }
}
