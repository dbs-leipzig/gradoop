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
package org.gradoop.flink.io.impl.csv;

import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;

public class CSVDataSourceTest extends CSVTestBase {

  @Test
  public void testRead() throws Exception {
    String csvPath = getFilePath("/data/csv/input_graph_collection");

    String gdlPath = getFilePath("/data/csv/expected/expected_graph_collection.gdl");

    DataSource dataSource = new CSVDataSource(csvPath, getConfig());
    GraphCollection input = dataSource.getGraphCollection();
    GraphCollection expected = getLoaderFromFile(gdlPath)
      .getGraphCollectionByVariables("expected1", "expected2");

    collectAndAssertTrue(input.equalsByGraphElementData(expected));
  }

  /**
   * Test reading a logical graph from csv files with properties
   * that are supported by csv source and sink
   *
   * @throws Exception on failure
   */
  @Test
  public void testReadExtendedProperties() throws Exception {
    LogicalGraph expected = getExtendedLogicalGraph();

    String csvPath = getFilePath("/data/csv/input_extended_properties");

    DataSource dataSource = new CSVDataSource(csvPath, getConfig());
    LogicalGraph sourceLogicalGraph = dataSource.getLogicalGraph();

    collectAndAssertTrue(sourceLogicalGraph.equalsByElementData(expected));

    dataSource.getLogicalGraph().getEdges().collect()
      .forEach(this::checkProperties);
    dataSource.getLogicalGraph().getVertices().collect()
      .forEach(this::checkProperties);
  }
}
