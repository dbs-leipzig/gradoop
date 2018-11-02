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
package org.gradoop.flink.io.impl.csv.indexed;

import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.junit.Test;

/**
 * Test class for indexed csv data source
 */
public class IndexedCSVDataSourceTest extends GradoopFlinkTestBase {

  /**
   * Test reading an indexed csv graph collection.
   *
   * @throws Exception on failure
   */
  @Test
  public void testRead() throws Exception {
    String csvPath = getFilePath("/data/csv/input_indexed");

    String gdlPath = getFilePath("/data/csv/expected/expected_graph_collection.gdl");

    DataSource dataSource = new IndexedCSVDataSource(csvPath, getConfig());
    GraphCollection input = dataSource.getGraphCollection();

    GraphCollection expected = getLoaderFromFile(gdlPath)
      .getGraphCollectionByVariables("expected1", "expected2");

    collectAndAssertTrue(input.equalsByGraphElementData(expected));
  }

  /**
   * Test reading a single logical indexed csv graph.
   *
   * @throws Exception on failure
   */
  @Test
  public void testReadSingleGraph() throws Exception {
    String csvPath = getFilePath("/data/csv/input_indexed");

    String gdlPath = getFilePath("/data/csv/expected/expected_graph_collection.gdl");

    DataSource dataSource = new IndexedCSVDataSource(csvPath, getConfig());
    LogicalGraph input = dataSource.getLogicalGraph();

    GraphCollection graphCollection = getLoaderFromFile(gdlPath)
      .getGraphCollectionByVariables("expected1", "expected2");
    LogicalGraph expected = graphCollection.reduce(new ReduceCombination());

    collectAndAssertTrue(input.equalsByElementData(expected));
  }

  /**
   * Test reading a indexed csv graph collection without edges.
   *
   * @throws Exception on failure
   */
  @Test
  public void testEmptyEdgeRead() throws Exception {
    String csvPath = getFilePath("/data/csv/input_indexed_no_edges");

    String gdlPath = getFilePath("/data/csv/expected/expected_no_edges.gdl");

    DataSource dataSource = new IndexedCSVDataSource(csvPath, getConfig());
    GraphCollection input = dataSource.getGraphCollection();

    GraphCollection expected = getLoaderFromFile(gdlPath)
      .getGraphCollectionByVariables("expected1", "expected2");

    collectAndAssertTrue(input.equalsByGraphElementData(expected));
  }
}
