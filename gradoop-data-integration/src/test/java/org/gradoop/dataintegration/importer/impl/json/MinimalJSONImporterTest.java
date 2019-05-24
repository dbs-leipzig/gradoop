/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.dataintegration.importer.impl.json;

import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

/**
 * Tests for {@link MinimalJSONImporter}.
 */
public class MinimalJSONImporterTest extends GradoopFlinkTestBase {

  /**
   * The path of the test JSON data.
   */
  private final String dirPath = MinimalJSONImporter.class.getResource("/json/testdata.json").getFile();

  /**
   * The path of a single file of the test JSON data.
   */
  private final String filePath = MinimalJSONImporter.class.getResource("/json/testdata.json/2").getFile();

  /**
   * The loader used to load the expected graph.
   */
  private FlinkAsciiGraphLoader loader = getLoaderFromString("expected1[" +
    "(:JsonRowVertex)" +
    "(:JsonRowVertex)" +
    "] expected2 [" +
    "(:JsonRowVertex {name: \"Some test name\", age: \"42\", height: \"1.75\"})" +
    "(:JsonRowVertex {data: \"{\\\"a\\\":1,\\\"b\\\":100000}\"})" +
    "] expected3 [" +
    "(:JsonRowVertex {canceled: \"false\", conferencename: \"Test\"})" +
    "]");

  /**
   * Test reading from a directory containing multiple parts.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testReadDir() throws Exception {
    DataSource dataImport = new MinimalJSONImporter(dirPath, getConfig());
    LogicalGraph read = dataImport.getLogicalGraph();
    LogicalGraph expected = loader.getLogicalGraph();
    GraphCollection expectedCollection =
      getConfig().getGraphCollectionFactory().fromGraph(expected);

    collectAndAssertTrue(expected.equalsByElementData(read));
    collectAndAssertTrue(dataImport.getGraphCollection()
      .equalsByGraphElementData(expectedCollection));
  }

  /**
   * Test reading a single file.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testReadFile() throws Exception {
    DataSource dataImport = new MinimalJSONImporter(filePath, getConfig());
    LogicalGraph read = dataImport.getLogicalGraph();
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected2");
    GraphCollection expectedCollection =
      getConfig().getGraphCollectionFactory().fromGraph(expected);

    collectAndAssertTrue(expected.equalsByElementData(read));
    collectAndAssertTrue(dataImport.getGraphCollection()
      .equalsByGraphElementData(expectedCollection));
  }
}
