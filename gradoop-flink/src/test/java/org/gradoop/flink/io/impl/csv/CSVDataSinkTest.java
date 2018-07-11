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
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.FileReader;

public class CSVDataSinkTest extends CSVTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testWrite() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader()
      .getDatabase()
      .getDatabaseGraph(true);

    checkCSVWrite(tmpPath, input);
  }

  @Test
  public void testWriteWithDifferentPropertyTypes() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "intFloat[(v1:a {key:1})-->(v2:a {key:1})]" +
      "intStr[(v3:a {key:1})-->(v4:a {key:\"Foo\"})]" +
      "strFloat[(v5:a {key:\"Bar\"})-->(v6:a {key:1.1})]" +
      "intFloatStr[(v7:a {key:1})-->(v8:a {key:1.1})-->(v9:a {key:\"BarBar\"})]" +
      "edges[" +
      "(v10:A)-[e1:a {keya:14, keyb:3, keyc:\"Foo\"}]->(v10:B)," +
      "(v11:B)-[e2:b {keya:1.1f, keyb:\"Bar\", keyc:2.5f}]->(v11:A)" +
      "]");

    checkCSVWrite(tmpPath, loader.getLogicalGraphByVariable("intFloat"));
    checkCSVWrite(tmpPath, loader.getLogicalGraphByVariable("intStr"));
    checkCSVWrite(tmpPath, loader.getLogicalGraphByVariable("strFloat"));
    checkCSVWrite(tmpPath, loader.getLogicalGraphByVariable("intFloatStr"));
    checkCSVWrite(tmpPath, loader.getLogicalGraphByVariable("edges"));
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

  /**
   * Test CSVDataSink to write a graph with all supported properties
   *
   * @throws Exception on failure
   */
  @Test
  public void testWriteExtendedProperties() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph logicalGraph = getExtendedLogicalGraph();
    DataSink csvDataSink = new CSVDataSink(tmpPath, getConfig());
    csvDataSink.write(logicalGraph, true);

    getExecutionEnvironment().execute();

    DataSource csvDataSource = new CSVDataSource(tmpPath, getConfig());
    LogicalGraph sourceLogicalGraph = csvDataSource.getLogicalGraph();

    collectAndAssertTrue(logicalGraph.equalsByElementData(sourceLogicalGraph));
    collectAndAssertTrue(logicalGraph.equalsByData(sourceLogicalGraph));

    sourceLogicalGraph.getEdges().collect().forEach(this::checkProperties);
    sourceLogicalGraph.getVertices().collect().forEach(this::checkProperties);
  }

  /**
   * Test the content of the metadata.csv file
   *
   * @throws Exception on failure
   */
  @Test
  public void testWriteMetadataCsv() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph logicalGraph = getExtendedLogicalGraph();
    DataSink csvDataSink = new CSVDataSink(tmpPath, getConfig());
    csvDataSink.write(logicalGraph, true);

    getExecutionEnvironment().execute();

    String metadataFile = tmpPath + "/metadata.csv";
    String line;

    BufferedReader br = new BufferedReader(new FileReader(metadataFile));
    while ((line = br.readLine()) != null) {
      checkMetadataCsvLine(line);
    }
  }

  private void checkCSVWrite(String tmpPath, LogicalGraph input) throws Exception {
    DataSink csvDataSink = new CSVDataSink(tmpPath, getConfig());
    csvDataSink.write(input, true);

    getExecutionEnvironment().execute();

    DataSource csvDataSource = new CSVDataSource(tmpPath, getConfig());
    LogicalGraph output = csvDataSource.getLogicalGraph();

    collectAndAssertTrue(input.equalsByElementData(output));
  }
}
