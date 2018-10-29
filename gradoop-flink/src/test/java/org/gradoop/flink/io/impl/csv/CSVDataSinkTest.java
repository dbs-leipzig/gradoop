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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.AddToGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests CSVDataSink
 */
public class CSVDataSinkTest extends CSVTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testWrite() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    GraphCollection graphCollection = getSocialNetworkLoader().getGraphCollection();

    checkCSVWrite(tmpPath, graphCollection);
  }

  /**
   * Test CSVDataSink to write a graph with different property types
   * using the same label on different elements with the same label.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testWriteWithDifferentPropertyTypes() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "vertices[" +
      "(v1:A {keya:1, keyb:2, keyc:\"Foo\"})," +
      "(v2:A {keya:1.2f, keyb:\"Bar\", keyc:2.3f})," +
      "(v3:A {keya:\"Bar\", keyb:true})" +
      "]" +
      "edges[" +
      "(v1)-[e1:a {keya:14, keyb:3, keyc:\"Foo\"}]->(v1)," +
      "(v1)-[e2:a {keya:1.1f, keyb:\"Bar\", keyc:2.5f}]->(v1)," +
      "(v1)-[e3:a {keya:true, keyb:3.13f}]->(v1)" +
      "]");

    checkCSVWrite(tmpPath, loader.getLogicalGraphByVariable("vertices"));
    checkCSVWrite(tmpPath, loader.getLogicalGraphByVariable("edges"));
  }

  /**
   * Test writing and reading a graph that uses the same label for vertices and edges.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testWriteWithSameLabel() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    // The properties are incompatible to get a conversion error
    // if the metadata is not separated
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "single[" +
      "(v1:A {keya:2})" +
      "(v1)-[e1:A {keya:false}]->(v1)," +
      "]" +
      "multiple[" +
      "(v2:B {keya:true, keyb:1, keyc:\"Foo\"})," +
      "(v3:B {keya:false, keyb:2})," +
      "(v4:C {keya:2.3f, keyb:\"Bar\"})," +
      "(v5:C {keya:1.1f})," +
      "(v2)-[e2:B {keya:1, keyb:2.23d, keyc:3.3d}]->(v3)," +
      "(v3)-[e3:B {keya:2, keyb:7.2d}]->(v2)," +
      "(v4)-[e4:C {keya:false}]->(v4)," +
      "(v5)-[e5:C {keya:true, keyb:13}]->(v5)" +
      "]");
    checkCSVWrite(tmpPath, loader.getLogicalGraphByVariable("single"));
    checkCSVWrite(tmpPath, loader.getLogicalGraphByVariable("multiple"));
  }

  /**
   * Test CSVDataSink to escape strings and labels that contain delimiter characters.
   * Escape characters get inserted before delimiter characters that are part of strings or labels.
   * Whitespace gets replaced by their control character sequence.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testWriteWithDelimiterCharacters() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();
    ExecutionEnvironment env = getExecutionEnvironment();

    String string1 = "abc;,|:\n=\\ def";
    String string2 = "def;,|:\n=\\ ghi";

    List<PropertyValue> list = Arrays.asList(PropertyValue.create(string2), PropertyValue.create(string1));
    Set<PropertyValue> set = new HashSet<>(list);
    Map<PropertyValue, PropertyValue> map1 = new HashMap<PropertyValue, PropertyValue>() {{
      put(PropertyValue.create(string1), PropertyValue.create(string2));
      put(PropertyValue.create("key"), PropertyValue.create(string1));
    }};
    Map<PropertyValue, PropertyValue> map2 = new HashMap<PropertyValue, PropertyValue>() {{
      put(PropertyValue.create(string1), PropertyValue.create(1));
      put(PropertyValue.create("key"), PropertyValue.create(2));
    }};
    Map<PropertyValue, PropertyValue> map3 = new HashMap<PropertyValue, PropertyValue>() {{
      put(PropertyValue.create(1), PropertyValue.create(string2));
      put(PropertyValue.create(2), PropertyValue.create(string1));
    }};

    Properties props = Properties.create();
    props.set(string1, string2);
    props.set(string2, GradoopTestUtils.BOOL_VAL_1);
    props.set(GradoopTestUtils.KEY_2, string2);
    props.set(GradoopTestUtils.KEY_3, list);
    props.set(GradoopTestUtils.KEY_4, set);
    props.set(GradoopTestUtils.KEY_5, map1);
    props.set(GradoopTestUtils.KEY_5, map2);
    props.set(GradoopTestUtils.KEY_6, map3);

    GraphHead graphHead = new GraphHeadFactory().createGraphHead(string1, props);
    DataSet<GraphHead> graphHeads = env.fromElements(graphHead);

    Vertex vertex = new VertexFactory().createVertex(string1, props);
    DataSet<Vertex> vertices = env.fromElements(vertex)
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;label;properties");

    DataSet<Edge> edges = env.fromElements(new EdgeFactory()
      .createEdge(string1, vertex.getId(), vertex.getId(), props))
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;label;properties");

    LogicalGraph graph = getConfig().getLogicalGraphFactory()
      .fromDataSets(graphHeads, vertices, edges);

    checkCSVWrite(tmpPath, graph);
  }

  /**
   * Test writing and reading a graph with a existing metadata file instead of aggregating
   * new metadata from the graph.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testWriteWithExistingMetaData() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    String csvPath = getFilePath("/data/csv/input_graph_collection");

    String gdlPath = getFilePath("/data/csv/expected/expected_graph_collection.gdl");

    LogicalGraph input = getLoaderFromFile(gdlPath).getLogicalGraphByVariable("expected");

    DataSink csvDataSink = new CSVDataSink(tmpPath, csvPath + "/metadata.csv", getConfig());
    csvDataSink.write(input, true);

    getExecutionEnvironment().execute();

    DataSource csvDataSource = new CSVDataSource(tmpPath, getConfig());
    LogicalGraph output = csvDataSource.getLogicalGraph();

    collectAndAssertTrue(input.equalsByElementData(output));
  }

  /**
   * Test CSVDataSink to write a graph with all supported properties.
   * CSVDataSource ignores the graph heads when using getLogicalGraph(),
   * therefore the graph head is not tested for equality.
   *
   * @throws Exception if the execution or IO fails.
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

    sourceLogicalGraph.getEdges().collect().forEach(this::checkProperties);
    sourceLogicalGraph.getVertices().collect().forEach(this::checkProperties);
  }

  /**
   * Test the content of the metadata.csv file
   *
   * @throws Exception if the execution or IO fails.
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

  /**
   * Test writing and reading the given graph to and from CSV
   *
   * @param tmpPath path to write csv
   * @param input logical graph
   * @throws Exception if the execution or IO fails.
   */
  private void checkCSVWrite(String tmpPath, LogicalGraph input) throws Exception {
    checkCSVWrite(tmpPath, input.getConfig().getGraphCollectionFactory().fromGraph(input));
  }

  /**
   * Test writing and reading the given graph to and from CSV
   *
   * @param tmpPath path to write csv
   * @param input graph collection
   * @throws Exception if the execution or IO fails.
   */
  private void checkCSVWrite(String tmpPath, GraphCollection input) throws Exception {
    DataSink csvDataSink = new CSVDataSink(tmpPath, getConfig());
    csvDataSink.write(input, true);

    getExecutionEnvironment().execute();

    DataSource csvDataSource = new CSVDataSource(tmpPath, getConfig());
    GraphCollection output = csvDataSource.getGraphCollection();

    collectAndAssertTrue(input.equalsByGraphElementData(output));
  }
}
