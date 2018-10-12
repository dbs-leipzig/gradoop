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
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.AddToGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test class for an indexed csv data sink
 */
public class IndexedCSVDataSinkTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  /**
   * Test writing an indexed csv graph collection.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testWrite() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    GraphCollection input = getSocialNetworkLoader().getGraphCollection();

    checkIndexedCSVWrite(tmpPath, input);
  }

  /**
   * Test writing a logical csv graph.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testWriteLogicalGraph() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    checkIndexedCSVWrite(tmpPath, input);
  }

  /**
   * Test IndexedCSVDataSink to write a graph with different property types
   * using the same label on different elements with the same label.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testWriteWithDifferentPropertyTypes() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g:graph1[" +
        "(v1:A {keya:1, keyb:2, keyc:\"Foo\"})," +
        "(v2:A {keya:1.2f, keyb:\"Bar\", keyc:2.3f})," +
        "(v3:A {keya:\"Bar\", keyb:true})," +
        "(v1)-[e1:a {keya:14, keyb:3, keyc:\"Foo\"}]->(v1)," +
        "(v1)-[e2:a {keya:1.1f, keyb:\"Bar\", keyc:2.5f}]->(v1)," +
        "(v1)-[e3:a {keya:true, keyb:3.13f}]->(v1)" +
        "]");

    checkIndexedCSVWrite(tmpPath, loader.getLogicalGraphByVariable("g"));
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
    // if the metadata is not separated.
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "single:graph1[" +
      "(v1:B {keya:2})" +
      "(v1)-[e1:A {keya:false}]->(v1)," +
      "]" +
      "multiple:graph2[" +
      "(v2:B {keya:true, keyb:1, keyc:\"Foo\"})," +
      "(v3:B {keya:false, keyb:2})," +
      "(v4:C {keya:2.3f, keyb:\"Bar\"})," +
      "(v5:C {keya:1.1f})," +
      "(v2)-[e2:B {keya:1, keyb:2.23d, keyc:3.3d}]->(v3)," +
      "(v3)-[e3:B {keya:2, keyb:7.2d}]->(v2)," +
      "(v4)-[e4:C {keya:false}]->(v4)," +
      "(v5)-[e5:C {keya:true, keyb:13}]->(v5)" +
      "]");

    GraphCollection graphCollection = loader.getGraphCollectionByVariables("single", "multiple");
    checkIndexedCSVWrite(tmpPath, graphCollection);
  }

  /**
   * Test writing and reading a graph with different labels that result in the same
   * indexed CSV path because of replaced illegal filename characters in windows.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testDifferentLabelsInSameFile() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();
    ExecutionEnvironment env = getExecutionEnvironment();

    // This results in the path "graphs/a_b" because < and > are illegal filename characters.
    GraphHead graphHead1 = new GraphHeadFactory().createGraphHead("a<b");
    GraphHead graphHead2 = new GraphHeadFactory().createGraphHead("a>b");
    DataSet<GraphHead> graphHeads = env.fromElements(graphHead1, graphHead2);

    // This results in the path "vertices/b_c" because < and > are illegal filename characters.
    Vertex vertex1 = new VertexFactory().createVertex("B<C");
    Vertex vertex2 = new VertexFactory().createVertex("B>C");
    DataSet<Vertex> vertices = env.fromElements(vertex1, vertex2)
      .map(new AddToGraph<>(graphHead1))
      .map(new AddToGraph<>(graphHead2))
      .withForwardedFields("id;label;properties");

    // This results in the path "edges/c_d" because < and > are illegal filename characters.
    Edge edge1 = new EdgeFactory().createEdge("c<d", vertex1.getId(), vertex2.getId());
    Edge edge2 = new EdgeFactory().createEdge("c>d", vertex2.getId(), vertex1.getId());
    DataSet<Edge> edges = env.fromElements(edge1, edge2)
      .map(new AddToGraph<>(graphHead1))
      .withForwardedFields("id;label;properties");

    LogicalGraph graph = getConfig().getLogicalGraphFactory()
      .fromDataSets(graphHeads, vertices, edges);

    checkIndexedCSVWrite(tmpPath, graph);
  }

  /**
   * Test IndexedCSVDataSink to escape strings and labels that contain delimiter characters.
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

    List<PropertyValue> list = Arrays.asList(PropertyValue.create(string2),
      PropertyValue.create(string1));
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

    Edge edge = new EdgeFactory().createEdge(string1, vertex.getId(), vertex.getId(), props);
    DataSet<Edge> edges = env.fromElements(edge)
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;label;properties");

    LogicalGraph graph = getConfig().getLogicalGraphFactory()
      .fromDataSets(graphHeads, vertices, edges);

    checkIndexedCSVWrite(tmpPath, graph);
  }

  /**
   * Test writing and reading a graph collection with empty strings as labels.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testWriteWithEmptyLabel() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g0[" +
      "(v1 {keya:2})" +
      "(v2)" +
      "(v3:_)" +
      "]" +
      "g1 {key:\"property\"}[" +
      "(v4)-[e1 {keya:1}]->(v4)" +
      "(v4)-[e2]->(v4)" +
      "(v4)-[e3:_ {keya:false}]->(v4)" +
      "]" +
      "g2:_ {graph:\"hasALabel\"}[" +
      "(v5)" +
      "]");

    GraphCollection graphCollection = loader.getGraphCollectionByVariables("g0", "g1", "g2");
    checkIndexedCSVWrite(tmpPath, graphCollection);
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

    String csvPath = getFilePath("/data/csv/input_indexed");

    String gdlPath = getFilePath("/data/csv/expected/expected_graph_collection.gdl");

    GraphCollection input = getLoaderFromFile(gdlPath).getGraphCollectionByVariables("expected1",
      "expected2");

    DataSink csvDataSink =
      new IndexedCSVDataSink(tmpPath, csvPath + "/metadata.csv", getConfig());

    csvDataSink.write(input, true);

    getExecutionEnvironment().execute();

    DataSource csvDataSource = new IndexedCSVDataSource(tmpPath, getConfig());
    GraphCollection output = csvDataSource.getGraphCollection();

    collectAndAssertTrue(input.equalsByGraphElementData(output));
  }

  /**
   * Test writing and reading the given graph to and from IndexedCSV.
   *
   * @param tmpPath path to write csv
   * @param input logical graph
   * @throws Exception if the execution or IO fails.
   */
  private void checkIndexedCSVWrite(String tmpPath, LogicalGraph input) throws Exception {
    checkIndexedCSVWrite(tmpPath, input.getConfig().getGraphCollectionFactory().fromGraph(input));
  }

  /**
   * Test writing and reading the given graph to and from CSV
   *
   * @param tmpPath path to write csv
   * @param input graph collection
   * @throws Exception if the execution or IO fails.
   */
  private void checkIndexedCSVWrite(String tmpPath, GraphCollection input) throws Exception {
    DataSink indexedCSVDataSink = new IndexedCSVDataSink(tmpPath, getConfig());
    indexedCSVDataSink.write(input, true);

    getExecutionEnvironment().execute();

    DataSource indexedCSVDataSource = new IndexedCSVDataSource(tmpPath, getConfig());
    GraphCollection output = indexedCSVDataSource.getGraphCollection();

    collectAndAssertTrue(input.equalsByGraphElementData(output));
  }
}
