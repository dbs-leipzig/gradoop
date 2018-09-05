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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.edgelist.VertexLabeledEdgeListDataSourceTest;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
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

public class IndexedCSVDataSinkTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testWrite() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    checkIndexedCSVWrite(tmpPath, input);
  }

  /**
   * Test IndexedCSVDataSink to write a graph with different property types
   * using the same label on different elements with the same label.
   *
   * @throws Exception on failure
   */
  @Test
  public void testWriteWithDifferentPropertyTypes() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g[" +
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
   * Test IndexedCSVDataSink to properly separate the metadata
   * of edges and vertices using the same label.
   *
   * @throws Exception
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
    checkIndexedCSVWrite(tmpPath, loader.getLogicalGraphByVariable("single"));
    checkIndexedCSVWrite(tmpPath, loader.getLogicalGraphByVariable("multiple"));
  }

  /**
   * Test IndexedCSVDataSink to properly escape strings and labels. The graph is created manually,
   * since GDL does not support special characters.
   *
   * @throws Exception on failure
   */
  @Test
  public void testWriteWithDelimiterCharacters() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();
    ExecutionEnvironment env = getExecutionEnvironment();

    String string1 = "abc;,|:\n=\\def";
    String string2 = "def;,|:\n=\\ghi";

    List<PropertyValue> list = Arrays.asList(PropertyValue.create(string2), PropertyValue.create(string2));
    Set<PropertyValue> set = new HashSet<>(list);
    Map<PropertyValue, PropertyValue> map1 = new HashMap<PropertyValue, PropertyValue>() {{
      put(PropertyValue.create(string1), PropertyValue.create(string2));
      put(PropertyValue.create("k"), PropertyValue.create(string1));
    }};
    Map<PropertyValue, PropertyValue> map2 = new HashMap<PropertyValue, PropertyValue>() {{
      put(PropertyValue.create(string1), PropertyValue.create(1));
      put(PropertyValue.create("k"), PropertyValue.create(2));
    }};
    Map<PropertyValue, PropertyValue> map3 = new HashMap<PropertyValue, PropertyValue>() {{
      put(PropertyValue.create(1), PropertyValue.create(string2));
      put(PropertyValue.create(2), PropertyValue.create(string1));
    }};

    Map<String, Object> data = new HashMap<>();
    data.put(string1, string2);
    data.put(string2, 13);
    data.put("key3", string2);
    data.put("key4", list);
    data.put("key5", set);
    data.put("key6", map1);
    data.put("key7", map2);
    data.put("key8", map3);

    Properties props = Properties.createFromMap(data);

    Vertex vertex = new VertexFactory().createVertex(string1, props);
    DataSet<Vertex> vertices = env.fromElements(vertex);

    DataSet<Edge> edges = env.fromElements(new EdgeFactory()
      .createEdge(string1, vertex.getId(), vertex.getId(), props));

    LogicalGraph graph = getConfig().getLogicalGraphFactory()
      .fromDataSets(vertices, edges);

    checkIndexedCSVWrite(tmpPath, graph);
  }

  @Test
  public void testWriteWithExistingMetaData() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    String csvPath = VertexLabeledEdgeListDataSourceTest.class
      .getResource("/data/csv/input_indexed").getFile();

    String gdlPath = IndexedCSVDataSourceTest.class
      .getResource("/data/csv/expected/expected.gdl").getFile();

    LogicalGraph input = getLoaderFromFile(gdlPath).getLogicalGraphByVariable("expected");

    DataSink csvDataSink =
      new IndexedCSVDataSink(tmpPath, csvPath + "/metadata.csv", getConfig());

    csvDataSink.write(input, true);

    getExecutionEnvironment().execute();

    DataSource csvDataSource = new IndexedCSVDataSource(tmpPath, getConfig());
    LogicalGraph output = csvDataSource.getLogicalGraph();

    collectAndAssertTrue(input.equalsByElementData(output));
  }

  /**
   * Test writing and reading the given graph to and from IndexedCSV
   *
   * @param tmpPath path to write csv
   * @param input logical graph
   * @throws Exception on failure
   */
  private void checkIndexedCSVWrite(String tmpPath, LogicalGraph input) throws Exception {
    DataSink csvDataSink = new IndexedCSVDataSink(tmpPath, getConfig());
    csvDataSink.write(input, true);

    getExecutionEnvironment().execute();

    DataSource csvDataSource = new IndexedCSVDataSource(tmpPath, getConfig());
    LogicalGraph output = csvDataSource.getLogicalGraph();

    collectAndAssertTrue(input.equalsByElementData(output));
  }
}
