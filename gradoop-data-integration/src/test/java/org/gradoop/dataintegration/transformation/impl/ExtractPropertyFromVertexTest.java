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
package org.gradoop.dataintegration.transformation.impl;

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.dataintegration.transformation.impl.config.EdgeDirection;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.operators.subgraph.functions.LabelIsIn;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

/**
 * Tests for the {@link ExtractPropertyFromVertex} operator.
 */
public class ExtractPropertyFromVertexTest extends GradoopFlinkTestBase {

  /**
   * Tests whether vertices are correctly deduplicated/condensed.
   *
   * @throws Exception If the data could not be loaded or collected properly.
   */
  @Test
  public void cityDeduplicationTest() throws Exception {
    LogicalGraph social = getSocialNetworkLoader().getLogicalGraph();

    // this operation should create 3 vertices with the property values: Berlin, Dresden, Leipzig
    // keep in mind that the default for condensation is 'true'
    UnaryGraphToGraphOperator extOne = new ExtractPropertyFromVertex("Person", "city",
      "City", "name");

    LogicalGraph extOneGraph = social.callForGraph(extOne);
    List<Vertex> city = extOneGraph.getVerticesByLabel("City").collect();
    Set<String> cityNames = new HashSet<>();
    for (Vertex v : city) {
      String cityName = v.getPropertyValue("name").getString();
      Assert.assertTrue(cityName.equals("Dresden") ||
        cityName.equals("Berlin") ||
        cityName.equals("Leipzig"));
      Assert.assertTrue(cityNames.add(cityName));
    }
    Assert.assertEquals(3, cityNames.size());

    // The number of edges should be the same as before.
    Assert.assertEquals(social.getEdges().count(), extOneGraph.getEdges().count());
  }

  /**
   * Tests whether edges are created properly according to Origin to New Vertex direction.
   *
   * @throws Exception If the data could not be loaded or collected properly.
   */
  @Test
  public void edgeCreationOriginToNewTest() throws Exception {
    LogicalGraph social = getSocialNetworkLoader().getLogicalGraph();
    testForEdgeDirection(social, EdgeDirection.ORIGIN_TO_NEWVERTEX);
  }

  /**
   * Tests whether edges are created properly according to New Vertex to Origin direction.
   *
   * @throws Exception If the data could not be loaded or collected properly.
   */
  @Test
  public void edgeCreationNewToOriginTest() throws Exception {
    LogicalGraph social = getSocialNetworkLoader().getLogicalGraph();
    testForEdgeDirection(social, EdgeDirection.NEWVERTEX_TO_ORIGIN);
  }

  /**
   * Tests whether edges are created properly according to bidrectional configuration.
   *
   * @throws Exception If the data could not be loaded or collected properly.
   */
  @Test
  public void edgeCreationBidirectionalTest() throws Exception {
    LogicalGraph social = getSocialNetworkLoader().getLogicalGraph();
    testForEdgeDirection(social, EdgeDirection.BIDIRECTIONAL);
  }

  /**
   * A private convenience method for easier testing of different setups in the edge creation process.
   *
   * @param graph The input graph for the tests.
   * @param direction The edge direction the graph is tested for.
   * @throws Exception Is thrown if the process cant be executed properly.
   */
  private void testForEdgeDirection(LogicalGraph graph, EdgeDirection direction) throws Exception {
    Set<String> cities = new HashSet<>(Arrays.asList("Dresden", "Berlin", "Leipzig"));
    Set<String> persons = new HashSet<>(Arrays.asList("Eve", "Alice", "Frank", "Dave", "Bob", "Carol"));

    UnaryGraphToGraphOperator extract = new ExtractPropertyFromVertex("Person", "city",
      "City", "name", direction, "newLabel");
    LogicalGraph extractedGraph = graph.callForGraph(extract);

    long expectedEdgeCount = direction.equals(EdgeDirection.BIDIRECTIONAL) ? 12 : 6;
    Assert.assertEquals(expectedEdgeCount, extractedGraph.getEdgesByLabel("newLabel").count());

    List<Vertex> vertices = new ArrayList<>();
    extractedGraph.getVertices()
      .filter(new LabelIsIn<>("Person", "City"))
      .output(new LocalCollectionOutputFormat<>(vertices));

    List<Edge> newEdges = new ArrayList<>();
    extractedGraph
      .getEdgesByLabel("newLabel")
      .output(new LocalCollectionOutputFormat<>(newEdges));

    getConfig().getExecutionEnvironment().execute();

    Map<GradoopId, String> idMap = new HashMap<>();
    vertices.forEach(v -> idMap.put(v.getId(), v.getPropertyValue("name").getString()));

    for (Edge e : newEdges) {
      String sourceName = idMap.get(e.getSourceId());
      String targetName = idMap.get(e.getTargetId());

      if (direction.equals(EdgeDirection.ORIGIN_TO_NEWVERTEX)) {
        Assert.assertTrue("source: " + sourceName + " | target: " + targetName +
            " | edge direction: " + direction.name(),
          persons.contains(sourceName) && cities.contains(targetName));
      } else if (direction.equals(EdgeDirection.NEWVERTEX_TO_ORIGIN)) {
        Assert.assertTrue("source: " + sourceName + " | target: " + targetName +
            " | edge direction: " + direction.name(),
          cities.contains(sourceName) && persons.contains(targetName));
      } else if (direction.equals(EdgeDirection.BIDIRECTIONAL)) {
        boolean cityContainment = cities.contains(sourceName) || cities.contains(targetName);
        boolean personContainment = persons.contains(sourceName) || persons.contains(targetName);
        Assert.assertTrue("vertex name 1: " + sourceName + " | vertex name 2: " + targetName +
            " | edge direction: " + direction.name(),
          cityContainment && personContainment);
      }
    }
  }

  /**
   * Tests whether vertices are created properly even if not deduplicated.
   *
   * @throws Exception If the data could not be loaded or collected properly.
   */
  @Test
  public void nonDeduplicationTest() throws Exception {
    LogicalGraph social = getSocialNetworkLoader().getLogicalGraph();

    // this operation should create 6 vertices with the property values:
    // 'Berlin', 3 times 'Dresden', 2 times 'Leipzig'
    ExtractPropertyFromVertex extract = new ExtractPropertyFromVertex("Person", "city", "City", "name");
    extract.setCondensation(false);

    LogicalGraph extractedGraph = social.callForGraph(extract);

    List<Vertex> vertices = new ArrayList<>();
    extractedGraph
      .getVerticesByLabel("City")
      .output(new LocalCollectionOutputFormat<>(vertices));

    getConfig().getExecutionEnvironment().execute();

    Map<String, Integer> cityCountMap = new HashMap<>();
    vertices.forEach(v -> cityCountMap.merge(v.getPropertyValue("name").getString(), 1,
      Integer::sum));

    Assert.assertEquals(1, cityCountMap.get("Berlin").intValue());
    Assert.assertEquals(3, cityCountMap.get("Dresden").intValue());
    Assert.assertEquals(2, cityCountMap.get("Leipzig").intValue());
  }

  /**
   * Tests whether list properties are extracted correctly.
   *
   * @throws Exception If collect doesn't work as expected.
   */
  @Test
  public void listPropertyTest() throws Exception {
    VertexFactory vf = getConfig().getVertexFactory();
    Vertex v1 = vf.createVertex("foo");
    v1.setProperty("a", PropertyValue.create(Arrays.asList(PropertyValue.create("m"), PropertyValue.create("n"))));

    Vertex v2 = vf.createVertex("foo");
    v2.setProperty("a", PropertyValue.create(Arrays.asList(PropertyValue.create("x"), PropertyValue.create("y"), PropertyValue.create("z"))));

    LogicalGraph input = getConfig().getLogicalGraphFactory().fromCollections(Arrays.asList(v1, v2), Collections.emptyList());

    ExtractPropertyFromVertex ext = new ExtractPropertyFromVertex("foo", "a", "A", "key");
    LogicalGraph output = input.callForGraph(ext);

    List<Vertex> createdVertices = new ArrayList<>();
    output.getVertices()
      .filter(new ByLabel<>("A"))
      .output(new LocalCollectionOutputFormat<>(createdVertices));

    input.getConfig().getExecutionEnvironment().execute();

    ArrayList<String> properties = new ArrayList<>();
    createdVertices.forEach(v -> properties.add(v.getPropertyValue("key").getString()));
    Assert.assertTrue(properties.containsAll(Arrays.asList("m", "n", "x", "y", "z")));
  }

}
