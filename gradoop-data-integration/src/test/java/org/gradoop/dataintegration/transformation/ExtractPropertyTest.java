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
package org.gradoop.dataintegration.transformation;

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.transformation.config.EdgeDirection;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.functions.filters.Or;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * Tests for the {@link ExtractProperty} operator.
 */
public class ExtractPropertyTest extends GradoopFlinkTestBase {

  /**
   * Tests whether vertices are correctly deduplicated/condensed.
   * @throws Exception If the data could not be loaded or collected properly.
   */
  @Test
  public void cityDeduplicationTest() throws Exception {
    LogicalGraph social = getSocialNetworkLoader().getLogicalGraph();

    // this operation should create 3 vertices with the property values: Berlin, Dresden, Leipzig
    // keep in mind that the default for condensation is 'true'
    UnaryGraphToGraphOperator extOne = new ExtractProperty("Person", "city",
        "City", "name");

    LogicalGraph extOneGraph = social.callForGraph(extOne);
    List<Vertex> city = extOneGraph.getVerticesByLabel("City").collect();
    Set<String> cityNames = new HashSet<>();
    for(Vertex v: city) {
      String cityName = v.getPropertyValue("name").getString();
      Assert.assertTrue(cityName.equals("Dresden") ||
          cityName.equals("Berlin") ||
          cityName.equals("Leipzig"));
      cityNames.add(cityName);
    }
    Assert.assertEquals(3, cityNames.size());

    // The number of edges should be the same as before.
    Assert.assertEquals(social.getEdges().count(), extOneGraph.getEdges().count());
  }

  /**
   * Tests whether edges are created properly according to the configuration.
   * @throws Exception If the data could not be loaded or collected properly.
   */
  @Test
  public void edgeCreationTest() throws Exception {
    LogicalGraph social = getSocialNetworkLoader().getLogicalGraph();

    // Test for Origin to New Vertex direction
    testForEdgeDirection(social, EdgeDirection.ORIGIN_TO_NEWVERTEX);

    // Test for New Vertex to Origin Vertex direction
    testForEdgeDirection(social, EdgeDirection.NEWVERTEX_TO_ORIGIN);

    // Test for bidirectional edges
    testForEdgeDirection(social, EdgeDirection.BIDIRECTIONAL);
  }

  private void testForEdgeDirection(LogicalGraph graph, EdgeDirection direction) throws Exception {
    Set<String> cities = new HashSet<>(Arrays.asList("Dresden", "Berlin", "Leipzig"));
    Set<String> persons = new HashSet<>(Arrays.asList("Eve", "Alice", "Frank", "Dave", "Bob", "Carol"));

    UnaryGraphToGraphOperator extract = new ExtractProperty("Person", "city",
        "City", "name", direction, "newLabel");
    LogicalGraph extractedGraph = graph.callForGraph(extract);

    long expectedEdgeCount = direction.equals(EdgeDirection.BIDIRECTIONAL) ? 12 : 6;
    Assert.assertEquals(expectedEdgeCount, extractedGraph.getEdgesByLabel("newLabel").count());

    List<Vertex> vertices = new ArrayList<>();
    extractedGraph.getVertices()
        .filter(new Or<>(new ByLabel<>("Person"), new ByLabel<>("City")))
        .output(new LocalCollectionOutputFormat<>(vertices));

    List<Edge> newEdges = new ArrayList<>();
    extractedGraph
        .getEdgesByLabel("newLabel")
        .output(new LocalCollectionOutputFormat<>(newEdges));

    getConfig().getExecutionEnvironment().execute();

    Map<GradoopId, String> idMap = new HashMap<>();
    vertices.forEach(v -> idMap.put(v.getId(), v.getPropertyValue("name").getString()));

    for(Edge e : newEdges) {
      String sourceName = idMap.get(e.getSourceId());
      String targetName = idMap.get(e.getTargetId());

      if(direction.equals(EdgeDirection.ORIGIN_TO_NEWVERTEX)) {
        Assert.assertTrue("source: " + sourceName + " | target: " + targetName
                + " | edge direction: " + direction.name(),
            persons.contains(sourceName) && cities.contains(targetName));
      } else if(direction.equals(EdgeDirection.NEWVERTEX_TO_ORIGIN)) {
        Assert.assertTrue("source: " + sourceName + " | target: " + targetName
                + " | edge direction: " + direction.name(),
            cities.contains(sourceName) && persons.contains(targetName));
      } else if(direction.equals(EdgeDirection.BIDIRECTIONAL)) {
        boolean cityContainment = cities.contains(sourceName) || cities.contains(targetName);
        boolean personContainment = persons.contains(sourceName) || persons.contains(targetName);
        Assert.assertTrue("vertex name 1: " + sourceName + " | vertex name 2: " + targetName
                + " | edge direction: " + direction.name(),
            cityContainment && personContainment);
      }
    }
  }

  /**
   * Tests whether vertices are created properly even if not deduplicated.
   * @throws Exception If the data could not be loaded or collected properly.
   */
  @Test
  public void nonDeduplicationTest() throws Exception {
    LogicalGraph social = getSocialNetworkLoader().getLogicalGraph();

    // this operation should create 6 vertices with the property values:
    // 'Berlin', 3 times 'Dresden', 2 times 'Leipzig'
    ExtractProperty extract = new ExtractProperty("Person", "city",
        "City", "name");
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

}
