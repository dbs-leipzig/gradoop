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
package org.gradoop.flink.model.impl.operators.verify;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.functions.epgm.ByProperty;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Test for the {@link Verify} operator.
 */
public class VerifyTest extends GradoopFlinkTestBase {

  /**
   * Test the verify operator with a subgraph operator.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testVerifyWithSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();
    loader.appendToDatabaseFromString("expected[" +
      "(eve)-[ekb:knows {since : 2015}]->(bob)" +
      "]");
    LogicalGraph input = loader.getLogicalGraphByVariable("g0");
    // Apply a subgraph operator that would result in dangling edges.
    LogicalGraph subgraph = input.subgraph(
      new ByProperty<Vertex>("name", PropertyValue.create("Alice")).negate(),
      new True<>(), Subgraph.Strategy.BOTH);
    // Make sure that the graph contains dangling edges.
    List<Edge> danglingEdges = getDanglingEdges(subgraph);
    List<Edge> expectedDanglingEdges = Arrays.asList(loader.getEdgeByVariable("eka"),
      loader.getEdgeByVariable("akb"), loader.getEdgeByVariable("bka"));
    Comparator<Edge> comparator = Comparator.comparing(Edge::getId);
    danglingEdges.sort(comparator);
    expectedDanglingEdges.sort(comparator);
    assertArrayEquals(expectedDanglingEdges.toArray(), danglingEdges.toArray());
    // Now run verify and check if those edges were removed.
    LogicalGraph verifiedSubgraph = subgraph.verify();
    assertEquals("Verified graph contained dangling edges.",
      0, getDanglingEdges(verifiedSubgraph).size());
    // Check if nothing else has been removed (i.e. the result is correct)
    collectAndAssertTrue(loader.getLogicalGraphByVariable("expected")
      .equalsByElementData(verifiedSubgraph));
  }

  /**
   * Determines all dangling edges of a graph.
   *
   * @throws Exception when the execution in Flink fails.
   */
  private List<Edge> getDanglingEdges(LogicalGraph graph) throws Exception {
    List<Vertex> vertices = new ArrayList<>();
    List<Edge> edges = new ArrayList<>();
    graph.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
    graph.getEdges().output(new LocalCollectionOutputFormat<>(edges));
    getExecutionEnvironment().execute();
    Set<GradoopId> ids = vertices.stream().map(Vertex::getId).collect(Collectors.toSet());
    return edges.stream()
      .filter(e -> !ids.contains(e.getSourceId()) || !ids.contains(e.getTargetId()))
      .collect(Collectors.toList());
  }

  /**
   * Test {@link VerifyGraphContainment} on a logical graph.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testVerifyGraphContainment() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph graph = loader.getLogicalGraphByVariable("g1");
    graph = graph.verifyGraphContainment();

    GradoopIdSet idSet = GradoopIdSet.fromExisting(graph.getGraphHead().map(new Id<>()).collect());

    assertGraphContainment(idSet, graph.getVertices());
    assertGraphContainment(idSet, graph.getEdges());
  }

  /**
   * Test {@link VerifyGraphsContainment} on a graph collection.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testVerifyGraphsContainment() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection collection = loader.getGraphCollectionByVariables("g1", "g2");
    collection = collection.verifyGraphsContainment();

    GradoopIdSet idSet = GradoopIdSet.fromExisting(collection.getGraphHeads().map(new Id<>()).collect());

    assertGraphContainment(idSet, collection.getVertices());
    assertGraphContainment(idSet, collection.getEdges());
  }

  /**
   * Function to test graph element datasets for dangling graph ids.
   *
   * @param idSet ids of the corresponding graph or graph collection.
   * @param elements element dataset to test for dangling grahp ids.
   * @param <E> element type.
   * @throws Exception when the execution in Flink fails.
   */
  private <E extends EPGMGraphElement> void assertGraphContainment(GradoopIdSet idSet, DataSet<E> elements)
    throws Exception {
    for (E element : elements.collect()) {
      GradoopIdSet ids = element.getGraphIds();
      assertFalse("Element has no graph ids", ids.isEmpty());
      ids.removeAll(idSet);
      assertTrue("Element has dangling graph ids", ids.isEmpty());
    }
  }
}
