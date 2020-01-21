/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.functions.epgm.ByProperty;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.subgraph.ApplySubgraph;
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
      new ByProperty<EPGMVertex>("name", PropertyValue.create("Alice")).negate(),
      new True<>(), Subgraph.Strategy.BOTH);

    // Make sure that the graph contains dangling edges.
    List<EPGMEdge> danglingEdges = getDanglingEdges(subgraph);
    List<EPGMEdge> expectedDanglingEdges = Arrays.asList(loader.getEdgeByVariable("eka"),
      loader.getEdgeByVariable("akb"), loader.getEdgeByVariable("bka"));
    Comparator<EPGMEdge> comparator = Comparator.comparing(EPGMEdge::getId);
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
   * Test the verify operator for graph collections with a subgraph operator.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testVerifyWithApplySubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();
    loader.appendToDatabaseFromString("expected0[" +
      "(eve)-[ekb:knows {since : 2015}]->(bob)" +
      "] expected1 [" +
      "(frank)-[fkd:knows {since : 2015}]->(dave)" +
      "]");
    GraphCollection input = loader.getGraphCollectionByVariables("g0", "g1");
    GradoopId id0 = loader.getGraphHeadByVariable("g0").getId();
    GradoopId id1 = loader.getGraphHeadByVariable("g1").getId();

    // Apply a subgraph operator that would result in dangling edges.
    GraphCollection subgraph = input.apply(new ApplySubgraph<>(
      new ByProperty<EPGMVertex>("name", PropertyValue.create("Alice"))
        .or(new ByProperty<>("name", PropertyValue.create("Carol")))
        .negate(),
      new True<>(), Subgraph.Strategy.BOTH));

    // Make sure that the graphs contain dangling edges.
    Comparator<EPGMEdge> comparator = Comparator.comparing(EPGMEdge::getId);
    List<EPGMEdge> danglingEdges0 = getDanglingEdges(subgraph.getGraph(id0));
    List<EPGMEdge> expectedDanglingEdges0 = Arrays.asList(loader.getEdgeByVariable("eka"),
      loader.getEdgeByVariable("akb"), loader.getEdgeByVariable("bka"));
    danglingEdges0.sort(comparator);
    expectedDanglingEdges0.sort(comparator);
    assertArrayEquals(expectedDanglingEdges0.toArray(), danglingEdges0.toArray());

    List<EPGMEdge> danglingEdges1 = getDanglingEdges(subgraph.getGraph(id1));
    List<EPGMEdge> expectedDanglingEdges1 = Arrays.asList(loader.getEdgeByVariable("fkc"),
      loader.getEdgeByVariable("ckd"), loader.getEdgeByVariable("dkc"));
    danglingEdges1.sort(comparator);
    expectedDanglingEdges1.sort(comparator);
    assertArrayEquals(expectedDanglingEdges1.toArray(), danglingEdges1.toArray());

    // Now run verify and check if those edges were removed.
    GraphCollection verifiedSubgraph = subgraph.verify();

    assertEquals("Verified graph contained dangling edges.",
      0, getDanglingEdges(verifiedSubgraph.getGraph(id0)).size());
    assertEquals("Verified graph contained dangling edges.",
      0, getDanglingEdges(verifiedSubgraph.getGraph(id1)).size());

    // Check if nothing else has been removed (i.e. the result is correct)
    collectAndAssertTrue(loader.getLogicalGraphByVariable("expected0")
      .equalsByElementData(verifiedSubgraph.getGraph(id0)));
    collectAndAssertTrue(loader.getLogicalGraphByVariable("expected1")
      .equalsByElementData(verifiedSubgraph.getGraph(id1)));
  }

  /**
   * Determines all dangling edges of a graph.
   *
   * @throws Exception when the execution in Flink fails.
   */
  private List<EPGMEdge> getDanglingEdges(LogicalGraph graph) throws Exception {
    List<EPGMVertex> vertices = new ArrayList<>();
    List<EPGMEdge> edges = new ArrayList<>();
    graph.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
    graph.getEdges().output(new LocalCollectionOutputFormat<>(edges));
    getExecutionEnvironment().execute();
    Set<GradoopId> ids = vertices.stream().map(EPGMVertex::getId).collect(Collectors.toSet());
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
  private <E extends GraphElement> void assertGraphContainment(GradoopIdSet idSet, DataSet<E> elements)
    throws Exception {
    for (E element : elements.collect()) {
      GradoopIdSet ids = element.getGraphIds();
      assertFalse("Element has no graph ids", ids.isEmpty());
      ids.removeAll(idSet);
      assertTrue("Element has dangling graph ids", ids.isEmpty());
    }
  }
}
