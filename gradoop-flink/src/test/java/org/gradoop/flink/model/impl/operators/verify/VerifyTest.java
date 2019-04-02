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

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.functions.epgm.ByProperty;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

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
  public void testWithSubgraph() throws Exception {
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
}
