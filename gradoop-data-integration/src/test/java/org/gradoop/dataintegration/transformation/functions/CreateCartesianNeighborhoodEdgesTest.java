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
package org.gradoop.dataintegration.transformation.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.dataintegration.transformation.impl.NeighborhoodVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test for the {@link CreateCartesianNeighborhoodEdges} function used by
 * {@link org.gradoop.dataintegration.transformation.ConnectNeighbors}.
 */
public class CreateCartesianNeighborhoodEdgesTest extends GradoopFlinkTestBase {

  /**
   * The label used for the newly created edges.
   */
  private final String edgeLabel = "test";

  /**
   * The function to test.
   */
  private CreateCartesianNeighborhoodEdges<Vertex, Edge> toTest;

  /**
   * The factory used to create new vertices.
   */
  private VertexFactory vertexFactory;

  /**
   * Set this test up, creating the function to test.
   */
  @Before
  public void setUp() {
    vertexFactory = getConfig().getVertexFactory();
    toTest = new CreateCartesianNeighborhoodEdges<>(getConfig().getEdgeFactory(), edgeLabel);
  }

  /**
   * Test the function using an empty neighborhood. Should produce no edges.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testWithEmptyNeighborhood() throws Exception {
    Vertex someVertex = vertexFactory.createVertex();
    Tuple2<Vertex, List<NeighborhoodVertex>> inputEmpty = new Tuple2<>(someVertex,
      Collections.emptyList());
    List<Edge> result = getExecutionEnvironment().fromElements(inputEmpty)
      .flatMap(toTest).collect();
    assertEquals(0, result.size());
  }

  /**
   * Test the function using a non-empty neighborhood.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testWithNonEmptyNeighborhood() throws Exception {
    Vertex someVertex = vertexFactory.createVertex();
    final int count = 10;
    List<GradoopId> ids = Stream.generate(GradoopId::get).limit(count).collect(Collectors.toList());
    // Create some dummy neighborhood vertex pojos.
    List<NeighborhoodVertex> vertexPojos = ids.stream()
      .map(id -> new NeighborhoodVertex(id, ""))
      .collect(Collectors.toList());
    Tuple2<Vertex, List<NeighborhoodVertex>> inputNonEmpty = new Tuple2<>(someVertex,
      vertexPojos);
    List<Edge> result = getExecutionEnvironment().fromElements(inputNonEmpty)
      .flatMap(toTest).collect();
    // Connect each neighbor with another neighbor, except for itself.
    assertEquals(count * (count - 1), result.size());
    // The result should not contain loops
    for (Edge edge : result) {
      assertNotEquals(edge.getSourceId(), edge.getTargetId());
    }
    // or duplicate edges.
    long disctinctCount = result.stream()
      .map(e -> new Tuple2<>(e.getSourceId(), e.getTargetId()))
      .distinct().count();
    assertEquals((long) result.size(), disctinctCount);
  }
}
