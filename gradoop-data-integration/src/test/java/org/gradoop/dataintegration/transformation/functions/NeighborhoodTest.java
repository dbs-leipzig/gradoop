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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.transformation.impl.Neighborhood;
import org.gradoop.dataintegration.transformation.impl.NeighborhoodVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Neighborhood}.
 */
@RunWith(Parameterized.class)
public class NeighborhoodTest extends GradoopFlinkTestBase {

  /**
   * Loader to get the test graphs from.
   */
  private final FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
    "(i1:I1)-->(center:Center)-->(o1:O1)" +
    "(center2:Center2)" +
    "]" +
    "incoming1 [(i1)]" +
    "outgoing1 [(o1)]" +
    "undirected1 [(i1)(o1)]" +
    "incoming2[] outgoing2[] undirected2[]");

  /**
   * The variable name of the center vertex.
   */
  @Parameterized.Parameter(0)
  public String centerVertex;

  /**
   * The variable name of the neighborhood graph.
   */
  @Parameterized.Parameter(1)
  public String neighborhood;

  /**
   * The direction of the neighborhood.
   */
  @Parameterized.Parameter(2)
  public Neighborhood.EdgeDirection direction;

  /**
   * Parameters for this test.
   *
   * @return the parameters for this test.
   */
  @Parameterized.Parameters(name = "neighborhood of {0} ({2})")
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(
      new Object[]{"center", "incoming1", Neighborhood.EdgeDirection.INCOMING},
      new Object[]{"center", "outgoing1", Neighborhood.EdgeDirection.OUTGOING},
      new Object[]{"center", "undirected1", Neighborhood.EdgeDirection.UNDIRECTED},
      new Object[]{"center2", "incoming2", Neighborhood.EdgeDirection.INCOMING},
      new Object[]{"center2", "outgoing2", Neighborhood.EdgeDirection.OUTGOING},
      new Object[]{"center2", "undirected2", Neighborhood.EdgeDirection.UNDIRECTED},
      new Object[]{"nocenter", "incoming2", Neighborhood.EdgeDirection.INCOMING},
      new Object[]{"nocenter", "outgoing2", Neighborhood.EdgeDirection.OUTGOING},
      new Object[]{"nocenter", "undirected2", Neighborhood.EdgeDirection.UNDIRECTED});
  }

  /**
   * Test {@link Neighborhood} with different parameters.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testNeighborhood() throws Exception {
    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    Vertex center = loader.getVertexByVariable(centerVertex);
    Collection<Vertex> neighborhoodVertices = loader.getVerticesByGraphVariables(neighborhood);
    DataSet<Vertex> centers = center == null ? getEmptyDataSet(new Vertex()) :
      getExecutionEnvironment().fromElements(center);
    List<Tuple2<Vertex, List<NeighborhoodVertex>>> neighborhoods =
      Neighborhood.getPerVertex(input, centers, direction).collect();
    // Make sure we only have 1 center vertex.
    long centerCount = neighborhoods.stream().map(h -> h.f0.getId()).distinct().count();
    assertEquals(center != null ? 1 : 0, centerCount);

    // Get all neighbor ids of that vertex.
    Object[] neighbors = neighborhoods.stream().flatMap(h -> h.f1.stream())
      .map(NeighborhoodVertex::getNeighborId)
      .sorted()
      .toArray();
    Object[] neighborIdsExpected = neighborhoodVertices.stream().map(Vertex::getId).sorted()
        .toArray();
    assertArrayEquals(neighborIdsExpected, neighbors);
  }
}
